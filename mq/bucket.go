package mq

import (
	"fmt"
	"gmq/logs"
	"gmq/utils"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	timerDefaultDuration = 1 * time.Second
	timerResetDuration   = 5 * time.Second
	timerSleepDuration   = 24 * time.Hour
)

type Bucket struct {
	sync.Mutex
	Id              string
	JobNum          int
	NextTime        time.Time
	recvJob         chan *JobCard
	addToReadyQueue chan string
	resetTimerChan  chan struct{}
}

type ByNum []*Bucket
type ById []*Bucket

func (b ByNum) Len() int           { return len(b) }
func (b ByNum) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByNum) Less(i, j int) bool { return b[i].JobNum < b[j].JobNum }
func (b ById) Len() int            { return len(b) }
func (b ById) Swap(i, j int)       { b[i], b[j] = b[j], b[i] }
func (b ById) Less(i, j int) bool  { return b[i].Id < b[j].Id }

func (b *Bucket) Key() string {
	return GetBucketKeyById(b.Id)
}

func (b *Bucket) run() {
	defer gmq.wg.Done()
	gmq.wg.Add(1)

	go b.retrievalTimeoutJobs()

	for {
		select {
		case card := <-b.recvJob:
			b.Lock()
			if err := AddToBucket(b, card); err != nil {
				// job添加到bucket失败了,要怎么处理???
				log.Error(fmt.Sprintf("Add to bucket failed, the error is %v", err))
				b.Unlock()
				continue
			}

			// 如果bucket下次扫描检索时间比新job的delay相差5秒以上,则重置定时器,
			// 确保新的job能够即时添加到readyQueue,设置5秒间隔可以防止频繁重置定时器
			subTime := b.NextTime.Sub(time.Now())
			if subTime > 0 && subTime-time.Duration(card.delay) > timerResetDuration {
				log.Debug(logs.LogCategory("resetTimer"), fmt.Sprintf("bid:%v,resettime", b.Id))
				b.resetTimerChan <- struct{}{}
			}

			b.JobNum++
			b.Unlock()
		case jobId := <-b.addToReadyQueue:
			if err := AddToReadyQueue(jobId); err != nil {
				// 添加ready queue失败了,要怎么处理
				log.Error(err)
				continue
			}
			b.JobNum--
		case <-gmq.notify:
			return
		}
	}
}

// 检索到时任务
func (b *Bucket) retrievalTimeoutJobs() {
	defer gmq.wg.Done()
	defer func() {
		log.Error("retrievalTimeoutJobs退出了")
	}()
	gmq.wg.Add(1)

	var (
		duration = timerDefaultDuration
		timer    = time.NewTimer(duration)
	)

	for {
		select {
		case <-timer.C:
			jobIds, nextTime, err := RetrivalTimeoutJobs(b)
			if err != nil {
				log.Error(fmt.Sprintf("bucketId: %v retrival failed, error:%v", b.Key(), err))
				timer.Reset(duration)
				break
			}

			// 若addToReadyQueue处理太慢,注意这里会阻塞
			// 不要单独的goroutine去处理,会导致addToReadyQueue堆积太多job
			for _, jobId := range jobIds {
				b.addToReadyQueue <- jobId
			}

			if nextTime == -1 {
				duration = timerSleepDuration
			} else {
				duration = time.Duration(nextTime) * time.Second
			}

			b.NextTime = time.Now().Add(duration)
			logInfo := fmt.Sprintf("%v,nexttime:%v", b.Key(), utils.FormatTime(b.NextTime))
			log.Info(logs.LogCategory("retrivaltime"), logInfo)

			timer.Reset(duration)
		case <-b.resetTimerChan:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			b.NextTime = time.Now().Add(timerDefaultDuration)
			timer.Reset(timerDefaultDuration)
		case <-gmq.notify:
			return
		}
	}
}

// 添加到bucket
// 有序集合score = 延迟秒数 + 当前时间戳, member = jobId
// 并且设置job.status = JOB_STATUS_DELAY
// TTR>0时,有可能job先被删除后再添加到bucket,所以添加到bucket前需要检测job是否存在
func AddToBucket(b *Bucket, card *JobCard) error {
	conn := Redis.Pool.Get()
	defer conn.Close()

	script := `
local isExist = redis.call('exists', KEYS[2])
if isExist == 0 then
    return 0
end

local res = redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
if res == 1 then
    redis.call('hset', KEYS[2], 'status', ARGV[3])
    return 1
end
return 0
`
	var score = int64(card.delay) + time.Now().Unix()
	var jobKey = GetJobKeyById(card.id)
	var ns = redis.NewScript(2, script)
	_, err := redis.Bool(ns.Do(conn, b.Key(), jobKey, score, card.id, JOB_STATUS_DELAY))

	return err
}

// 移除bucket
func RemoveFromBucket() error {
	return nil
}

// 从指定bucket检索到期的job
// nextTime参数如下:
// 	-1 当前bucket已经没有jobs
//  >0 当前bucket下个job到期时间
func RetrivalTimeoutJobs(b *Bucket) (jobIds []string, nextTime int, err error) {
	jobIds = nil
	nextTime = -1
	var records []string
	now := time.Now().Unix()

	// 检索到期jobs,每次最多取50个(若堆积很多jobs,限制每次处理数量)
	records, err = Redis.Strings("ZRANGEBYSCORE", b.Key(), 0, now, "WITHSCORES", "LIMIT", 0, 50)
	if err != nil {
		return
	}

	for k, r := range records {
		// 跳过score
		if k%2 != 0 {
			continue
		}
		status, err := GetJobStatus(r)
		if err != nil {
			// 移除已被确认消费的job
			if err == redis.ErrNil {
				Redis.Int("ZREM", b.Key(), r)
			}
			continue
		}
		// 跳过没有设置为bucket的job(正常情况下不会出现状态错误问题)
		if status != JOB_STATUS_DELAY && status != JOB_STATUS_RESERVED {
			continue
		}
		// 被检索后,记得删除bucket中的jobs,这里不根据score范围批量删除,是考虑到如果删除过程中,
		// 刚好又有其他job加入到bucket,这样会误删新加入的job
		n, err := Redis.Int("ZREM", b.Key(), r)
		if err == nil && n > 0 {
			jobIds = append(jobIds, r)
		}
	}

	// 下一次定时器执行时间
	records, err = Redis.Strings("ZRANGE", b.Key(), 0, 0, "WITHSCORES")
	if err != nil {
		return
	}
	if len(records) == 0 {
		nextTime = -1
	} else {
		nextTime, _ = strconv.Atoi(records[1])
		nextTime = nextTime - int(now)
		// 积累了很多到期任务,还没来得及处理,这时下个job到期时间实际上已经小于0,此时设置为1秒不要执行太快
		if nextTime < 0 {
			nextTime = 1
		}
	}

	return
}

func GetBucketJobNum(b *Bucket) int {
	n, _ := Redis.Int("ZCARD", b.Key())

	return n
}
