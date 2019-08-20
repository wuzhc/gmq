// bucket延迟job管理
// 功能:
//		- 定时器扫描bucket
//		- 添加到期job到ready requeue

package gnode

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

var (
	timerDefaultDuration = 1 * time.Second
	timerResetDuration   = 5 * time.Second
	timerSleepDuration   = 24 * time.Hour
)

type Bucket struct {
	sync.Mutex
	Id              string
	JobNum          int32
	NextTime        time.Time
	recvJob         chan *JobCard
	addToReadyQueue chan string
	resetTimerChan  chan struct{}
	closed          chan struct{}
	ctx             *Context
}

type ByNum []*Bucket
type ById []*Bucket

func (b ByNum) Len() int           { return len(b) }
func (b ByNum) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByNum) Less(i, j int) bool { return b[i].JobNum < b[j].JobNum }
func (b ById) Len() int            { return len(b) }
func (b ById) Swap(i, j int)       { b[i], b[j] = b[j], b[i] }
func (b ById) Less(i, j int) bool {
	iid, _ := strconv.Atoi(b[i].Id)
	jid, _ := strconv.Atoi(b[j].Id)
	return iid < jid
}

func (b *Bucket) Key() string {
	return GetBucketKeyById(b.Id)
}

func (b *Bucket) run() {
	go b.startTimer()

	for {
		select {
		case card := <-b.recvJob:
			b.Lock()
			if err := AddToBucket(b, card); err != nil {
				// job添加到bucket失败了,要怎么处理???
				b.ctx.Logger.Error(fmt.Sprintf("Add to bucket failed, the error is %v", err))
				b.Unlock()
				continue
			}

			// 如果bucket下次扫描检索时间比新job的delay相差5秒以上,则重置定时器,
			// 确保新的job能够即时添加到readyQueue,设置5秒间隔可以防止频繁重置定时器
			subTime := b.NextTime.Sub(time.Now())
			if subTime > 0 && subTime-time.Duration(card.delay) > timerResetDuration {
				b.ctx.Logger.Debug(logs.LogCategory("resetTimer"), fmt.Sprintf("bid:%v,resettime", b.Id))
				b.resetTimerChan <- struct{}{}
			}

			atomic.AddInt32(&b.JobNum, 1)
			b.Unlock()
		case jobId := <-b.addToReadyQueue:
			if err := AddToReadyQueue(jobId); err != nil {
				// 添加ready queue失败了,要怎么处理
				b.ctx.Logger.Error(err)
				continue
			}
			atomic.AddInt32(&b.JobNum, -1)
		case <-b.closed:
			// 接收timer退出通知
			return
		}
	}
}

// 定时器周期性检索到期任务
func (b *Bucket) startTimer() {
	var (
		duration = timerDefaultDuration
		timer    = time.NewTimer(duration)
	)

	for {
		select {
		case <-timer.C:
			jobIds, nextTime, err := RetrivalTimeoutJobs(b)
			if err != nil {
				b.ctx.Logger.Error(fmt.Sprintf("bucketId: %v retrival failed, error:%v", b.Key(), err))
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
			b.ctx.Logger.Info(logs.LogCategory("retrivaltime"), logInfo)

			timer.Reset(duration)
		case <-b.resetTimerChan:
			// 大并发场景下,会出现在重置定时器期间有多个重置的请求,目前来说影响不大
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			b.NextTime = time.Now().Add(timerDefaultDuration)
			timer.Reset(timerDefaultDuration)
		case <-b.ctx.Dispatcher.closed:
			b.closed <- struct{}{}
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
// 可能整个事务需要保证原子一致性
func RetrivalTimeoutJobs(b *Bucket) (jobIds []string, nextTime int, err error) {
	conn := Redis.Pool.Get()
	defer conn.Close()

	script := `
		local jobIds = redis.call('zrangebyscore',KEYS[1], 0, ARGV[4], 'withscores', 'limit', 0, 200)
		local res = {}
		for k,jobId in ipairs(jobIds) do 
			if k%2~=0 then
				local jobKey = string.format('%s:%s', ARGV[3], jobId)
				local status = redis.call('hget', jobKey, 'status')
				if tonumber(status) == tonumber(ARGV[1]) or tonumber(status) == tonumber(ARGV[2]) then
					local isDel = redis.call('zrem', KEYS[1], jobId)
					if isDel == 1 then
						table.insert(res, jobId)
					end
				else
					redis.call('zrem',KEYS[1],jobId)
				end
			end
		end
		
		local nextTime
		local nextJob = redis.call('zrange', KEYS[1], 0, 0, 'withscores')
		if next(nextJob) == nil then
			nextTime = -1
		else
			nextTime = tonumber(nextJob[2]) - tonumber(ARGV[4])
			if nextTime < 0 then
				nextTime = 1
			end
		end
		
		table.insert(res,1,tostring(nextTime))
		return res
	
	`

	var ns = redis.NewScript(1, script)
	res, err := redis.Strings(ns.Do(conn, b.Key(), JOB_STATUS_DELAY, JOB_STATUS_RESERVED, JOB_POOL_KEY, time.Now().Unix()))
	if err != nil {
		b.ctx.Logger.Debug(err)
		return nil, 0, err
	}

	nextTime, err = strconv.Atoi(res[0])
	if err != nil {
		return
	}
	if len(res) > 1 {
		jobIds = res[1:]
	} else {
		jobIds = nil
	}
	return
}

func GetBucketJobNum(b *Bucket) int32 {
	n, _ := Redis.Int32("ZCARD", b.Key())

	return n
}
