package mq

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

var redisPool *redis.Pool

const (
	JOB_POOL_KEY    = "job_pool"
	BUCKET_KEY      = "bucket"
	READY_QUEUE_KEY = "ready_queue"
)

func GetJobKeyById(id string) string {
	return JOB_POOL_KEY + ":" + id
}

func GetJobQueueByTopic(topic string) string {
	return READY_QUEUE_KEY + ":" + topic
}

func GetBucketKeyById(id string) string {
	return BUCKET_KEY + ":" + id
}

func init() {
	redisPool = &redis.Pool{
		MaxIdle:     30,
		MaxActive:   3000,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialPassword(""))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

// 添加到任务池
func AddToJobPool(j *Job) error {
	conn := redisPool.Get()
	defer func() {
		conn.Close()
	}()

	isExist, err := redis.Bool(conn.Do("EXISTS", j.Key()))
	if err != nil {
		return err
	}
	if isExist {
		return fmt.Errorf(fmt.Sprintf("jobKey:%v,error:has exist", j.Key()))
	}

	_, err = conn.Do("HMSET", redis.Args{}.Add(j.Key()).AddFlat(j)...)
	return err
}

// 添加到bucket
// 有序集合score = 延迟秒数 + 当前时间戳
func AddToBucket(b *Bucket, card *JobCard) error {
	conn := redisPool.Get()
	defer conn.Close()

	score := int64(card.delay) + time.Now().Unix()
	_, err := conn.Do("ZADD", b.Key(), score, card.id)
	return err
}

// 移除bucket
func RemoveFromBucket() error {
	return nil
}

// 添加到准备队列
func AddToReadyQueue(jobId string) error {
	conn := redisPool.Get()
	defer conn.Close()

	key := GetJobKeyById(jobId)
	record, err := redis.Strings(conn.Do("HMGET", key, "topic", "status"))
	if err != nil {
		return fmt.Errorf("jobKey:%v,error:%v", key, err)
	}
	if len(record) != 2 {
		return fmt.Errorf("jobKey:%v,error:job struct error")
	}

	topic := record[0]
	status, err := strconv.Atoi(record[1])
	if err != nil {
		return fmt.Errorf("jobKey:%v,error:", key, err)
	}
	if status != JOB_STATUS_DELAY {
		return fmt.Errorf("jobKey:%v,error:status is not delay", key)
	}

	queue := GetJobQueueByTopic(topic)
	_, err = conn.Do("LPUSH", queue, jobId)
	return err
}

// 根据jobId获取topic
func GetTopicByJobId(jobId string) (string, error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := GetJobKeyById(jobId)
	return redis.String(conn.Do("HGET", key, "topic"))
}

// 设置任务状态
func SetJobStatus(jobId string, status int) error {
	conn := redisPool.Get()
	defer conn.Close()

	key := GetJobKeyById(jobId)
	_, err := conn.Do("HSET", key, "status", status)
	return err
}

func GetJobStatus(jobId string) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := GetJobKeyById(jobId)
	return redis.Int(conn.Do("HGET", key, "status"))
}

// 从指定bucket检索到期的job
// nextTime
// 	-1 当前bucket已经没有jobs
//  >0 当前bucket下个job到期时间
func RetrivalTimeoutJobs(b *Bucket) (jobIds []string, nextTime int, err error) {
	conn := redisPool.Get()
	defer conn.Close()

	jobIds = nil
	nextTime = -1
	var records []string
	now := time.Now().Unix()

	// 检索到期jobs,每次最多取50个(若堆积很多jobs,限制每次处理数量)
	records, err = redis.Strings(conn.Do("ZRANGEBYSCORE", b.Key(), 0, now, "WITHSCORES", "LIMIT", 0, 50))
	if err != nil {
		return
	}

	for k, r := range records {
		// 跳过score
		if k%2 != 0 {
			continue
		}
		// 跳过没有准备好的job
		if status, err := GetJobStatus(r); err != nil || status != JOB_STATUS_DELAY {
			continue
		}
		// 被检索后,记得删除bucket中的jobs,这里不根据score范围批量删除,是考虑到如果删除过程中,
		// 刚好又有其他job加入到bucket,这样会误删新加入的job
		n, err := redis.Int(conn.Do("ZREM", b.Key(), r))
		if err == nil && n > 0 {
			jobIds = append(jobIds, r)
		}
	}

	// 下一次定时器执行时间
	records, err = redis.Strings(conn.Do("ZRANGE", b.Key(), 0, 0, "WITHSCORES"))
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

// 获取bucket中job数量
func GetBucketJobNum(b *Bucket) int {
	conn := redisPool.Get()
	defer conn.Close()

	n, _ := redis.Int(conn.Do("ZCARD", b.Key()))
	return n
}
