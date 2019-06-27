package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/gomodule/redigo/redis"
)

type Job struct {
	Id         string `redis:"id"`
	Topic      string `redis:"topic"`
	Delay      int    `redis:"delay"`
	TTR        int    `redis:"TTR"` // time-to-run
	Body       string `redis:"body"`
	Status     int    `redis:"status"`
	ConsumeNum int    `redis:"consume_num"`
}

const (
	JOB_STATUS_DETAULT  = iota
	JOB_STATUS_DELAY    // delay：不可执行状态，等待时钟周期
	JOB_STATUS_READY    // ready：可执行状态，等待消费
	JOB_STATUS_RESERVED // reserved: 已被消费者读取，但还未得到消费者的响应（delete、finish）
)

var (
	ErrJobIdEmpty    = errors.New("job.id is empty")
	ErrJobTopicEmpty = errors.New("job.topic is empty")
)

func (j *Job) CheckJobData() error {
	if len(j.Id) == 0 {
		return ErrJobIdEmpty
	}
	if len(j.Topic) == 0 {
		return ErrJobTopicEmpty
	}

	return nil
}

func (j *Job) String() string {
	s, _ := Encode(j)
	return s
}

type JobCard struct {
	id    string
	delay int
	topic string
}

func (j *Job) Card() *JobCard {
	return &JobCard{
		id:    j.Id,
		delay: j.Delay,
		topic: j.Topic,
	}
}

func (j *Job) Key() string {
	return GetJobKeyById(j.Id)
}

func Encode(j *Job) (string, error) {
	nbyte, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	return string(nbyte), nil
}

func Decode(j string) (*Job, error) {
	job := &Job{}
	err := json.Unmarshal([]byte(j), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func Pop(topics ...string) (map[string]string, error) {
	if len(topics) == 0 {
		return nil, errors.New("topics is empty")
	}

	var ts []interface{}
	for _, t := range topics {
		ts = append(ts, GetJobQueueByTopic(t))
	}
	ts = append(ts, 2)

	// 每次只会消费一个job,多个consumer消费时,redis会轮询分配给各个consumer
	// consumer订阅多个topic时,会按照topic顺序读取,即先消费完第一个topic所有job,才会进行下一个topic
	records, err := Redis.Strings("BRPOP", ts...)
	if err != nil {
		if err == redis.ErrNil {
			return nil, errors.New("empty")
		}
		return nil, err
	}

	jobId := records[1]
	if err := SetJobStatus(jobId, JOB_STATUS_RESERVED); err != nil {
		return nil, err
	}
	detail, err := GetJobDetailById(jobId)
	if err != nil {
		return nil, err
	}

	// TTR表示job执行超时时间(即消费者读取到job到确认删除这段时间)
	// TTR>0时,若执行时间超过TTR,将重新添加到ready_queue,然后再次被消费
	// TTR<=0时,消费者读取到job时,即会删除任务池中的job单元
	TTR, err := strconv.Atoi(detail["TTR"])
	if err != nil {
		return nil, err
	}
	if TTR > 0 {
		Dper.addToTTRBucket <- &JobCard{
			id:    detail["id"],
			delay: TTR + 3,
			topic: detail["topic"],
		}
		// 计数被消费次数
		IncrJobConsumeNum(jobId)
	} else {
		Ack(detail["id"])
	}

	return detail, nil
}

func Ack(jobId string) (bool, error) {
	job, err := GetJobStuctById(jobId)
	if err != nil {
		return false, err
	}

	// TTR=0,被消费
	if job.Status == JOB_STATUS_RESERVED {
		return Redis.Bool("DEL", GetJobKeyById(jobId))
	}
	// TTR>0,被消费后重新加到bucket,如果到期未确认删除会重新加到readyQueue再次被消费
	if job.Status == JOB_STATUS_DELAY && job.ConsumeNum > 0 {
		return Redis.Bool("DEL", GetJobKeyById(jobId))
	}
	// 可能正在被再次消费或者未被消费
	if job.Status == JOB_STATUS_READY {
		if job.ConsumeNum > 0 {
			return false, errors.New("TTR has expired and will be consumed again")
		} else {
			return false, errors.New("Job is not be reserved")
		}
	}

	return false, errors.New("Unknown error")
}

func Push(j string) error {
	job, err := Decode(j)
	if err != nil {
		return err
	}

	return AddToJobPool(job)
}

func AddToJobPool(j *Job) error {
	isExist, err := Redis.Bool("EXISTS", j.Key())
	if err != nil {
		return err
	}
	if isExist {
		return fmt.Errorf(fmt.Sprintf("jobKey:%v,error:has exist", j.Key()))
	}

	_, err = Redis.Do("HMSET", redis.Args{}.Add(j.Key()).AddFlat(j)...)
	return err
}

func AddToReadyQueue(jobId string) error {
	conn := Redis.Pool.Get()
	defer conn.Close()

	script := `
		local c = redis.call('llen', KEYS[1])
		local r = redis.call('lpush', KEYS[1], ARGV[1])
		if c + 1 == r then
		    redis.call('hset', KEYS[2], 'status', ARGV[2])
		    return 1
		end
		return 0
	`

	key := GetJobKeyById(jobId)
	job, err := GetJobStuctById(jobId)
	if err != nil {
		return err
	}

	if job.Status != JOB_STATUS_DELAY && job.Delay > 0 {
		return fmt.Errorf("jobKey%v,error:job.status is error", key)
	}

	queue := GetJobQueueByTopic(job.Topic)
	var ns = redis.NewScript(2, script)
	_, err = redis.Bool(ns.Do(conn, queue, key, jobId, JOB_STATUS_READY))

	return err
}

func GetTopicByJobId(jobId string) (string, error) {
	key := GetJobKeyById(jobId)
	return Redis.String("HGET", key, "topic")
}

func GetJobDetailById(jobId string) (map[string]string, error) {
	key := GetJobKeyById(jobId)
	return Redis.StringMap("HGETALL", key)
}

func GetJobStuctById(jobId string) (*Job, error) {
	detail, err := GetJobDetailById(jobId)
	if err != nil {
		return nil, err
	}

	delay, err := strconv.Atoi(detail["delay"])
	if err != nil {
		return nil, err
	}
	TTR, err := strconv.Atoi(detail["TTR"])
	if err != nil {
		return nil, err
	}
	status, err := strconv.Atoi(detail["status"])
	if err != nil {
		return nil, err
	}
	consume_num, err := strconv.Atoi(detail["consume_num"])
	if err != nil {
		return nil, err
	}
	return &Job{
		Id:         detail["id"],
		Topic:      detail["topic"],
		Delay:      delay,
		TTR:        TTR,
		Body:       detail["body"],
		Status:     status,
		ConsumeNum: consume_num,
	}, nil
}

func SetJobStatus(jobId string, status int) error {
	key := GetJobKeyById(jobId)
	_, err := Redis.Do("HSET", key, "status", status)
	return err
}

func GetJobStatus(jobId string) (int, error) {
	key := GetJobKeyById(jobId)
	return Redis.Int("HGET", key, "status")
}

func IncrJobConsumeNum(jobId string) (bool, error) {
	key := GetJobKeyById(jobId)
	return Redis.Bool("HINCRBY", key, "consume_num", 1)
}

func GetJobConsumeNum(jobId string) (int, error) {
	key := GetJobKeyById(jobId)
	return Redis.Int("HGET", key, "consume_num")
}
