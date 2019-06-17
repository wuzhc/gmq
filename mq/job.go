package mq

import (
	"encoding/json"
	"errors"
)

type Job struct {
	Id    string `redis:"id"`
	Topic string `redis:"topic"`
	Delay int    `redis:"delay"`
	TTR   int    `redis:"TTR"` // time-to-run
	Body  string `redis:"body"`
}

// ready：可执行状态，等待消费。
// delay：不可执行状态，等待时钟周期。
// reserved：已被消费者读取，但还未得到消费者的响应（delete、finish）。
// deleted：已被消费完成或者已被删除。
const (
	JOB_STATUS_DELAY = iota // delay：不可执行状态，等待时钟周期
	JOB_STATUS_READY        // ready：可执行状态，等待消费
	JOB_STATUS_RESERVED
	JOB_STATUS_DELETE
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

func Decode(j string) *Job {
	job := &Job{}
	err := json.Unmarshal([]byte(j), job)
	if err != nil {
		return nil
	}
	return job
}
