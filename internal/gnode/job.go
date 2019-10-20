package gnode

import (
	"encoding/json"
	"errors"
)

const (
	JOB_MAX_DELAY = 2592000 // default to 1 month
	JOB_MAX_TTR   = 30      // default to 30 seconds
)

type Msg struct {
	Id    int64  `json:"id"`
	Topic string `json:"topic"`
	Body  string `json:"body"`
	Delay int    `json:"delay"`
}

type Job struct {
	Id         int64
	Topic      string
	Delay      int
	TTR        int // time-to-run
	Body       []byte
	Status     int
	ConsumeNum int
}

var (
	ErrJobIdEmpty    = errors.New("job.id is empty")
	ErrJobTopicEmpty = errors.New("job.topic is empty")
)

func (j *Job) Validate() error {
	if j.Id == 0 {
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
	id    int64
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
