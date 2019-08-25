package gnode

import (
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/wuzhc/gmq/configs"
)

type RedisDB struct {
	Pool *redis.Pool
}

const (
	JOB_POOL_KEY          = "gmq:jobpool"
	BUCKET_KEY            = "gmq:bucket"
	READY_QUEUE_KEY       = "gmq:readyqueue"
	READY_QUEUE_CACHE_KEY = "gmq:rqcachekey"
)

var Redis *RedisDB

func init() {
	Redis = &RedisDB{}
}

func (db *RedisDB) InitPool(cfg *configs.GnodeConfig) *RedisDB {
	db.Pool = &redis.Pool{
		MaxIdle:     cfg.RedisMaxIdle,
		MaxActive:   cfg.RedisMaxActive,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.RedisHost+":"+cfg.RedisPort, redis.DialPassword(""))
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
	return db
}

func (db *RedisDB) Do(command string, args ...interface{}) (interface{}, error) {
	conn := db.Pool.Get()
	defer conn.Close()
	return conn.Do(command, args...)
}

func (db *RedisDB) String(command string, args ...interface{}) (string, error) {
	return redis.String(db.Do(command, args...))
}

func (db *RedisDB) Bool(command string, args ...interface{}) (bool, error) {
	return redis.Bool(db.Do(command, args...))
}

func (db *RedisDB) Strings(command string, args ...interface{}) ([]string, error) {
	return redis.Strings(db.Do(command, args...))
}

func (db *RedisDB) Int(command string, args ...interface{}) (int, error) {
	return redis.Int(db.Do(command, args...))
}

func (db *RedisDB) Int32(command string, args ...interface{}) (int32, error) {
	v, err := db.Int(command, args...)
	return int32(v), err
}

func (db *RedisDB) Int64(command string, args ...interface{}) (int64, error) {
	return redis.Int64(db.Do(command, args...))
}

func (db *RedisDB) Ints(command string, args ...interface{}) ([]int, error) {
	return redis.Ints(db.Do(command, args...))
}

func (db *RedisDB) StringMap(command string, args ...interface{}) (map[string]string, error) {
	return redis.StringMap(db.Do(command, args...))
}

func GetJobKeyById(id int64) string {
	return JOB_POOL_KEY + ":" + strconv.FormatInt(id, 10)
}

func GetJobQueueByTopic(topic string) string {
	return READY_QUEUE_KEY + ":" + topic
}

func GetBucketKeyById(id string) string {
	return BUCKET_KEY + ":" + id
}
