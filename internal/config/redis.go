package config

import (
	"os"

	"github.com/hibiken/asynq"
)

func GetRedisClientOpt() asynq.RedisClientOpt {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := 0

	return asynq.RedisClientOpt{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	}
}

func GetRedisClusterClientOpt() asynq.RedisClusterClientOpt {
	return asynq.RedisClusterClientOpt{
		Addrs: []string{
			"redis-node-1:6379",
			"redis-node-2:6379",
			"redis-node-3:6379",
		},
	}
}
