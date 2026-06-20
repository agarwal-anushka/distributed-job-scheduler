package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
	"os"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func newRedisClient() *redis.Client {
    addr := os.Getenv("REDIS_ADDR")
    if addr == "" {
        addr = "localhost:6379"
    }
    return redis.NewClient(&redis.Options{
        Addr: addr,
    })
}

func getQueueName(jobType string) string {
	switch jobType {
	case "short":
		return "job_queue:short"
	case "long":
		return "job_queue:long"
	default:
		return "job_queue:default"
	}
}

func publishJob(rdb *redis.Client, jobID int, jobName string, jobType string) error {
	queueName := getQueueName(jobType)
	err := rdb.LPush(ctx, queueName, fmt.Sprintf("%d:%s:%s", jobID, jobName, jobType)).Err()
	if err != nil {
		return err
	}
	fmt.Printf("  Queued job: %s → %s\n", jobName, queueName)
	return nil
}

func startWorker(rdb *redis.Client, db *sql.DB, queueName string) {
	fmt.Printf("Worker started — listening on %s\n", queueName)
	for {
		result, err := rdb.BRPop(ctx, 0, queueName).Result()
		if err != nil {
			fmt.Println("Worker error:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		parts := strings.SplitN(result[1], ":", 3)
		jobID, _ := strconv.Atoi(parts[0])
		jobName := parts[1]
		jobType := "default"
		if len(parts) == 3 {
			jobType = parts[2]
		}

		fmt.Printf("[%s] Worker picked up: %s\n", queueName, jobName)

		runID, err := createRun(db, jobID)
		if err != nil {
			fmt.Println("Error creating run:", err)
			continue
		}

		fmt.Printf("  Executing %s... run_id=%d\n", jobName, runID)

		jobFailed := runID%3 == 0

		if jobFailed {
			updateRunStatus(db, runID, "failed")
			fmt.Printf("  Job %s FAILED\n", jobName)

			retryCount, _ := incrementRetry(db, jobID)
			maxRetries, _ := getMaxRetries(db, jobID)

			if retryCount >= maxRetries {
				moveToDeadLetter(rdb, jobID, jobName, jobType)
				resetRetryCount(db, jobID)
			} else {
				fmt.Printf("  Attempt %d of %d\n", retryCount, maxRetries)
				requeueJob(rdb, jobID, jobName, jobType)
			}
		} else {
			updateRunStatus(db, runID, "success")
			resetRetryCount(db, jobID)
			fmt.Printf("  Job %s done!\n", jobName)
		}
	}
}

func moveToDeadLetter(rdb *redis.Client, jobID int, jobName string, jobType string) error {
	err := rdb.LPush(ctx, "job_queue_dead",
		fmt.Sprintf("%d:%s:%s", jobID, jobName, jobType)).Err()
	if err != nil {
		return err
	}
	fmt.Printf("  ⚠ Job %s moved to dead letter queue\n", jobName)
	return nil
}

func requeueJob(rdb *redis.Client, jobID int, jobName string, jobType string) error {
	queueName := getQueueName(jobType)
	err := rdb.LPush(ctx, queueName,
		fmt.Sprintf("%d:%s:%s", jobID, jobName, jobType)).Err()
	if err != nil {
		return err
	}
	fmt.Printf("  Retrying job %s → %s\n", jobName, queueName)
	return nil
}

func resetRetryCount(db *sql.DB, jobID int) error {
	_, err := db.Exec("UPDATE jobs SET retry_count=0 WHERE id=$1", jobID)
	return err
}
