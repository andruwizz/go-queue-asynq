package main

import (
	"fmt"
	"go-queue-asynq/internal/config"
	"log"

	"github.com/hibiken/asynq"
)

func main() {
	inspector := asynq.NewInspector(config.GetRedisClientOpt())

	// Get all queue information
	queues, err := inspector.Queues()
	if err != nil {
		log.Fatalf("Failed to get queues: %v", err)
	}

	fmt.Println("===Queue Status===")
	for _, queueName := range queues {
		info, err := inspector.GetQueueInfo(queueName)
		if err != nil {
			log.Printf("Failed to get info for queue %s: %v", queueName, err)
			continue
		}

		fmt.Printf("\nQueue: %s\n", queueName)
		fmt.Printf(" Active: %d\n", info.Active)
		fmt.Printf(" Pending: %d\n", info.Pending)
		fmt.Printf(" Scheduled: %d\n", info.Scheduled)
		fmt.Printf(" Retry: %d\n", info.Retry)
		fmt.Printf(" Archived: %d\n", info.Archived)
		fmt.Printf(" Completed: %d\n", info.Completed)
		fmt.Printf(" Processed: %d\n", info.Processed)
		fmt.Printf(" Failed: %d\n", info.Failed)
	}

	// List pending tasks in a specific queue
	fmt.Println("\n=== Pending Tasks (default queue) ===")
	pendingTasks, err := inspector.ListPendingTasks("default", asynq.PageSize(10))
	if err != nil {
		log.Printf("Failed to list pending tasks: %v", err)
	} else {
		for _, task := range pendingTasks {
			fmt.Printf(" - ID: %s, Type: %s\n", task.ID, task.Type)
		}
	}

	// List scheduled tasks
	fmt.Println("\n=== Scheduled Tasks ===")
	scheduledTasks, err := inspector.ListScheduledTasks("default", asynq.PageSize(10))
	if err != nil {
		log.Printf("Failed to list scheduled tasks: %v", err)
	} else {
		for _, task := range scheduledTasks {
			fmt.Printf(" - ID: %s, Type: %s, ProcessedAt: %v\n",
				task.ID, task.Type, task.NextProcessAt)
		}
	}

	// List retry tasks
	fmt.Println("\n=== Retry Tasks ===")
	retryTasks, err := inspector.ListRetryTasks("default", asynq.PageSize(10))
	if err != nil {
		log.Printf("Failed to list retry tasks: %v", err)
	} else {
		for _, task := range retryTasks {
			fmt.Printf(" - ID: %s, Type: %s, Retried: %d, Error: %s\n",
				task.ID, task.Type, task.Retried, task.LastErr)
		}
	}
}
