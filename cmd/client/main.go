package main

import (
	"errors"
	"fmt"
	"go-job-queue/internal/config"
	"go-job-queue/internal/tasks"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

func main() {
	// Create an Asynq client
	client := asynq.NewClient(config.GetRedisClientOpt())
	defer client.Close()

	// Example 1: Basic task enqueueing
	enqueueBasicTask(client)

	// Example 2: Delayed/scheduled tasks
	enqueueDelayedTask(client)

	// Example 3: Tasks with custom retry options
	enqueueTaskWithRetries(client)

	// Example 4: Priority queue tasks
	enqueuePriorityTask(client)

	// Example 5: Unique tasks (preventing duplicates)
	enqueueUniqueTask(client)

	// Example 6: Tasks with deadline
	enqueueTaskWithDeadline(client)

	log.Println("All tasks enqueued successfully!")
}

// enqueueBasicTask demonstrates simple task enqueueing
func enqueueBasicTask(client *asynq.Client) {
	payload := tasks.EmailDeliveryPayload{
		UserID:  12345,
		To:      "user@example.com",
		Subject: "Welcome to Platform!",
		Body:    "Thank you for signing up!",
	}

	task, err := tasks.NewEmailDeliveryTask(payload)
	if err != nil {
		log.Fatalf("Failed to create tasks: %v", err)
	}

	// Enqueue the task for immediate processing
	info, err := client.Enqueue(task)
	if err != nil {
		log.Fatalf("Failed to enqueue task: %v", err)
	}

	log.Printf("Enqueue basic task: id=%s, queue=%s", info.ID, info.Queue)
}

// enqueueDelayedTask demonstrates scheduling tasks for future execution
func enqueueDelayedTask(client *asynq.Client) {
	payload := tasks.EmailDeliveryPayload{
		UserID:  12345,
		To:      "user@example.com",
		Subject: "Reminder: Complete your profile",
		Body:    "We noticed you haven't completed your profile...",
	}

	task, err := tasks.NewEmailDeliveryTask(payload)
	if err != nil {
		log.Fatalf("Failed to create tasks: %v", err)
	}

	// Option 1: Process after a specific delay
	info, err := client.Enqueue(task, asynq.ProcessIn(24*time.Hour))
	if err != nil {
		log.Fatalf("Failed to enqueue delayed task: %v", err)
	}
	log.Printf("Enqueue delayed task (24h): id=%s", info.ID)

	// Option 2: Process at a specific time
	scheduledTime := time.Now().Add(7 * 24 * time.Hour) // One week from now
	info, err = client.Enqueue(task, asynq.ProcessAt(scheduledTime))
	if err != nil {
		log.Fatalf("Failed to enqueue scheduled task: %v", err)
	}
	log.Printf("Enqueue scheduled task: id=%s, scheduled_at=%v", info.ID, scheduledTime)
}

// enqueueTaskWithRetries demonstrates tasks with custom retry configuration
func enqueueTaskWithRetries(client *asynq.Client) {
	payload := tasks.WebhookDeliveryPayload{
		WebhookID: "wh_123",
		URL:       "https://example.com/webhook",
		Method:    "POST",
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer auth-token",
		},
		Body:    []byte(`{"event":"order:created","data":{}}`),
		Timeout: 10 * time.Second,
	}

	task, err := tasks.NewWebhookDeliveryTask(payload)
	if err != nil {
		log.Fatalf("Failed to create tasks: %v", err)
	}

	// Configure retry behavior
	info, err := client.Enqueue(task,
		// Maximum number of retry attempts
		asynq.MaxRetry(5),

		// Timeout for each attempt
		asynq.Timeout(10*time.Second),

		// Retention period after completion (for inspection)
		asynq.Retention(24*time.Hour),
	)

	if err != nil {
		log.Fatalf("Failed to enqueue task with retries: %v", err)
	}

	log.Printf("Enqueue task with custom retries: id=%s, max_retry=%d", info.ID, info.MaxRetry)
}

// enqueuePriorityTasks demonstrates using priority queues
func enqueuePriorityTask(client *asynq.Client) {
	// Critical priority - password reset emails
	criticalPayload := tasks.EmailDeliveryPayload{
		UserID:  12345,
		To:      "user@example.com",
		Subject: "Password Reset Request",
		Body:    "Click here to reset your password.",
	}

	criticalTask, err := tasks.NewEmailDeliveryTask(criticalPayload)
	info, err := client.Enqueue(criticalTask, asynq.Queue("critical"))
	if err != nil {
		log.Fatalf("Failed to enqueue critical task: %v", err)
	}
	log.Printf("Enqueue critical priority task: id=%s, queue=%s", info.ID, info.Queue)

	// Default priority - regular notifications
	defaultPayload := tasks.EmailDeliveryPayload{
		UserID:  12345,
		To:      "user@example.com",
		Subject: "New follower",
		Body:    "Someone just followed you...",
	}

	defaultTask, err := tasks.NewEmailDeliveryTask(defaultPayload)
	info, err = client.Enqueue(defaultTask, asynq.Queue("default"))
	if err != nil {
		log.Fatalf("Failed to enqueue default task: %v", err)
	}
	log.Printf("Enqueue default priority task: id=%s, queue=%s", info.ID, info.Queue)

	// Low priority - marketing emails
	lowPayload := tasks.EmailDeliveryPayload{
		UserID:  12345,
		To:      "user@example.com",
		Subject: "Weekly digest",
		Body:    "Here is what you missed this week...",
	}

	lowTask, err := tasks.NewEmailDeliveryTask(lowPayload)
	info, err = client.Enqueue(lowTask, asynq.Queue("low"))
	if err != nil {
		log.Fatalf("Failed to enqueue low task: %v", err)
	}
	log.Printf("Enqueue low priority task: id=%s, queue=%s", info.ID, info.Queue)
}

// enqueueUniqueTask demonstrates preventing duplicate tasks
func enqueueUniqueTask(client *asynq.Client) {
	payload := tasks.ReportGenerationPayload{
		ReportID:   "report_daily_20240427",
		ReportType: "daily_summary",
		UserID:     12345,
		Format:     "pdf",
		StartDate:  time.Now().AddDate(0, 0, -1), // yesterday
		EndDate:    time.Now(),
	}
	task, _ := tasks.NewReportGenerationTask(payload)

	// Use unique option to prevent duplicate tasks
	// if a task with the same type and payload is already in the queue
	// this will return an error instead of creating a duplicate
	info, err := client.Enqueue(task,
		// Task is unique for 1 hour - no duplicate within this window
		asynq.Unique(1*time.Hour),

		// Custom task ID for easier tracking
		asynq.TaskID(fmt.Sprintf("report:%s", payload.ReportID)),
	)

	if err != nil {
		if errors.Is(err, asynq.ErrDuplicateTask) {
			log.Printf("Task already exists, skipping duplicate")
			return
		}
		log.Fatalf("Failed to enqueue unique task: %v", err)
	}

	log.Printf("Enqueue unique task: id=%s", info.ID)
}

// enqueueTaskWithDeadline demonstrates task deadline configuration
func enqueueTaskWithDeadline(client *asynq.Client) {
	payload := tasks.ImageResizePayload{
		ImageID:      "img_456",
		SourceURL:    "https://example.com/image.jpg",
		TargetWidth:  800,
		TargetHeight: 600,
		Format:       "webp",
	}

	task, _ := tasks.NewImageResizeTask(payload)

	// Deadline sets an absolute time by which the task must complete
	// if the task is still running after this time, it will be cancelled
	deadline := time.Now().Add(5 * time.Minute)

	info, err := client.Enqueue(task,
		asynq.Deadline(deadline),
		asynq.Queue("default"),
	)
	if err != nil {
		log.Fatalf("Failed to enqueue task with deadline: %v", err)
	}

	log.Printf("Enqueue task with deadline: id=%s, deadline=%v", info.ID, deadline)
}
