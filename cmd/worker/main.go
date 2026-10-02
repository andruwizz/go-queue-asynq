package main

import (
	"context"
	"fmt"
	"go-queue-asynq/internal/config"
	"go-queue-asynq/internal/tasks"
	"log"

	"time"

	"github.com/hibiken/asynq"
)

func main() {
	// Create the Asynq server with Redis configuration
	srv := asynq.NewServer(
		config.GetRedisClientOpt(),
		asynq.Config{
			// Concurrency specifies the maximum number of concurrent workers
			Concurrency: 10,

			// Queues is a map of queue names to their priority.
			// With StrictPriority enabled, higher-weight queues are checked first
			Queues: map[string]int{
				"critical": 6, // Highest priority
				"default":  3, // Default priority
				"low":      1, // Lowest priority
			},

			// StrictPriority ensures higher priority queues are always processed first
			// When true, lower prioerity queues are only processed when higher ones are empty
			StrictPriority: true,

			// ShutdownTimeout specifies how long to wait for active tasks to complete
			ShutdownTimeout: 30 * time.Second,

			// Logger for Asynq internal logging
			Logger: NewAsynqLogger(),

			// ErrorHandler is called whenever a task handler returns an error
			ErrorHandler: asynq.ErrorHandlerFunc(handleError),

			// RetryDelayFunc customizes the delay between retries
			RetryDelayFunc: customRetryDelay,

			// HealthCheckFUnc is called periodically to check worker health
			HealthCheckFunc: func(err error) {
				if err != nil {
					log.Printf("Health check failed: %v", err)
				}
			},

			// HealthCheckInterval specifies how often to run health checks
			HealthCheckInterval: 15 * time.Second,
		},
	)

	// Create a ServeMux to route tasks to handlers
	mux := asynq.NewServeMux()

	// Use middleware for cross-cutting concerns
	mux.Use(loggingMiddleware)
	mux.Use(recoveryMiddleware)

	// Register task handlers
	mux.Handle(tasks.TypeEmailDelivery, &tasks.EmailHandler{})
	mux.Handle(tasks.TypeImageResize, &tasks.ImageHandler{})
	mux.Handle(tasks.TypeWebhookDelivery, &tasks.WebhookHandler{})
	mux.Handle(tasks.TypeReportGeneration, &tasks.ReportHandler{})

	// Start the server. Run listen for SIGNTERM/SIGNINT and gracefully shutdown
	log.Println("Starting Asynq worker server...")
	if err := srv.Run(mux); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

// handleError is called whenever a task handler returns an error
func handleError(ctx context.Context, task *asynq.Task, err error) {
	log.Printf("Task %s failed: %v", task.Type(), err)

	// Here you might want to:
	// - Send an alert to your monitoring system
	// - Log to a dead letter queue table in your database
	// - Notify administrators via Slack/PagerDuty
}

// customRetryDelay implements exponential backoff
func customRetryDelay(n int, err error, task *asynq.Task) time.Duration {
	// Exponential backoff: 1s, 2s, 8s, 16s, capped at 1 hour
	delay := time.Duration(1<<uint(n)) * time.Second
	maxDelay := 1 * time.Hour

	if delay > maxDelay {
		return maxDelay
	}

	return delay
}

// loggingMiddleware logs task execution details
func loggingMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		start := time.Now()
		log.Printf("Starting task: type=%s", t.Type())

		err := h.ProcessTask(ctx, t)

		duration := time.Since(start)
		if err != nil {
			log.Printf("Task failed: type=%s, duration=%v, error=%v", t.Type(), duration, err)
		} else {
			log.Printf("Task completed: type=%s, duration=%v", t.Type(), duration)
		}

		return err
	})
}

// recoveryMiddleware recovers from panics and converts them to errors
func recoveryMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Panic recovered in task %s: %v", t.Type(), r)
				err = fmt.Errorf("panic: %v", r)
			}
		}()

		return h.ProcessTask(ctx, t)
	})
}

// AsynqLogger implements the asynq.Logger interface
type AsynqLogger struct{}

func NewAsynqLogger() *AsynqLogger {
	return &AsynqLogger{}
}

func (l *AsynqLogger) Debug(args ...interface{}) {
	log.Println(args...)
}

func (l *AsynqLogger) Info(args ...interface{}) {
	log.Println(args...)
}

func (l *AsynqLogger) Warn(args ...interface{}) {
	log.Println(args...)
}

func (l *AsynqLogger) Error(args ...interface{}) {
	log.Println(args...)
}

func (l *AsynqLogger) Fatal(args ...interface{}) {
	log.Println(args...)
}
