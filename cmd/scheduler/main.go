package main

import (
	"go-queue-asynq/internal/config"
	"go-queue-asynq/internal/tasks"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
)

func main() {
	// Create a new scheduler
	scheduler := asynq.NewScheduler(
		config.GetRedisClientOpt(),
		&asynq.SchedulerOpts{
			Location: time.UTC,
		},
	)

	// Register periodic tasks

	// Daily report at 6 AM UTC
	dailyReportPayload := tasks.ReportGenerationPayload{
		ReportType: "daily_summary",
		Format:     "pdf",
	}
	dailyReportTask, _ := tasks.NewReportGenerationTask(dailyReportPayload)
	scheduler.Register("0 6 * * *", dailyReportTask, asynq.Queue("low"))

	// Hourly data sync
	dataSyncTask := asynq.NewTask(tasks.TypeDataSync, nil)
	scheduler.Register("0 * * * *", dataSyncTask, asynq.Queue("default"))

	// Every 5 minutes health check
	healthCheckTask := asynq.NewTask("health:check", nil)
	scheduler.Register("*/5 * * * *", healthCheckTask, asynq.Queue("critical"))

	// Handle graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		scheduler.Shutdown()
	}()

	log.Println("Starting Asynq scheduler...")
	if err := scheduler.Run(); err != nil {
		log.Fatalf("Failed to run scheduler: %v", err)
	}
}
