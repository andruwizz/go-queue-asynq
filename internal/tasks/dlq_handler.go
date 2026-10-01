package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

// DeadLetterQueueProcessor handles tasks that have failed all retries
// It provides mechanisms for monitoring, alerting, and reprocessing
type DeadLetterQueueProcessor struct {
	inspector *asynq.Inspector
	client    *asynq.Client
}

// NewDeadLatterQueueProcessor creates a new DLQ processor
func NewDeadLetterQueueProcessor(redisOpt asynq.RedisClientOpt) *DeadLetterQueueProcessor {
	return &DeadLetterQueueProcessor{
		inspector: asynq.NewInspector(redisOpt),
		client:    asynq.NewClient(redisOpt),
	}
}

// Close releases resources
func (p *DeadLetterQueueProcessor) Close() error {
	return errors.Join(p.inspector.Close(), p.client.Close())
}

// ListArchivedTasks retrieves tasks from the dead letter queue
func (p *DeadLetterQueueProcessor) ListArchivedTasks(queueName string, limit int) ([]*asynq.TaskInfo, error) {
	tasks, err := p.inspector.ListArchivedTasks(queueName, asynq.PageSize(limit))
	if err != nil {
		return nil, fmt.Errorf("failed to list archived tasks: %w", err)
	}
	return tasks, nil
}

// RetryArchivedTask moves a task from the archive back to the pending queue
func (p *DeadLetterQueueProcessor) RetryArchivedTask(queueName, taskID string) error {
	err := p.inspector.RunTask(queueName, taskID)
	if err != nil {
		return fmt.Errorf("failed to retry archived task: %w", err)
	}
	log.Printf("Retried archived task: queue=%s, id=%s", queueName, taskID)
	return nil
}

// RetryAllArchivedTasks retries all tasks in the dead letter queue
func (p *DeadLetterQueueProcessor) RetryAllArchivedTasks(queueName string) (int, error) {
	count, err := p.inspector.RunAllArchivedTasks(queueName)
	if err != nil {
		return 0, fmt.Errorf("failed to retry all archived tasks: %w", err)
	}
	log.Printf("Retried %d archived tasks from queue %s", count, queueName)
	return count, nil
}

// DeleteArchivedTask permanently removes a task from the archive
func (p *DeadLetterQueueProcessor) DeleteArchivedTask(queueName, taskID string) error {
	err := p.inspector.DeleteTask(queueName, taskID)
	if err != nil {
		return fmt.Errorf("failed to delete archived task: %w", err)
	}
	log.Printf("Deleted archived task: queue=%s, id=%s", queueName, taskID)
	return nil
}

// DeleteAllArchivedTasks clears the entire dead letter queue
func (p *DeadLetterQueueProcessor) DeleteAllArchivedTasks(queueName string) (int, error) {
	count, err := p.inspector.DeleteAllArchivedTasks(queueName)
	if err != nil {
		return 0, fmt.Errorf("failed to delete all archived tasks: %w", err)
	}
	log.Printf("Deleted %d archived tasks from queue %s", count, queueName)
	return count, nil
}

// ArchiveStats holds statistics about the dead letter queue
type ArchiveStats struct {
	QueueName      string
	ArchivedCount  int
	OldestTaskTime time.Time
	TasksByType    map[string]int
}

// GetArchiveStats retrieves statistics about archived tasks
func (p *DeadLetterQueueProcessor) GetArchiveStats(queueName string) (*ArchiveStats, error) {
	info, err := p.inspector.GetQueueInfo(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue info: %w", err)
	}

	stats := &ArchiveStats{
		QueueName:     queueName,
		ArchivedCount: info.Archived,
		TasksByType:   make(map[string]int),
	}

	// Get archived tasks to analyze by type
	tasks, err := p.inspector.ListArchivedTasks(queueName, asynq.PageSize(100))
	if err != nil {
		return stats, nil // Return partial stats
	}

	for _, task := range tasks {
		stats.TasksByType[task.Type]++
		if stats.OldestTaskTime.IsZero() || task.LastFailedAt.Before(stats.OldestTaskTime) {
			stats.OldestTaskTime = task.LastFailedAt
		}
	}

	return stats, nil
}

// MonitorDeadLetterQueue periodically checks the DLQ and alerts if thresholds are exceeded
func (p *DeadLetterQueueProcessor) MonitorDeadLetterQueue(ctx context.Context, queueName string, threshold int) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := p.GetArchiveStats(queueName)
			if err != nil {
				log.Printf("Failed to get archive stats: %v", err)
				continue
			}

			if stats.ArchivedCount > threshold {
				log.Printf("ALERT: Dead letter queue %s has %d tasks (threshold: %d)",
					queueName, stats.ArchivedCount, threshold)

				// Send alert (integrate with monitoring system)
				p.sendAlert(stats)
			}
		}
	}
}

// SendAlert sends an alert about DLQ status (implement based on our alerting system)
func (p *DeadLetterQueueProcessor) sendAlert(stats *ArchiveStats) {
	// Example: Send to Slack, PagerDity, OneUptime, etc
	alertData, _ := json.MarshalIndent(stats, "", "  ")
	log.Printf("DLQ Alert:\n%s", alertData)
}
