package tasks

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
)

// EmailDeliveryPayload is the payload for the email delivery task.
type EmailDeliveryPayload struct {
	UserID      int64    `json:"user_id"`
	To          string   `json:"to"`
	Subject     string   `json:"subject"`
	Body        string   `json:"body"`
	TemplateID  string   `json:"template_id,omitempty"`
	Attachments []string `json:"attachments,omitempty"`
}

// NewEmailDeliveryTask creates a new email delivery task with the given payload.
// The primary way to enqueue email jobs.
func NewEmailDeliveryTask(payload EmailDeliveryPayload) (*asynq.Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal email payload: %w", err)
	}

	// asynq.Task is immutable and contains the task type and payload data.
	return asynq.NewTask(TypeEmailDelivery, data), nil
}

// ImageResizePayload is the payload for the image resize task.
type ImageResizePayload struct {
	ImageID      string `json:"image_id"`
	SourceURL    string `json:"source_url"`
	TargetWidth  int    `json:"target_width"`
	TargetHeight int    `json:"target_height"`
	Quality      int    `json:"quality"`
	Format       string `json:"format"` // jpeg, png, webp, etc.
}

// NewImageResizeTask creates a new image resize task with the given payload.
func NewImageResizeTask(payload ImageResizePayload) (*asynq.Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal image resize payload: %w", err)
	}

	return asynq.NewTask(TypeImageResize, data), nil
}

// WebhookDeliveryPayload is the payload for the webhook delivery task.
type WebhookDeliveryPayload struct {
	WebhookID  string            `json:"webhook_id"`
	URL        string            `json:"url"`
	Method     string            `json:"method"` // GET, POST, PUT, DELETE, etc.
	Headers    map[string]string `json:"headers"`
	Body       json.RawMessage   `json:"body"`
	RetryCount int               `json:"retry_count"`
}

// NewWebhookDeliveryTask creates a new webhook delivery task with the given payload.
func NewWebhookDeliveryTask(payload WebhookDeliveryPayload) (*asynq.Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal webhook delivery payload: %w", err)
	}

	return asynq.NewTask(TypeWebhookDelivery, data), nil
}

// ReportGenerationPayload is the payload for the report generation task.
type ReportGenerationPayload struct {
	ReportID   string         `json:"report_id"`
	ReportType string         `json:"report_type"`
	UserID     int64          `json:"user_id"`
	Parameters map[string]any `json:"parameters,omitempty"`
	Format     string         `json:"format"` // pdf, csv, xlsx, etc.
	StartDate  time.Time      `json:"start_date"`
	EndDate    time.Time      `json:"end_date"`
}

// NewReportGenerationTask creates a new report generation task with the given payload.
func NewReportGenerationTask(payload ReportGenerationPayload) (*asynq.Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal report generation payload: %w", err)
	}

	return asynq.NewTask(TypeReportGeneration, data), nil
}
