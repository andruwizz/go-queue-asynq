package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hibiken/asynq"
)

// EmailHandler processes email delivery tasks.
// Implements the asynq.Handler interface.
type EmailHandler struct {
	// emailClient *sendGrid.Client
}

func (h *EmailHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload EmailDeliveryPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		// Return asynq.SkipRetry to indicate that this task should not be retried.
		return fmt.Errorf("failed to unmarshal email delivery payload: %w: %v", asynq.SkipRetry, err)
	}

	log.Printf("Processing email delivery: to=%s, subject=%s", payload.To, payload.Subject)

	// Simulate email sending
	// err := h.emailClient.Send(payload.To, payload.Subject, payload.Body)
	time.Sleep(100 * time.Millisecond) // Simulate sending delay

	log.Printf("Email sent successfully to %s", payload.To)
	return nil
}

// ImageHandler processes image resize tasks.
type ImageHandler struct {
	// imageProcessor *imaging.Processor
}

// ProcessTask handles image resizing
func (h *ImageHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload ImageResizePayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		// Return asynq.SkipRetry to indicate that this task should not be retried.
		return fmt.Errorf("failed to unmarshal image resize payload: %w: %v", asynq.SkipRetry, err)
	}

	log.Printf("Processing image resize: id=%s,dimensions=%dx%d", payload.ImageID, payload.TargetWidth, payload.TargetHeight)

	// Check context for cancellation (important for long-running tasks)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Simulate image resizing
	time.Sleep(500 * time.Millisecond)

	log.Printf("Image %s resized successfully", payload.ImageID)

	return nil
}

// WebhookHandler processes webhook delivery tasks.
type WebhookHandler struct {
	httpClient *http.Client
}

// NewWebhookHandler creates a new webhook handler with a configured HTTP client.
func NewWebhookHandler() *WebhookHandler {
	return &WebhookHandler{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ProcessTask handles webhook delivery tasks.
func (h *WebhookHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload WebhookDeliveryPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		// Return asynq.SkipRetry to indicate that this task should not be retried.
		return fmt.Errorf("failed to unmarshal payload: %w: %v", asynq.SkipRetry, err)
	}

	log.Printf("Delivering webhook: id=%s, url=%s", payload.WebhookID, payload.URL)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, payload.Method, payload.URL, bytes.NewReader(payload.Body))
	if err != nil {
		// Return asynq.SkipRetry to indicate that this task should not be retried.
		return fmt.Errorf("failed to create request: %w: %v", asynq.SkipRetry, err)
	}

	// Add headers
	for key, value := range payload.Headers {
		req.Header.Set(key, value)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		// Network errors should be retried
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode >= 500 {
		// Server errors should be retried
		return fmt.Errorf("webhook returned server error: %d", resp.StatusCode)
	}

	if resp.StatusCode >= 400 {
		// Client errors should not be retried
		return fmt.Errorf("webhook returned client error: %d: %w", resp.StatusCode, asynq.SkipRetry)
	}

	log.Printf("Webhook %s delivered successfully", payload.WebhookID)

	return nil
}

// ReportHandler processes report generation tasks.
type ReportHandler struct{}

// ProcessTask handles report generation.
func (h *ReportHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload ReportGenerationPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		// Return asynq.SkipRetry to indicate that this task should not be retried.
		return fmt.Errorf("failed to unmarshal payload: %w: %v", asynq.SkipRetry, err)
	}

	log.Printf("Generating report: id=%s, type=%s, format=%s", payload.ReportID, payload.ReportType, payload.Format)

	// Report generation might take a while; check for cancellation periodically
	for i := 0; i < 5; i++ {
		select {
		case <-ctx.Done():
			// Task was cancelled
			log.Printf("Report generation cancelled for %s", payload.ReportID)
			return ctx.Err()
		default:
			// Simulate work
			time.Sleep(200 * time.Millisecond)
		}
	}

	log.Printf("Report %s generated successfully", payload.ReportID)

	return nil
}
