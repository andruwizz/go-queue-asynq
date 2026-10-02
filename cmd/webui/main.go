package main

import (
	"go-queue-asynq/internal/config"
	"log"
	"net/http"

	"github.com/hibiken/asynqmon"
)

func main() {
	// Create the Asynqmon handler
	h := asynqmon.New(asynqmon.Options{
		RootPath:         "/monitoring",
		RedisConnOpt:     config.GetRedisClientOpt(),
		PayloadFormatter: asynqmon.PayloadFormatterFunc(formatPayload),
		ResultFormatter:  asynqmon.ResultFormatterFunc(formatResult),
	})

	// Create a new HTTP server mux
	mux := http.NewServeMux()
	mux.Handle("/monitoring/", h)

	// Add a health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Println("Starting Asynqmon web UI on :8080/monitoring")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// formatPayload customizes how task payloads are displayed in the UI
func formatPayload(taskType string, payload []byte) string {
	return string(payload)
}

// formatResult customizes how task results are displayed in the UI
func formatResult(taskType string, result []byte) string {
	return string(result)
}
