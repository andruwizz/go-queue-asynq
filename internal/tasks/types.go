package tasks

const (
	// TypeEmailDelivery is for delivering emails.
	TypeEmailDelivery = "email:deliver"

	// TypeImageResize is for resizing images.
	TypeImageResize = "image:resize"

	// TypeWebhookDelivery is for sending webhooks.
	TypeWebhookDelivery = "webhook:deliver"

	// TypeReportGeneration is for generating PDF/Excel reports.
	TypeReportGeneration = "report:generate"

	// TypeDataSync is for synchronizing data between systems.
	TypeDataSync = "data:sync"
)
