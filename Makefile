GOPATH ?= $(HOME)/go

ifeq ($(OS), Windows_NT)
	PACKAGE = $(shell (Get-Content go.mod -head 1).Split(" ")[1])
else
	PACKAGE = $(shell head -1 go.mod | awk '{print $$2}')
endif

run-client:
	go run cmd/client/main.go

run-inspector:
	go run cmd/inspector/main.go

run-scheduler:
	go run cmd/scheduler/main.go

run-webui:
	go run cmd/webui/main.go

run-worker:
	go run cmd/worker/main.go