// Workflow Engine worker: polls the database and executes pending step_runs.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/SheykoWk/workflow-engine/internal/app/executor"
	"github.com/SheykoWk/workflow-engine/internal/app/ports"
	"github.com/SheykoWk/workflow-engine/internal/infrastructure/db"
	"github.com/SheykoWk/workflow-engine/internal/infrastructure/eventstore"
	"github.com/joho/godotenv"
)

func main() {
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Fatalf("load .env: %v", err)
		}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	sqlDB, err := db.OpenSQL(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer func() { _ = sqlDB.Close() }()

	var publisher ports.EventPublisher
	if esURL := os.Getenv("EVENT_STREAMING_BASE_URL"); esURL != "" {
		publisher = eventstore.NewHTTPClient(esURL, os.Getenv("EVENT_STREAMING_API_TOKEN"), logger)
		logger.Info("event publishing enabled", "url", esURL)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	stepRunRepo := db.NewStepRunRepository(sqlDB)
	executor.Start(ctx, stepRunRepo, publisher, logger)
	log.Printf("workflow worker started")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("shutting down worker...")
	stop()
	log.Printf("worker stopped")
}
