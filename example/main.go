package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sknv/pqueue"
	"github.com/sknv/pqueue/storage/postgres"
)

func main() {
	// Database connection
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create connection pool
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatal("Failed to parse database URL:", err)
	}

	// Configure pool settings
	poolConfig.MaxConns = 20
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = 1 * time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test connection
	if err = db.Ping(ctx); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	// Use the default Postgres storage
	storage := postgres.NewStorage(db)

	// Create a queue with custom configuration
	config := &pqueue.QueueConfig{
		Poll: pqueue.PollConfig{
			BatchSize:    5,
			Concurrency:  5,
			PollInterval: time.Second * 3,
		},
		Processing: pqueue.ProcessingConfig{
			DbTimeout:      time.Second * 10,
			DefaultBackoff: time.Second * 30,
		},
		ColdCleanup: pqueue.CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Minute * 5,
			CleanupBatchSize:  5,
		},
		DeadCleanup: pqueue.CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Minute * 10,
			CleanupBatchSize:  5,
		},
	}

	queue := pqueue.NewQueue(storage, config)
	decoder := queue.Decoder()

	// Register job handlers
	queue.RegisterHandler("email", HandleEmailJob(decoder),
		pqueue.WithJobHandlerBackoffCalculator(CalculateBackoff))

	// Start the queue processor
	queue.Start(ctx)
	log.Println("Job queue started successfully")

	// Example: enqueue various jobs
	if err = enqueueExampleJobs(ctx, db, queue); err != nil {
		log.Printf("Failed to enqueue example jobs: %v", err)
	}

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down job queue...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err = queue.Stop(shutdownCtx); err != nil {
		log.Printf("Failed to stop job queue: %v", err)
	}

	log.Println("Job queue stopped")
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func HandleEmailJob(decoder pqueue.Decoder) pqueue.JobHandler {
	return func(ctx context.Context, job *pqueue.Job) error {
		var payload EmailJob
		if err := decoder(job.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal email job payload: %w", err)
		}

		log.Printf("Processing email job %s: Sending email to %s with subject: %s\n",
			job.ID, payload.To, payload.Subject)

		// Simulate email sending
		time.Sleep(100 * time.Millisecond)

		// Simulate occasional failures for testing retry logic
		if rand.N(3)%3 == 0 {
			log.Printf("Temporary email service error for job %s", job.ID)

			return fmt.Errorf("temporary email service error")
		}

		log.Printf("Email job %s completed successfully", job.ID)

		return nil
	}
}

func CalculateBackoff(attempt uint) time.Duration {
	return time.Minute * time.Duration(attempt)
}

// enqueueExampleJobs demonstrates various job enqueueing scenarios.
func enqueueExampleJobs(ctx context.Context, db *pgxpool.Pool, queue *pqueue.Queue) error {
	// 1. Immediate job with default priority
	emailJob := EmailJob{
		To:      "user@example.com",
		Subject: "Welcome!",
		Body:    "Welcome to our service!",
	}

	job1, err := queue.Enqueue(ctx, db, uuid.Must(uuid.NewV7()), "email", emailJob)
	if err != nil {
		return fmt.Errorf("failed to enqueue email job: %w", err)
	}

	log.Printf("Enqueued immediate email job with ID: %s", job1.ID)

	// 2. High priority job
	urgentEmailJob := EmailJob{
		To:      "admin@example.com",
		Subject: "Urgent: System Alert",
		Body:    "Critical system event detected",
	}

	job2, err := queue.Enqueue(ctx, db, uuid.Must(uuid.NewV7()), "email", urgentEmailJob,
		pqueue.WithJobPriority(10))
	if err != nil {
		return fmt.Errorf("failed to enqueue urgent email job: %w", err)
	}

	log.Printf("Enqueued high-priority email job with ID: %s", job2.ID)

	// 3. Batch of jobs with varying priorities
	for i := range 10 {
		batchEmailJob := EmailJob{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Newsletter #%d", i),
			Body:    "Here's your weekly newsletter",
		}

		priority := i % 3 // Priorities 0, 1, 2

		_, err := queue.Enqueue(ctx, db, uuid.Must(uuid.NewV7()), "email", batchEmailJob,
			pqueue.WithJobPriority(priority),
			pqueue.WithJobMaxAttempts(3),
			pqueue.WithJobScheduledAt(time.Now().Add(time.Duration(i)*time.Second*3)))
		if err != nil {
			log.Printf("Failed to enqueue batch email job %d: %v", i, err)

			continue
		}
	}

	log.Println("Enqueued batch of 10 email jobs with varying priorities")

	return nil
}
