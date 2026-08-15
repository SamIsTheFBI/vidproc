package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	"video-processing/internal/transcoder"

	"github.com/google/uuid"
)

const (
	maxRetries  = 3
	numWorkers  = 4
	queueBuffer = 100
)

type Queue struct {
	store  *Store
	jobs   chan Job
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func New(store *Store) *Queue {
	return &Queue{
		store: store,
		jobs:  make(chan Job, queueBuffer),
	}
}

func (q *Queue) runTranscode(ctx context.Context, job Job) error {
	progress, errs := transcoder.TranscodeAll(
		ctx, job.InputPath, job.OutputDir, transcoder.DefaultRenditions,
	)

	go func() {
		for range progress {
		}
	}()

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Queue) processWithRetry(ctx context.Context, job Job, workerId int) {
	log.Printf("worker %d: starting job %s", workerId, job.ID)
	q.store.UpdateStatus(job.ID, StatusRunning, "")

	// todo
	for a := 0; a <= maxRetries; a++ {
		if a > 0 {
			// exponential backoff
			wait := time.Duration(1<<a) * time.Second
			log.Printf("worker %d: retrying job %s in %s (attempt %d/%d)",
				workerId, job.ID, wait, a, maxRetries,
			)
			time.Sleep(wait)
		}

		err := q.runTranscode(ctx, job)
		if err == nil {
			q.store.UpdateStatus(job.ID, StatusDone, "")
			log.Printf("worker %d: job %s done", workerId, job.ID)
			return
		}

		if ctx.Err() != nil {
			q.store.UpdateStatus(job.ID, StatusPending, "")
			return
		}

		q.store.IncrementRetry(job.ID)
		log.Printf("worker %d: job %s attempt %d failed: %v", workerId, job.ID, a+1, err)
	}

	q.store.UpdateStatus(job.ID, StatusFailed, fmt.Sprintf("failed after %d attempts", maxRetries))
	log.Printf("worker %d: job %s permanently failed", workerId, job.ID)
}

func (q *Queue) worker(ctx context.Context, id int) {
	defer q.wg.Done()

	for job := range q.jobs {
		q.processWithRetry(ctx, job, id)
	}
}

// Start launches workers and re-enqueues any jobs that survived a crash
func (q *Queue) Start(ctx context.Context) error {
	workerCtx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	//reenqueue jobs that were running when crashed
	pending, err := q.store.GetPendingJobs()
	if err != nil {
		return fmt.Errorf("load pending jobs: %w", err)
	}

	for _, job := range pending {
		log.Printf("re-enqueuing recovered job: %s", job.ID)
		q.jobs <- job
	}

	// launch workders
	for i := 0; i < numWorkers; i++ {
		q.wg.Add(1)
		go q.worker(workerCtx, i)
	}

	return nil
}

func (q *Queue) Shutdown() {
	q.cancel()
	close(q.jobs)
	q.wg.Wait()
}

func (q *Queue) Submit(inputPath, outputDir string) (string, error) {
	job := Job{
		ID:        uuid.NewString(),
		InputPath: inputPath,
		OutputDir: outputDir,
	}

	if err := q.store.CreateJob(job); err != nil {
		return "", fmt.Errorf("create job: %w", err)
	}

	q.jobs <- job // blocks if queue is full — natural backpressure
	return job.ID, nil
}
