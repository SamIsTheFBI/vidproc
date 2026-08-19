package queue

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
	"video-processing/internal/transcoder"
	"video-processing/internal/transcriber"

	"github.com/google/uuid"
)

const (
	maxRetries  = 3
	numWorkers  = 4
	queueBuffer = 100
)

type Queue struct {
	store       *Store
	jobs        chan Job
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	mu          sync.RWMutex
	subscribers map[string][]chan transcoder.Progress
	transcriber *transcriber.Config
}

func New(store *Store, tc *transcriber.Config) *Queue {
	return &Queue{
		store:       store,
		jobs:        make(chan Job, queueBuffer),
		subscribers: make(map[string][]chan transcoder.Progress),
		transcriber: tc,
	}
}

func (q *Queue) Subscribe(jobID string) (<-chan transcoder.Progress, func()) {
	ch := make(chan transcoder.Progress, 20)

	q.mu.Lock()
	q.subscribers[jobID] = append(q.subscribers[jobID], ch)
	q.mu.Unlock()

	cancel := func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		subs := q.subscribers[jobID]
		for i, s := range subs {
			if s == ch {
				q.subscribers[jobID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}

		close(ch)
	}

	return ch, cancel
}

func (q *Queue) broadcast(p transcoder.Progress) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for _, ch := range q.subscribers[p.JobID] {
		select {
		case ch <- p:
		default:
		}
	}
}

func (q *Queue) runTranscode(ctx context.Context, job Job) error {
	progress, errs := transcoder.TranscodeAll(
		ctx, job.InputPath, job.OutputDir, transcoder.DefaultRenditions,
	)

	go func() {
		for p := range progress {
			q.broadcast(p)
		}
	}()

	for err := range errs {
		if err != nil {
			return err
		}
	}

	if q.transcriber != nil {
		jobOutputDir := filepath.Join(job.OutputDir, filepath.Base(job.InputPath))

		result, err := transcriber.Transcribe(ctx, *q.transcriber, job.InputPath, jobOutputDir)
		if err != nil {
			// transcription failure is non-fatal — log and continue
			log.Printf("job %s: transcription failed (continuing): %v", job.ID, err)
			return nil
		}

		log.Printf("job %s: transcribed %d segments", job.ID, len(result.Segments))

		for _, r := range transcoder.DefaultRenditions {
			videoPath := filepath.Join(jobOutputDir,
				fmt.Sprintf("output_%s.mp4", r.Name))
			subtitledPath := filepath.Join(jobOutputDir,
				fmt.Sprintf("output_%s_subtitled.mp4", r.Name))

			if err := transcriber.EmbedSubtitles(ctx, videoPath, result.SRTPath, subtitledPath); err != nil {
				log.Printf("job %s: embed subtitles %s failed: %v", job.ID, r.Name, err)
			}
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
