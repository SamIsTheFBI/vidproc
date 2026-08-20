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

func (q *Queue) closeSubscribers(jobID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, ch := range q.subscribers[jobID] {
		close(ch)
	}
	delete(q.subscribers, jobID)
}

func (q *Queue) runTranscode(ctx context.Context, job Job) error {
	jobOutputDir := filepath.Join(job.OutputDir, filepath.Base(job.InputPath))

	progress, errs := transcoder.TranscodeAll(
		ctx, job.InputPath, jobOutputDir, transcoder.DefaultRenditions,
	)

	go func() {
		for p := range progress {
			p.JobID = job.ID
			q.broadcast(p)
		}
	}()

	for err := range errs {
		if err != nil {
			return err
		}
	}

	var srtPath string
	if q.transcriber != nil {
		result, err := transcriber.Transcribe(ctx, *q.transcriber, job.InputPath, jobOutputDir)
		if err != nil {
			log.Printf("job %s: transcription failed (continuing): %v", job.ID, err)
		} else {
			log.Printf("job %s: transcribed %d segments", job.ID, len(result.Segments))
			srtPath = result.SubtitlePath
		}
	}

	if srtPath != "" {
		if err := transcoder.WriteSubtitlePlaylist(jobOutputDir, srtPath); err != nil {
			log.Printf("job %s: subtitle playlist failed: %v", job.ID, err)
			srtPath = "" // don't reference it in master if it failed
		}
	}

	if err := transcoder.WriteMasterPlaylist(jobOutputDir, transcoder.DefaultRenditions, srtPath); err != nil {
		return fmt.Errorf("master playlist: %w", err)
	}

	log.Printf("job %s: HLS output ready at %s/master.m3u8", job.ID, jobOutputDir)
	return nil
}

func (q *Queue) processWithRetry(ctx context.Context, job Job, workerId int) {
	log.Printf("worker %d: starting job %s", workerId, job.ID)
	q.store.UpdateStatus(job.ID, StatusRunning, "")
	defer q.closeSubscribers(job.ID)

	for a := 0; a <= maxRetries; a++ {
		if a > 0 {
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

	q.store.UpdateStatus(job.ID, StatusFailed, fmt.Sprintf("failed after %d attempts", maxRetries+1))
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
