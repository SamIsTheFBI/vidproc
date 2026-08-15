package queue

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type JobStatus string

const (
	StatusPending JobStatus = "pending"
	StatusRunning JobStatus = "running"
	StatusDone    JobStatus = "done"
	StatusFailed  JobStatus = "failed"
)

type Job struct {
	ID        string
	InputPath string
	OutputDir string
	Status    JobStatus
	Retries   int
	Error     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Store struct {
	db *sql.DB
}

func NewStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// wal mode: allows concurrent read while writing
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("wal mode: %w", err)
	}

	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
  		CREATE TABLE IF NOT EXISTS jobs (
            id          TEXT PRIMARY KEY,
            input_path  TEXT NOT NULL,
            output_dir  TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            retries     INTEGER NOT NULL DEFAULT 0,
            error       TEXT NOT NULL DEFAULT '',
            created_at  DATETIME NOT NULL,
            updated_at  DATETIME NOT NULL
        )

	`)

	return err
}

func (s *Store) CreateJob(job Job) error {
	_, err := s.db.Exec(`
		INSERT INTO jobs (id, input_path, output_dir, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		job.ID, job.InputPath, job.OutputDir,
		StatusPending, time.Now(), time.Now(),
	)

	return err
}

func (s *Store) UpdateStatus(id string, status JobStatus, errMsg string) error {
	_, err := s.db.Exec(`
		 UPDATE jobs SET status = ?, error = ?, updated_at = ?
		 WHERE id = ?`,
		status, errMsg, time.Now(), id,
	)

	return err
}

func (s *Store) IncrementRetry(id string) (int, error) {
	var retries int
	err := s.db.QueryRow(`
		UPDATE jobs SET retries = retries + 1, updated_at = ?
		WHERE id = ?
		RETURNING retries`,
		time.Now(), id,
	).Scan(&retries)

	return retries, err
}

func (s *Store) GetJob(id string) (Job, error) {
	var j Job
	err := s.db.QueryRow(`
		SELECT id, input_path, output_dir, status, retries, error, created_at, updated_at
		FROM jobs where id = ?`, id,
	).Scan(&j.ID, &j.InputPath, &j.OutputDir, &j.Status, &j.Retries, &j.Error, &j.CreatedAt, &j.UpdatedAt)

	if err == sql.ErrNoRows {
		return Job{}, fmt.Errorf("job %s not found", id)
	}

	return j, err
}

func (s *Store) GetPendingJobs() ([]Job, error) {
	rows, err := s.db.Query(`
		SELECT id, input_path, output_dir, retries
		FROM jobs WHERE status IN ('pending', 'running')
		ORDER BY created_at ASC`)

	if err != nil {
		return nil, err
	}

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.InputPath, &j.OutputDir, &j.Retries); err != nil {
			return nil, err
		}

		jobs = append(jobs, j)
	}

	return jobs, rows.Err()
}
