package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EntryStore[T any] interface {
	CreateEntry(entry *T) error
}

type PgProgramStore struct {
	pool *pgxpool.Pool
}

func NewPgProgramStore(pool *pgxpool.Pool) EntryStore[ProgramEntry] {
	return &PgProgramStore{
		pool: pool,
	}
}

func (s *PgProgramStore) CreateEntry(entry *ProgramEntry) error {
	_, err := s.pool.Exec(context.Background(), `
		INSERT INTO entries (age, program, insurer_id, district_id, gender_id, creation_date)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, entry.Age, entry.Program, entry.EPS, entry.Location, entry.Sex, entry.Date)

	return err
}

type PgBufferedProgramStore struct {
	pool         *pgxpool.Pool
	batch        *pgx.Batch
	maxBatchSize int
	timeout      time.Duration
	lastExecuted time.Time
	mu           sync.Mutex // Mutex to protect concurrent access to shared state
}

// NewPgBufferedProgramStore initializes a new PgBufferedEntryStore with a batch size limit and timeout.
func NewPgBufferedProgramStore(pool *pgxpool.Pool, maxBatchSize int, timeout time.Duration) EntryStore[ProgramEntry] {
	return &PgBufferedProgramStore{
		pool:         pool,
		batch:        &pgx.Batch{},
		maxBatchSize: maxBatchSize,
		timeout:      timeout,
		lastExecuted: time.Now(),
	}
}

// AddEntryToBatch adds an entry to the batch and checks if it's time to execute the batch.
func (s *PgBufferedProgramStore) CreateEntry(entry *ProgramEntry) error {
	// Lock the mutex to ensure only one goroutine can modify the batch at a time
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add the entry to the batch
	s.batch.Queue(`
		INSERT INTO entries (age, program, insurer_id, district_id, gender_id, creation_date)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, entry.Age, entry.Program, entry.EPS, entry.Location, entry.Sex, entry.Date)

	// Check if the batch size has reached the maximum or if the timeout has elapsed
	if s.batch.Len() >= s.maxBatchSize || time.Since(s.lastExecuted) >= s.timeout {
		if err := s.ExecuteBatch(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

// ExecuteBatch sends all the queued inserts in one batch.
func (s *PgBufferedProgramStore) ExecuteBatch(ctx context.Context) error {
	// Send the batch to the database
	br := s.pool.SendBatch(ctx, s.batch)
	defer br.Close()

	// Check for any error in the batch execution
	if err := br.Close(); err != nil {
		return fmt.Errorf("batch execution failed: %w", err)
	}

	// Clear the batch and reset the timestamp of last execution
	s.batch = &pgx.Batch{}
	s.lastExecuted = time.Now()

	return nil
}

func (s *PgBufferedProgramStore) Close() error {
	// Lock the mutex to ensure only one goroutine can modify the batch at a time
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ExecuteBatch(context.Background())
}

type PgBufferedPopulationStore struct {
	pool         *pgxpool.Pool
	batch        *pgx.Batch
	maxBatchSize int
	timeout      time.Duration
	lastExecuted time.Time
	mu           sync.Mutex // Mutex to protect concurrent access to shared state
}

// NewPgBufferedProgramStore initializes a new PgBufferedEntryStore with a batch size limit and timeout.
func NewPgBufferedPopulationStore(pool *pgxpool.Pool, maxBatchSize int, timeout time.Duration) EntryStore[PopulationEntry] {
	return &PgBufferedPopulationStore{
		pool:         pool,
		batch:        &pgx.Batch{},
		maxBatchSize: maxBatchSize,
		timeout:      timeout,
		lastExecuted: time.Now(),
	}
}

// AddEntryToBatch adds an entry to the batch and checks if it's time to execute the batch.
func (s *PgBufferedPopulationStore) CreateEntry(entry *PopulationEntry) error {
	// Lock the mutex to ensure only one goroutine can modify the batch at a time
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add the entry to the batch
	s.batch.Queue(`
		INSERT INTO population (year, age, population, district_id)
		VALUES ($1, $2, $3, $4)
	`, entry.Year, entry.Age, entry.Population, entry.District)

	// Check if the batch size has reached the maximum or if the timeout has elapsed
	if s.batch.Len() >= s.maxBatchSize || time.Since(s.lastExecuted) >= s.timeout {
		if err := s.ExecuteBatch(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

// ExecuteBatch sends all the queued inserts in one batch.
func (s *PgBufferedPopulationStore) ExecuteBatch(ctx context.Context) error {
	// Send the batch to the database
	br := s.pool.SendBatch(ctx, s.batch)
	defer br.Close()

	// Check for any error in the batch execution
	if err := br.Close(); err != nil {
		return fmt.Errorf("batch execution failed: %w", err)
	}

	// Clear the batch and reset the timestamp of last execution
	s.batch = &pgx.Batch{}
	s.lastExecuted = time.Now()

	return nil
}

func (s *PgBufferedPopulationStore) Close() error {
	// Lock the mutex to ensure only one goroutine can modify the batch at a time
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ExecuteBatch(context.Background())
}
