package internal

import (
	"log"
	"sync"
)

type EntryParser[T any] interface {
	ParseFile(path string) (<-chan T, error)
}

type ImportController[T any] struct {
	store   EntryStore[T]
	parser  EntryParser[T]
	workers int
	wg      sync.WaitGroup
}

func NewImportController[T any](store EntryStore[T], parser EntryParser[T], workers int) *ImportController[T] {
	return &ImportController[T]{
		store:   store,
		parser:  parser,
		workers: workers,
		wg:      sync.WaitGroup{},
	}
}

func (c *ImportController[T]) Import(path string) error {
	channel, err := c.parser.ParseFile(path)
	if err != nil {
		return err
	}

	for i := 0; i < c.workers; i++ {
		log.Printf("Starting worker %d...", i)

		c.wg.Add(1)
		go c.worker(channel)
	}

	c.wg.Wait()
	return nil
}

func (c *ImportController[T]) worker(channel <-chan T) {
	for entry := range channel {
		if err := c.store.CreateEntry(&entry); err != nil {
			panic(err)
		}
	}

	c.wg.Done()
}
