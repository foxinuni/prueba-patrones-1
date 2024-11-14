package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/foxinuni/prueba-patrones/internal"
	"github.com/foxinuni/prueba-patrones/internal/parsers"
	"github.com/jackc/pgx/v5/pgxpool"
)

var databaseUrl string
var filepath string

func init() {
	flag.StringVar(&databaseUrl, "db", "postgres://postgres:postgres@localhost/postgres", "Postgres URI")
	flag.StringVar(&filepath, "file", "", "Path to import file")
	flag.Parse()

	if filepath == "" {
		panic("-file argument must be set")
	}
}

func main() {
	// create pgx pool
	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		panic(err)
	}

	// ping database
	if err := pool.Ping(context.Background()); err != nil {
		panic(err)
	}

	// create store
	store := internal.NewPgBufferedPopulationStore(pool, 500, 30*time.Second)

	// create parser
	parser := parsers.NewPopulationParser()

	// create import controller
	controller := internal.NewImportController(store, parser, 8)

	// Start import
	if err := controller.Import(filepath); err != nil {
		panic(err)
	}

	if buffered, ok := store.(*internal.PgBufferedPopulationStore); ok {
		buffered.Close()
	}

	fmt.Println("Done!")
}
