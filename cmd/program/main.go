package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/foxinuni/prueba-patrones/internal"
	"github.com/foxinuni/prueba-patrones/internal/parsers"
	"github.com/jackc/pgx/v5/pgxpool"
)

var databaseUrl string
var filepath string
var program int

func init() {
	flag.StringVar(&databaseUrl, "db", "postgres://postgres:postgres@localhost/postgres", "Postgres URI")
	flag.StringVar(&filepath, "file", "", "Path to import file")
	flag.IntVar(&program, "prog", -1, "Number of program to parse as")
	flag.Parse()

	if filepath == "" {
		panic("-file argument must be set")
	}

	if program == -1 {
		panic("-prog argument must be set")
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
	store := internal.NewPgBufferedProgramStore(pool, 500, 30*time.Second)

	// create parser
	parser, err := CreateParser(program)
	if err != nil {
		panic(err)
	}

	// create import controller
	controller := internal.NewImportController(store, parser, 8)

	// yeet the data to the database >:D
	if err := controller.Import(filepath); err != nil {
		panic(err)
	}

	// close the buffered store
	if buffered, ok := store.(*internal.PgBufferedProgramStore); ok {
		buffered.Close()
	}

	fmt.Println("Done!")
}

func CreateParser(program int) (internal.EntryParser[internal.ProgramEntry], error) {
	switch program {
	case 1:
		return parsers.NewProgramOneParser(), nil
	case 2:
		return parsers.NewProgramTwoParser(), nil
	case 3:
		return parsers.NewProgramThreeParser(), nil
	case 4:
		return parsers.NewProgramFourParser(), nil
	default:
		return nil, errors.New("program not supported")
	}
}
