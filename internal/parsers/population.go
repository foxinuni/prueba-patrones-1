package parsers

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/foxinuni/prueba-patrones/internal"
)

type PopulationParser struct {
	wg sync.WaitGroup
}

func NewPopulationParser() internal.EntryParser[internal.PopulationEntry] {
	return &PopulationParser{}
}

func (p *PopulationParser) ParseFile(path string) (<-chan internal.PopulationEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// create reader
	reader := csv.NewReader(file)
	reader.Comma = '|'

	// make the reading thread
	outgoing := make(chan internal.PopulationEntry)
	incomming := make(chan []string, 100)

	// processesing thread
	for i := 0; i < 8; i++ {
		p.wg.Add(1)
		go p.EntryWorker(incomming, outgoing)
	}

	go func() {
		defer file.Close()

		for {
			// read record from csv
			record, err := reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				log.Printf("error whilst reading record: %v", err)
				continue
			}

			// process the record
			incomming <- record
		}

		// close the incomming channel
		close(incomming)

		// wait for all data to be processed
		p.wg.Wait()

		// close the outgoing channel (done)
		close(outgoing)
	}()

	return outgoing, nil
}

func (p *PopulationParser) EntryWorker(incomming <-chan []string, outgoing chan<- internal.PopulationEntry) {
	for record := range incomming {
		// parse record
		entry, err := p.ParseEntry(record)
		if err != nil {
			log.Printf("error whilst parsing entry: %v", err)
			continue
		}

		// send to channel
		outgoing <- *entry
	}

	// notify of thread finishing
	p.wg.Done()
}

func (p *PopulationParser) ParseEntry(entry []string) (*internal.PopulationEntry, error) {
	// parse year
	year, err := strconv.Atoi(entry[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse year %q", entry[0])
	}

	// parse location
	district, err := strconv.Atoi(entry[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse district %q", entry[0])
	}

	// fix district
	if district == 0 {
		district = 99
	}

	// age
	age, err := strconv.Atoi(entry[4])
	if err != nil {
		return nil, fmt.Errorf("failed to parse age %q", entry[4])
	}

	// population
	population, err := strconv.Atoi(entry[7])
	if err != nil {
		return nil, fmt.Errorf("failed to parse population %q", entry[7])
	}

	return &internal.PopulationEntry{
		Year:       year,
		Age:        age,
		Population: population,
		District:   district,
	}, nil
}
