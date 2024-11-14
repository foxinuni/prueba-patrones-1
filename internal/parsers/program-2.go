package parsers

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/foxinuni/prueba-patrones/internal"
)

type ProgramTwoParser struct {
	wg sync.WaitGroup
}

func NewProgramTwoParser() internal.EntryParser[internal.ProgramEntry] {
	return &ProgramTwoParser{}
}

func (p *ProgramTwoParser) ParseFile(path string) (<-chan internal.ProgramEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// create reader
	reader := csv.NewReader(file)
	reader.Comma = '|'

	// make the reading thread
	outgoing := make(chan internal.ProgramEntry)
	incomming := make(chan []string, 100)

	// processesing thread
	for i := 0; i < 8; i++ {
		p.wg.Add(1)
		go p.EntryWorker(incomming, outgoing)
	}

	// reading thread
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

func (p *ProgramTwoParser) EntryWorker(incomming <-chan []string, outgoing chan<- internal.ProgramEntry) {
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

func (p *ProgramTwoParser) ParseEntry(entry []string) (*internal.ProgramEntry, error) {
	// parse location
	var location int
	switch entry[1] {
	case "":
		location = internal.LocationUnknown
	case "USAQUEN":
		location = internal.LocationUsaquen
	case "CHAPINERO":
		location = internal.LocationChapinero
	case "SANTA FE":
		location = internal.LocationSantaFe
	case "SAN CRISTOBAL":
		location = internal.LocationSanCristobal
	case "USME":
		location = internal.LocationUsme
	case "TUNJUELITO":
		location = internal.LocationTunjuelito
	case "BOSA":
		location = internal.LocationBosa
	case "KENNEDY":
		location = internal.LocationKennedy
	case "FONTIBON":
		location = internal.LocationFontibon
	case "ENGATIVA":
		location = internal.LocationEngativa
	case "SUBA":
		location = internal.LocationSuba
	case "BARRIOS UNIDOS":
		location = internal.LocationBarriosUnidos
	case "TEUSAQUILLO":
		location = internal.LocationTeusaquillo
	case "LOS MARTIRES":
		location = internal.LocationLosMartires
	case "ANTONIO NARIÑO":
		location = internal.LocationAntonioNarino
	case "PUENTE ARANDA":
		location = internal.LocationPuenteAranda
	case "CANDELARIA":
		location = internal.LocationLaCandelaria
	case "RAFAEL URIBE URIBE":
		location = internal.LocationRafaelUribe
	case "CIUDAD BOLIVAR":
		location = internal.LocationCiudadBolivar
	case "SUMAPAZ":
		location = internal.LocationSumapaz
	default:
		return nil, fmt.Errorf("unknown district %q", entry[1])
	}

	insurers := map[string]int{
		"NINGUNA": internal.EpsNone,
	}
	var insurer int
	if entry[2] == "" {
		insurer = internal.EpsUnkown
	} else {
		found := false
		for key, val := range insurers {
			if strings.Contains(entry[2], key) {
				insurer = val
				found = true
				break
			}
		}

		if !found {
			log.Printf("unknown insurer: %q - defaulting to other", entry[2])
			insurer = internal.EpsOther
		}
	}

	var sex int
	switch strings.ToUpper(entry[0]) {
	case "HOMBRE":
		sex = internal.SexMale
	case "MUJER":
		sex = internal.SexFemale
	case "INTERSEXUAL":
		sex = internal.SexNonBinary
	case "1":
		sex = internal.SexMale
	case "2":
		sex = internal.SexFemale

	default:
		return nil, fmt.Errorf("unknown gender %q", entry[0])
	}

	// parse date
	date, err := time.Parse("2/1/2006", entry[7])
	if err != nil {
		return nil, fmt.Errorf("error parsing date: %q", entry[7])
	}

	// parse age
	birthday, err := time.Parse("2/1/2006", entry[3])
	if err != nil {
		return nil, fmt.Errorf("error parsing birthday: %q", entry[3])
	}

	age := date.Year() - birthday.Year()

	return &internal.ProgramEntry{
		Program:  2,
		Location: location,
		EPS:      insurer,
		Sex:      sex,
		Age:      age,
		Date:     date,
	}, nil
}
