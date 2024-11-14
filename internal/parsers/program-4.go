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

type ProgramFourParser struct {
	wg sync.WaitGroup
}

func NewProgramFourParser() internal.EntryParser[internal.ProgramEntry] {
	return &ProgramFourParser{}
}

func (p *ProgramFourParser) ParseFile(path string) (<-chan internal.ProgramEntry, error) {
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

func (p *ProgramFourParser) EntryWorker(incomming <-chan []string, outgoing chan<- internal.ProgramEntry) {
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

func (p *ProgramFourParser) ParseEntry(entry []string) (*internal.ProgramEntry, error) {
	// parse location
	var location int
	switch entry[0] {
	case "":
		location = internal.LocationUnknown
	case "1":
		location = internal.LocationUsaquen
	case "2":
		location = internal.LocationChapinero
	case "3":
		location = internal.LocationSantaFe
	case "4":
		location = internal.LocationSanCristobal
	case "5":
		location = internal.LocationUsme
	case "6":
		location = internal.LocationTunjuelito
	case "7":
		location = internal.LocationBosa
	case "8":
		location = internal.LocationKennedy
	case "9":
		location = internal.LocationFontibon
	case "10":
		location = internal.LocationEngativa
	case "11":
		location = internal.LocationSuba
	case "12":
		location = internal.LocationBarriosUnidos
	case "13":
		location = internal.LocationTeusaquillo
	case "14":
		location = internal.LocationLosMartires
	case "15":
		location = internal.LocationAntonioNarino
	case "16":
		location = internal.LocationPuenteAranda
	case "17":
		location = internal.LocationLaCandelaria
	case "18":
		location = internal.LocationRafaelUribe
	case "19":
		location = internal.LocationCiudadBolivar
	case "20":
		location = internal.LocationSumapaz
	default:
		return nil, fmt.Errorf("unknown district %q", entry[0])
	}

	insurers := map[string]int{
		"NO AFILIADO":   internal.EpsNone,
		"NINGUNA":       internal.EpsNone,
		"NO ASEGURADO":  internal.EpsNone,
		"CAPITAL SALUD": internal.EpsCapitalSalud,
		"SALUD TOTAL":   internal.EpsSaludTotal,
		"NUEVA EPS":     internal.EpsNuevaEPS,
		"SURAMERICANA":  internal.EpsSuramericana,
		"FERROCARRILES": internal.EpsFerrocarriles,
		"BOLIVAR":       internal.EpsSaludBolivar,
		"COMPENSAR":     internal.EpsCompensar,
		"SANITAS":       internal.EpsSanitas,
		"FAMISANAR":     internal.EpsFamisanar,
		"ALIANSALUD":    internal.EpsAlianSalud,
		"COOSALUD":      internal.EpsCoosalud,
		"ECOOPSOS":      internal.EpsSOS,
		"MALLAMAS":      internal.EspMallamas,
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

	// parse date
	date, err := time.Parse("2006-1-2", strings.Split(entry[6], " ")[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing date: %q", entry[6])
	}

	// parse age
	birthday, err := time.Parse("2006-1-2", strings.Split(entry[3], " ")[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing birthday: %q", entry[3])
	}

	age := date.Year() - birthday.Year()
	sex := internal.SexUnknown

	return &internal.ProgramEntry{
		Program:  4,
		Location: location,
		EPS:      insurer,
		Sex:      sex,
		Age:      age,
		Date:     date,
	}, nil
}
