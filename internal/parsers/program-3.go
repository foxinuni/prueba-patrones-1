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

type ProgramThreeParser struct {
	wg sync.WaitGroup
}

func NewProgramThreeParser() internal.EntryParser[internal.ProgramEntry] {
	return &ProgramThreeParser{}
}

func (p *ProgramThreeParser) ParseFile(path string) (<-chan internal.ProgramEntry, error) {
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

func (p *ProgramThreeParser) EntryWorker(incomming <-chan []string, outgoing chan<- internal.ProgramEntry) {
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

func (p *ProgramThreeParser) ParseEntry(entry []string) (*internal.ProgramEntry, error) {
	// parse location
	var location int
	switch entry[0] {
	case "":
		location = internal.LocationUnknown
	case "Usaquén":
		location = internal.LocationUsaquen
	case "Chapinero":
		location = internal.LocationChapinero
	case "Santa Fe":
		location = internal.LocationSantaFe
	case "San Cristóbal":
		location = internal.LocationSanCristobal
	case "Usme":
		location = internal.LocationUsme
	case "Tunjuelito":
		location = internal.LocationTunjuelito
	case "Bosa":
		location = internal.LocationBosa
	case "Kennedy":
		location = internal.LocationKennedy
	case "Fontibón":
		location = internal.LocationFontibon
	case "Engativá":
		location = internal.LocationEngativa
	case "Suba":
		location = internal.LocationSuba
	case "Barrios Unidos":
		location = internal.LocationBarriosUnidos
	case "Teusaquillo":
		location = internal.LocationTeusaquillo
	case "Los Mártires":
		location = internal.LocationLosMartires
	case "Antonio Nariño":
		location = internal.LocationAntonioNarino
	case "Puente Aranda":
		location = internal.LocationPuenteAranda
	case "La Candelaria":
		location = internal.LocationLaCandelaria
	case "Rafael Uribe Uribe":
		location = internal.LocationRafaelUribe
	case "Ciudad Bolivar":
		location = internal.LocationCiudadBolivar
	case "Sumapaz":
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

	var sex int
	if strings.HasPrefix(entry[5], "2") {
		sex = internal.SexFemale
	} else if strings.HasPrefix(entry[5], "1") {
		sex = internal.SexMale
	} else if strings.HasPrefix(entry[5], "3") {
		sex = internal.SexNonBinary
	} else if entry[5] == "" {
		sex = internal.SexUnknown
	} else {
		sex = internal.SexOther
	}

	// parse date
	date, err := time.Parse("20060102", strings.Split(entry[7], " ")[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing date: %q", entry[7])
	}

	// parse age
	birthday, err := time.Parse("2006-1-2", strings.Split(entry[3], " ")[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing birthday: %q", entry[3])
	}

	age := date.Year() - birthday.Year()

	return &internal.ProgramEntry{
		Program:  3,
		Location: location,
		EPS:      insurer,
		Sex:      sex,
		Age:      age,
		Date:     date,
	}, nil
}
