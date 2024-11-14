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

type ProgramOneParser struct {
	wg sync.WaitGroup
}

func NewProgramOneParser() internal.EntryParser[internal.ProgramEntry] {
	return &ProgramOneParser{}
}

func (p *ProgramOneParser) ParseFile(path string) (<-chan internal.ProgramEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// create reader
	reader := csv.NewReader(file)

	// make the reading thread
	outgoing := make(chan internal.ProgramEntry)
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

func (p *ProgramOneParser) EntryWorker(incomming <-chan []string, outgoing chan<- internal.ProgramEntry) {
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

func (p *ProgramOneParser) ParseEntry(entry []string) (*internal.ProgramEntry, error) {
	// parse location
	var location int
	switch entry[1] {
	case "":
		location = internal.LocationUnknown
	case "Usaquen":
		location = internal.LocationUsaquen
	case "Chapinero":
		location = internal.LocationChapinero
	case "Santa Fe":
		location = internal.LocationSantaFe
	case "San Cristobal":
		location = internal.LocationSanCristobal
	case "Usme":
		location = internal.LocationUsme
	case "Tunjuelito":
		location = internal.LocationTunjuelito
	case "Bosa":
		location = internal.LocationBosa
	case "Kennedy":
		location = internal.LocationKennedy
	case "Fontibon":
		location = internal.LocationFontibon
	case "Engativa":
		location = internal.LocationEngativa
	case "Suba":
		location = internal.LocationSuba
	case "Barrios Unidos":
		location = internal.LocationBarriosUnidos
	case "Teusaquillo":
		location = internal.LocationTeusaquillo
	case "Los Martires":
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
		return nil, fmt.Errorf("unknown district %q", entry[1])
	}

	insurers := map[string]int{
		"NO AFILIADO":   internal.EpsNone,
		"NINGUNA":       internal.EpsNone,
		"CAPITAL SALUD": internal.EpsCapitalSalud,
		"SALUD TOTAL":   internal.EpsSaludTotal,
		"NUEVA EPS":     internal.EpsNuevaEPS,
		"SURAMERICANA":  internal.EpsSuramericana,
		"FERROCARRILES": internal.EpsFerrocarriles,
		"SALUD BOLIVAR": internal.EpsSaludBolivar,
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
	switch entry[4] {
	case "MASCULINO":
		sex = internal.SexMale
	case "FEMENINO":
		sex = internal.SexFemale
	default:
		return nil, fmt.Errorf("unknown gender %q", entry[4])
	}

	// parse date
	date, err := time.Parse("2/1/2006", entry[5])
	if err != nil {
		return nil, fmt.Errorf("error parsing date: %q", entry[5])
	}

	// parse age
	birthday, err := time.Parse("2/1/2006", entry[3])
	if err != nil {
		return nil, fmt.Errorf("error parsing birthday: %q", entry[3])
	}

	age := date.Year() - birthday.Year()

	return &internal.ProgramEntry{
		Program:  1,
		Location: location,
		EPS:      insurer,
		Sex:      sex,
		Age:      age,
		Date:     date,
	}, nil
}
