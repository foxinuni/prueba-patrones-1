package internal

import "time"

const (
	SexMale      = 0
	SexFemale    = 1
	SexNonBinary = 2
	SexOther     = 3
	SexUnknown   = 4
)

const (
	EpsUnkown        = 0
	EpsNone          = 1
	EpsOther         = 2
	EpsCapitalSalud  = 3
	EpsNuevaEPS      = 4
	EpsSaludTotal    = 5
	EpsSuramericana  = 6
	EpsFerrocarriles = 7
	EpsSaludBolivar  = 8
	EpsCompensar     = 9
	EpsSanitas       = 10
	EpsFamisanar     = 11
	EpsAlianSalud    = 12
	EpsCoosalud      = 13
	EpsSOS           = 14
	EspMallamas      = 15
)

const (
	LocationUsaquen       = 1
	LocationChapinero     = 2
	LocationSantaFe       = 3
	LocationSanCristobal  = 4
	LocationUsme          = 5
	LocationTunjuelito    = 6
	LocationBosa          = 7
	LocationKennedy       = 8
	LocationFontibon      = 9
	LocationEngativa      = 10
	LocationSuba          = 11
	LocationBarriosUnidos = 12
	LocationTeusaquillo   = 13
	LocationLosMartires   = 14
	LocationAntonioNarino = 15
	LocationPuenteAranda  = 16
	LocationLaCandelaria  = 17
	LocationRafaelUribe   = 18
	LocationCiudadBolivar = 19
	LocationSumapaz       = 20
	LocationUnknown       = 99
)

type ProgramEntry struct {
	Program  int
	Location int
	Sex      int
	EPS      int
	Age      int
	Date     time.Time
}


type PopulationEntry struct {
	Year       int
	Age        int
	Population int
	District   int
}
