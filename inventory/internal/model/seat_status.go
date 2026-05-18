package model

type SeatStatus string

const (
	SeatStatusReserved SeatStatus = "reserved"
	SeatStatusFree     SeatStatus = "free"
	SeatStatusSold     SeatStatus = "sold"
)
