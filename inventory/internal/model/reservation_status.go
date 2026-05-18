package model

type ReservationStatus string

const (
	ReservationStatusReserved ReservationStatus = "reserved"
	ReservationStatusReleased ReservationStatus = "released"
)

func (s ReservationStatus) String() string {
	return string(s)
}
