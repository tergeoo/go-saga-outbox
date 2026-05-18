package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-saga-outbox/inventory/internal/model"
	"go-saga-outbox/pkg/trx"

	"github.com/google/uuid"
)

var (
	ErrReservationNotFound error = fmt.Errorf("reservation not found")
)

type ReservationRepo struct {
	trx *trx.Transaction
}

func NewReservationRepo(trx *trx.Transaction) *ReservationRepo {
	return &ReservationRepo{
		trx: trx,
	}
}

func (r *ReservationRepo) Create(ctx context.Context, reservation model.Reservation) error {
	tx := r.trx.FromContext(ctx)

	query := `
	INSERT INTO reservation (id, saga_id, seat_id, status, created_at) 
	VALUES ($1, $2, $3, $4, $5)`

	_, err := tx.ExecContext(ctx, query, reservation.ID, reservation.SagaID, reservation.SeatID, model.ReservationStatusReserved, reservation.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert reservation: %w", err)
	}

	return nil
}

func (r *ReservationRepo) FindBySagaID(ctx context.Context, sagaID uuid.UUID) (model.Reservation, error) {
	tx := r.trx.FromContext(ctx)
	const query = `
	SELECT id, saga_id, seat_id, status, created_at 
	FROM reservation
	WHERE saga_id = $1
	LIMIT 1`

	var reservation model.Reservation
	err := tx.GetContext(ctx, &reservation, query, sagaID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Reservation{}, ErrReservationNotFound
		}
		return model.Reservation{}, fmt.Errorf("failed to fetch reservation: %w", err)
	}

	return reservation, nil
}

func (r *ReservationRepo) Update(ctx context.Context, reservation model.Reservation) error {
	tx := r.trx.FromContext(ctx)
	query := `UPDATE reservation SET status = $1 WHERE id = $2`

	_, err := tx.ExecContext(ctx, query, reservation.Status, reservation.ID)
	if err != nil {
		return fmt.Errorf("failed to update reservation: %w", err)
	}

	return nil
}
