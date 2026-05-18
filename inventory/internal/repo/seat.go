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
	ErrNoFreeSeats  = errors.New("no free seats")
	ErrSeatNotFound = errors.New("seat not found")
)

type SeatRepo struct {
	trx *trx.Transaction
}

func NewSeatRepo(trx *trx.Transaction) *SeatRepo {
	return &SeatRepo{trx: trx}
}

// FindFreeSeat должен вызываться внутри trx.Transaction.run,
// иначе FOR UPDATE SKIP LOCKED отработает в auto-commit и не защитит от гонки.
func (r *SeatRepo) FindFreeSeat(ctx context.Context, eventID uuid.UUID) (model.Seat, error) {
	tx := r.trx.FromContext(ctx)

	const query = `                                       
	SELECT id, event_id, status                                                                                                                                                                                                                                                                                             
	FROM seat                               
	WHERE event_id = $1 AND status = $2                                                                                                                                                                                                                                                                                     
	LIMIT 1                                                                                                                                                                                                                                                                                                                 
	FOR UPDATE SKIP LOCKED`

	var seat model.Seat

	err := tx.GetContext(ctx, &seat, query, eventID, model.SeatStatusFree)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Seat{}, ErrNoFreeSeats
		}
		return model.Seat{}, fmt.Errorf("failed to fetch free seat: %w", err)
	}

	return seat, nil
}

func (r *SeatRepo) MarkSeatReserved(ctx context.Context, seatID uuid.UUID) error {
	tx := r.trx.FromContext(ctx)
	query := `
	UPDATE seat
	SET status = $1
	WHERE id = $2 AND status = $3`

	res, err := tx.ExecContext(ctx, query, model.SeatStatusReserved, seatID, model.SeatStatusFree)
	if err != nil {
		return fmt.Errorf("failed to mark free seat: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to fetch affected rows: %w", err)
	}

	if n == 0 {
		return ErrSeatNotFound
	}

	return nil
}

func (r *SeatRepo) MarkSeatReleased(ctx context.Context, seatID uuid.UUID) error {
	tx := r.trx.FromContext(ctx)
	query := `
	UPDATE seat
	SET status = $1
	WHERE id = $2`

	res, err := tx.ExecContext(ctx, query, model.SeatStatusFree, seatID)
	if err != nil {
		return fmt.Errorf("failed to mark free seat: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to fetch affected rows: %w", err)
	}

	if n == 0 {
		return ErrSeatNotFound
	}

	return nil
}
