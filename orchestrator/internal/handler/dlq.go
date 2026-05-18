package handler

import (
	"errors"
	"log/slog"
	"net/http"

	"go-saga-outbox/orchestrator/internal/service"
	"go-saga-outbox/pkg/dlq"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type DLQHandler struct {
	sagaService *service.SagaService
}

func NewDLQHandler(s *service.SagaService) *DLQHandler {
	return &DLQHandler{sagaService: s}
}

func (h *DLQHandler) Replay(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "invalid id"})
	}

	err = h.sagaService.ReplayDeadMessage(c.Request().Context(), id)
	switch {
	case err == nil:
		return c.NoContent(http.StatusAccepted)
	case errors.Is(err, dlq.ErrDeadMessageNotFound):
		return c.JSON(http.StatusNotFound, echo.Map{"error": "not found"})
	case errors.Is(err, dlq.ErrDeadMessageAlreadyReplayed):
		return c.JSON(http.StatusConflict, echo.Map{"error": "already replayed"})
	case errors.Is(err, dlq.ErrDeadMessageCannotReplay):
		return c.JSON(http.StatusUnprocessableEntity, echo.Map{"error": "cannot replay: missing saga_id"})
	default:
		slog.Error("replay failed", "err", err, "id", id)
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "internal"})
	}
}

func (h *DLQHandler) RegisterRoutes(r *echo.Echo) {
	r.POST("/dead-messages/:id/replay", h.Replay)
}
