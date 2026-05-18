package handler

import (
	"errors"
	"go-saga-outbox/orchestrator/internal/dto"
	"go-saga-outbox/orchestrator/internal/repo"
	"go-saga-outbox/orchestrator/internal/service"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type BookingHandler struct {
	service *service.SagaService
}

func NewBookingHandler(service *service.SagaService) *BookingHandler {
	return &BookingHandler{service: service}
}

func (h *BookingHandler) RegisterRoutes(router *echo.Echo) {
	router.POST("/bookings", h.create)
	router.GET("/bookings/:id", h.get)
}

func (h *BookingHandler) create(c echo.Context) error {
	var req dto.CreateBookingRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if req.EventID == uuid.Nil {
		return echo.NewHTTPError(http.StatusBadRequest, "event_id is required")
	}
	if req.UserID == uuid.Nil {
		return echo.NewHTTPError(http.StatusBadRequest, "user_id is required")
	}

	sagaID, err := h.service.StartBooking(c.Request().Context(), req.EventID, req.UserID, req.Channel, req.Amount)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusAccepted, dto.CreateBookingResponse{SagaID: sagaID})
}

func (h *BookingHandler) get(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid saga id")
	}

	saga, err := h.service.GetBooking(c.Request().Context(), id)
	if err != nil {
		if errors.Is(err, repo.ErrSagaNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "saga not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, dto.BookingResponse{
		SagaID:      saga.ID,
		Type:        saga.Type,
		State:       string(saga.State),
		CurrentStep: string(saga.CurrentStep),
		Payload:     saga.Payload,
		Context:     saga.Context,
		Attempts:    saga.Attempts,
		CreatedAt:   saga.CreatedAt,
		UpdatedAt:   saga.UpdatedAt,
	})
}
