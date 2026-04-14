package http

import (
	"net/http"
	"strconv"
	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"
)

// About list cartitem
func (h *HttpRouters) ListCartItemWindow(rw http.ResponseWriter, req *http.Request) error {
	
	ctx, cancel, span := h.withContext(req, "ListCartItemWindow")
	defer cancel()
	defer span.End()

	query := req.URL.Query()
	sku := query.Get("sku")
	if sku == "" {
		return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
	}

	// default window is 24, can be override by query parameter
	window := 24
	windowParam := query.Get("window")
	if windowParam != "" {
		parsedWindow, err := strconv.Atoi(windowParam)
		if err != nil || parsedWindow <= 0 {
			return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
		}
		window = parsedWindow
	}

	cartitem := model.CartItem{Product: model.Product{Sku: sku}}

	res, err := h.workerService.ListCartItemWindow(ctx, window, &cartitem)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}
