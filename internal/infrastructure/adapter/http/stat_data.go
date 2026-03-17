package http

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/go-cart/internal/domain/model"
)

// About list cartitem
func (h *HttpRouters) ListCartItemWindow(rw http.ResponseWriter, req *http.Request) error {
	
	ctx, cancel, span := h.withContext(req, "ListCartItemWindow")
	defer cancel()
	defer span.End()

	// decode payload			
	vars := mux.Vars(req)
	sku := vars["id"]

	cartitem := model.CartItem{Product: model.Product{Sku: sku}}

	res, err := h.workerService.ListCartItemWindow(ctx, &cartitem)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}
