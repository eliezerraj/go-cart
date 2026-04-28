package http

import (
	"net/http"
	"encoding/json"	
	
	"github.com/gorilla/mux"

	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"

	"go.opentelemetry.io/otel/codes"	
)

// About add cart and cart itens
func (h *HttpRouters) AddCart(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "AddCart")
	defer cancel()
	defer span.End()
	
	// decode payload			
	cart := model.Cart{}
	err := json.NewDecoder(req.Body).Decode(&cart)
	defer req.Body.Close()

    if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
    }

	// call service
	res, err := h.workerService.AddCart(ctx, &cart)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}

	return h.writeJSON(rw, http.StatusCreated, res)
}

// About get cart and cart itens
func (h *HttpRouters) GetCart(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "GetCart")
	defer cancel()
	defer span.End()

	// decode payload
	vars := mux.Vars(req)
	cartID, err := h.parseIDParam(vars)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}

	cart := model.Cart{ID: cartID}

	res, err := h.workerService.GetCart(ctx, &cart)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}

	return h.writeJSON(rw, http.StatusOK, res)
}

// About update cart
func (h *HttpRouters) UpdateCart(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "UpdateCart")
	defer cancel()
	defer span.End()
	
	// decode payload	
	cart := model.Cart{}
	err := json.NewDecoder(req.Body).Decode(&cart)
	defer req.Body.Close()

	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
	}

	// get put parameter		
	vars := mux.Vars(req)
	cartID, err := h.parseIDParam(vars)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}

	cart.ID = cartID

	// call service	
	res, err := h.workerService.UpdateCart(ctx, &cart)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}

// About update cartItem
func (h *HttpRouters) UpdateCartItem(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "UpdateCartItem")
	defer cancel()
	defer span.End()
	
	// decode payload	
	cartItem := model.CartItem{}
	err := json.NewDecoder(req.Body).Decode(&cartItem)
	defer req.Body.Close()

	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
	}
	
	// get put parameter		
	vars := mux.Vars(req)
	cartItemID, err := h.parseIDParam(vars)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}

	cartItem.ID = cartItemID

	// call service	
	res, err := h.workerService.UpdateCartItem(ctx, &cartItem)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}
