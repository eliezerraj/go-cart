package http

import (
	"fmt"
	"time"
	"reflect"
	"net/http"
	"context"
	"strings"
	"strconv"
	"encoding/json"	

	"github.com/rs/zerolog"
	"github.com/gorilla/mux"

	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"
	"github.com/go-cart/internal/domain/service"

	go_core_midleware "github.com/eliezerraj/go-core/v2/middleware"
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
)

var (
	coreMiddleWareApiError	go_core_midleware.APIError
	coreMiddleWareWriteJSON	go_core_midleware.MiddleWare

	tracerProvider go_core_otel_trace.TracerProvider
)

type HttpRouters struct {
	workerService 	*service.WorkerService
	appServer		*model.AppServer
	logger			*zerolog.Logger
}

// Type for async result
type result struct {
		data interface{}
		err  error
}

// Above create routers
func NewHttpRouters(appServer *model.AppServer,
					workerService *service.WorkerService,
					appLogger *zerolog.Logger) HttpRouters {
	logger := appLogger.With().
						Str("package", "adapter.http").
						Logger()

	logger.Info().
			Str("func","NewHttpRouters").Send()

	return HttpRouters{
		workerService: workerService,
		appServer: appServer,
		logger: &logger,
	}
}

// About handle error
func (h *HttpRouters) ErrorHandler(trace_id string, err error) *go_core_midleware.APIError {

	var httpStatusCode int = http.StatusInternalServerError

	if strings.Contains(err.Error(), "context deadline exceeded") {
    	httpStatusCode = http.StatusGatewayTimeout
	}

	if strings.Contains(err.Error(), "check parameters") {
    	httpStatusCode = http.StatusBadRequest
	}

	if strings.Contains(err.Error(), "not found") {
    	httpStatusCode = http.StatusNotFound
	}

	if strings.Contains(err.Error(), "duplicate key") || 
	   strings.Contains(err.Error(), "unique constraint") {
   		httpStatusCode = http.StatusBadRequest
	}

	coreMiddleWareApiError = coreMiddleWareApiError.NewAPIError(err, 
																trace_id, 
																httpStatusCode)

	return &coreMiddleWareApiError
}

// About return a health
func (h *HttpRouters) Health(rw http.ResponseWriter, req *http.Request) {
	json.NewEncoder(rw).Encode(model.MessageRouter{Message: "true"})
}

// About return a live
func (h *HttpRouters) Live(rw http.ResponseWriter, req *http.Request) {
	json.NewEncoder(rw).Encode(model.MessageRouter{Message: "true"})
}

// About show all header received
func (h *HttpRouters) Header(rw http.ResponseWriter, req *http.Request) {
	h.logger.Info().
			Str("func","Header").Send()
	
	json.NewEncoder(rw).Encode(req.Header)
}

// About show all context values
func (h *HttpRouters) Context(rw http.ResponseWriter, req *http.Request) {
	h.logger.Info().
			Str("func","Context").Send()
	
	contextValues := reflect.ValueOf(req.Context()).Elem()

	json.NewEncoder(rw).Encode(fmt.Sprintf("%v",contextValues))
}

// About info
func (h *HttpRouters) Info(rw http.ResponseWriter, req *http.Request) {
	// extract context		
	ctx, cancel := context.WithTimeout(req.Context(), 
										time.Duration(h.appServer.Server.CtxTimeout) * time.Second)
    defer cancel()

	// log with context
	h.logger.Info().
			Ctx(ctx).
			Str("func","Info").Send()

	// trace	
	ctx, span := tracerProvider.SpanCtx(ctx, "adapter.http.Info")
	defer span.End()

	json.NewEncoder(rw).Encode(h.appServer)
}

// About add cart and cart itens
func (h *HttpRouters) AddCart(rw http.ResponseWriter, req *http.Request) error {
	// extract context	
	ctx, cancel := context.WithTimeout(req.Context(), time.Duration(h.appServer.Server.CtxTimeout) * time.Second)
    defer cancel()

	h.logger.Info().
			Ctx(ctx).
			Str("func","AddCart").Send()

	// trace and log	
	ctx, span := tracerProvider.SpanCtx(ctx, "adapter.http.AddCart")
	defer span.End()
	
	// decode payload			
	cart := model.Cart{}
	err := json.NewDecoder(req.Body).Decode(&cart)
    if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, erro.ErrBadRequest)
    }
	defer req.Body.Close()

	// call service
	res, err := h.workerService.AddCart(ctx, &cart)
	if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, err)
	}
	
	return coreMiddleWareWriteJSON.WriteJSON(rw, http.StatusOK, res)
}

// About get cart and cart itens
func (h *HttpRouters) GetCart(rw http.ResponseWriter, req *http.Request) error {
	// extract context		
	ctx, cancel := context.WithTimeout(req.Context(), time.Duration(h.appServer.Server.CtxTimeout) * time.Second)
    defer cancel()

	// log with context
	h.logger.Info().
			Ctx(ctx).
			Str("func","GetCart").Send()

	// trace	
	ctx, span := tracerProvider.SpanCtx(ctx, "adapter.http.GetCart")
	defer span.End()

	vars := mux.Vars(req)
	varID := vars["id"]

	varIDint, err := strconv.Atoi(varID)
    if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, erro.ErrBadRequest)
    }

	cart := model.Cart{ID: varIDint}

	res, err := h.workerService.GetCart(ctx, &cart)
	if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, err)
	}
	
	return coreMiddleWareWriteJSON.WriteJSON(rw, http.StatusOK, res)
}

// About update cart
func (h *HttpRouters) UpdateCart(rw http.ResponseWriter, req *http.Request) error {
	// extract context	
	ctx, cancel := context.WithTimeout(req.Context(), time.Duration(h.appServer.Server.CtxTimeout) * time.Second)
    defer cancel()

	h.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCart").Send()

	// trace	
	ctx, span := tracerProvider.SpanCtx(ctx, "adapter.http.UpdateCart")
	defer span.End()
	
	// decode payload	
	cart := model.Cart{}
	err := json.NewDecoder(req.Body).Decode(&cart)
    if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, erro.ErrBadRequest)
    }
	defer req.Body.Close()

	// get put parameter		
	vars := mux.Vars(req)
	varID := vars["id"]

	varIDint, err := strconv.Atoi(varID)
    if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, erro.ErrBadRequest)
    }

	cart.ID = varIDint

	// call service	
	res, err := h.workerService.UpdateCart(ctx, &cart)
	if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, err)
	}
	
	return coreMiddleWareWriteJSON.WriteJSON(rw, http.StatusOK, res)
}

// About update cartItem
func (h *HttpRouters) UpdateCartItem(rw http.ResponseWriter, req *http.Request) error {
	// extract context	
	ctx, cancel := context.WithTimeout(req.Context(), time.Duration(h.appServer.Server.CtxTimeout) * time.Second)
    defer cancel()

	h.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCartItem").Send()

	// trace	
	ctx, span := tracerProvider.SpanCtx(ctx, "adapter.http.UpdateCartItem")
	defer span.End()
	
	// decode payload	
	cartItem := model.CartItem{}
	err := json.NewDecoder(req.Body).Decode(&cartItem)
    if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, erro.ErrBadRequest)
    }
	defer req.Body.Close()

	// get put parameter		
	vars := mux.Vars(req)
	varID := vars["id"]

	varIDint, err := strconv.Atoi(varID)
    if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, erro.ErrBadRequest)
    }

	cartItem.ID = varIDint

	// call service	
	res, err := h.workerService.UpdateCartItem(ctx, &cartItem)
	if err != nil {
		trace_id := fmt.Sprintf("%v",ctx.Value("request-id"))
		return h.ErrorHandler(trace_id, err)
	}
	
	return coreMiddleWareWriteJSON.WriteJSON(rw, http.StatusOK, res)
}
