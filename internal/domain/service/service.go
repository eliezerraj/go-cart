package service

import (
	"fmt"
	"time"
	"context"
	"encoding/json"

	"github.com/rs/zerolog"

	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"

	database "github.com/go-cart/internal/infrastructure/repo/database"

	go_core_http "github.com/eliezerraj/go-core/v2/http"
	go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_midleware "github.com/eliezerraj/go-core/v2/middleware"
)

type WorkerService struct {
	workerRepository 	*database.WorkerRepository
	logger 				*zerolog.Logger
	tracerProvider 		*go_core_otel_trace.TracerProvider
	httpService			*go_core_http.HttpService
	endpoint			*[]model.Endpoint		 	
}

// about do http call 
func (s *WorkerService) doHttpCall(ctx context.Context,	httpClientParameter go_core_http.HttpClientParameter) (interface{}, error) {
	s.logger.Info().
			 Ctx(ctx).
			 Str("func","doHttpCall").Send()

	resPayload, statusCode, err := s.httpService.DoHttp(ctx, httpClientParameter)

	if err != nil {
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, err
	}

	s.logger.Debug().
		Interface("+++++++++++++++++> httpClientParameter.Url:",httpClientParameter.Url).
		Interface("+++++++++++++++++> resPayload:",resPayload).
		Interface("+++++++++++++++++> statusCode:",statusCode).
		Interface("+++++++++++++++++> err:", err).
		Send()

	switch (statusCode) {
		case 200:
			return resPayload, nil
		case 201:
			return resPayload, nil	
		case 400:
		case 401:
		case 403:
		case 404:
		case 500:
			newErr := fmt.Errorf("internal server error (status code %d) - (process: %s)", statusCode, httpClientParameter.Url)
			s.logger.Error().
				Ctx(ctx).
				Err(newErr).Send()
			return nil, newErr
		default:
	}

	// marshal response payload
	jsonString, err := json.Marshal(resPayload)
	if err != nil {
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to marshal http response: %w (process: %s)", err, httpClientParameter.Url)
	}

	// parse error message
	message := model.APIError{}
	if err := json.Unmarshal(jsonString, &message); err != nil {
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to unmarshal error response: %w (process: %s)", err, httpClientParameter.Url)
	}

	newErr := fmt.Errorf("%s - (status code %d) - (process: %s)", message.Msg,statusCode, httpClientParameter.Url)
	s.logger.Warn().
		Ctx(ctx).
		Err(newErr).Send()
		
	return nil, newErr
}

// About new worker service
func NewWorkerService(	workerRepository *database.WorkerRepository, 
						appLogger 		*zerolog.Logger,
						tracerProvider 	*go_core_otel_trace.TracerProvider,
						endpoint		*[]model.Endpoint	) *WorkerService{
							
	logger := appLogger.With().
				Str("package", "domain.service").
				Logger()
	logger.Info().
		Str("func","NewWorkerService").Send()

	httpService := go_core_http.NewHttpService(&logger)					

	return &WorkerService{
		workerRepository: workerRepository,
		logger: &logger,
		tracerProvider: tracerProvider,
		httpService: httpService,
		endpoint: endpoint,
	}
}

// Helper: Get service endpoint by index with error handling
func (s *WorkerService) getServiceEndpoint(index int) (*model.Endpoint, error) {
	if s.endpoint == nil || len(*s.endpoint) <= index {
		return nil, fmt.Errorf("service endpoint at index %d not found", index)
	}
	return &(*s.endpoint)[index], nil
}

// Helper: Build HTTP headers with request ID
func (s *WorkerService) buildHeaders(ctx context.Context) map[string]string {
	requestID := go_core_midleware.GetRequestID(ctx)
	return map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"X-Request-Id":  requestID,
	}
}

// Helper: Parse product from HTTP response payload
func (s *WorkerService) parseProductFromPayload(ctx context.Context, payload interface{}) (*model.Product, error) {
	jsonString, err := json.Marshal(payload)
	if err != nil {
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to marshal response payload: %w", err)
	}
	
	product := &model.Product{}
	if err := json.Unmarshal(jsonString, product); err != nil {		
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return nil, fmt.Errorf("FAILED to unmarshal product: %w", err)
	}
	return product, nil
}

// About database stats
func (s *WorkerService) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	s.logger.Info().
			Ctx(ctx).
			Str("func","Stat").Send()

	return s.workerRepository.Stat(ctx)
}

// About check health service
func (s * WorkerService) HealthCheck(ctx context.Context) error {
	s.logger.Info().
			Ctx(ctx).
			Str("func","HealthCheck").Send()

	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.HealthCheck", trace.SpanKindInternal)
	defer span.End()

	// Check database health
	ctx, spanDB := s.tracerProvider.SpanCtx(ctx, "DatabasePG.Ping", trace.SpanKindInternal)
	err := s.workerRepository.DatabasePG.Ping()
	spanDB.End()
	
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())		
		s.logger.Error().
			Ctx(ctx).
			Err(err).Msg("*** Database HEALTH CHECK FAILED ***")
		return erro.ErrHealthCheck
	}

	s.logger.Info().
		Ctx(ctx).
		Msg("*** Database HEALTH CHECK SUCCESSFULL ***")

	// ------------------------------------------------------------
	// check service/dependencies 
	endpoint, err := s.getServiceEndpoint(0)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(err).Send()
		return erro.ErrHealthCheck
	}

	headers := s.buildHeaders(ctx)

	ctxService00, spanService00 := s.tracerProvider.SpanCtx(ctx, "health.service." + endpoint.HostName, trace.SpanKindServer)
	httpClientParameter := go_core_http.HttpClientParameter {
		Url:	endpoint.Url + "/health",
		Method:	"GET",
		Timeout: endpoint.HttpTimeout,
		Headers: &headers,
	}

	// call a service via http
	_, err = s.doHttpCall(ctxService00, 
						   httpClientParameter)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())		
		s.logger.Error().
			Ctx(ctxService00).
			Err(err).Msgf("*** Service %s HEALTH CHECK FAILED ***", endpoint.HostName)
		return erro.ErrHealthCheck
	}
	spanService00.End()

	s.logger.Info().
			Ctx(ctx).
			Msgf("*** Service %s HEALTH CHECK SUCCESSFULL ***", endpoint.HostName)

	return nil
}

// About create a cart and cart itens
func (s *WorkerService) AddCart(ctx context.Context, 
								cart *model.Cart) (*model.Cart, error){
	s.logger.Info().
		Ctx(ctx).
		Str("func","AddCart").Send()

	// trace and log 
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.AddCart", trace.SpanKindServer)

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())		
		return nil, err
	}

	// handle connection
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
		s.workerRepository.DatabasePG.ReleaseTx(conn)
		span.End()
	}()

	// prepare data
	cart.CreatedAt = time.Now()
	if cart.Status == "" {	
		cart.Status = "CART:PENDING"
	}
	
	// Create cart
	res_cart, err := s.workerRepository.AddCart(ctx, tx, cart)
	if err != nil {
		return nil, err
	}
	cart.ID = res_cart.ID

	// Get service endpoint
	endpoint, err := s.getServiceEndpoint(0)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return nil, err
	}
	
	headers := s.buildHeaders(ctx)

	// Create cart itens
    for i := range *cart.CartItem { 
		cartItem := &(*cart.CartItem)[i]

		httpClientParameter := go_core_http.HttpClientParameter {
			Url:  endpoint.Url + "/product/" + cartItem.Product.Sku,
			Method: "GET",
			Timeout: endpoint.HttpTimeout,
			Headers: &headers,
		}
		
		// call a service via http
		resPayload, err := s.doHttpCall(ctx, 
										httpClientParameter)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())				
			return nil, err
		}

		product, err := s.parseProductFromPayload(ctx, resPayload)
		if err != nil {
			return nil, err
		}

		// prepare data
		cartItem.CreatedAt = cart.CreatedAt
		cartItem.Status = "CART_ITEM:PENDING"
		cartItem.Product = *product
		
		if cartItem.Quantity <= 0 || cartItem.Price <= 0 {
			err := fmt.Errorf("cart item quantity / price must be greater than zero")
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

    	res_cart_item, err := s.workerRepository.AddCartItem(ctx,
															 tx,
															 cart, 
															 cartItem)
		if err != nil {
			return nil, err
		}
		(*cart.CartItem)[i] = *res_cart_item
    }

	return cart, nil
}

// About get cart and cart itens
func (s * WorkerService) GetCart(ctx context.Context, 
								 cart *model.Cart) (*model.Cart, error){
	s.logger.Info().
		Ctx(ctx).
		Str("func","GetCart").Send()

	// trace
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.GetCart", trace.SpanKindInternal)
	defer span.End()

	// Call a service
	resCart, err := s.workerRepository.GetCart(ctx, cart)
	if err != nil {
		return nil, err
	}

	headers := s.buildHeaders(ctx)

	// Get product details for each cart item
	for i := range *resCart.CartItem {
		cartItem := &(*resCart.CartItem)[i]

		endpoint, err := s.getServiceEndpoint(0)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())	
			return nil, err
		}

		httpClientParameter := go_core_http.HttpClientParameter {
			Url:  fmt.Sprintf("%v%v%v", endpoint.Url, "/productId/", cartItem.Product.ID ),
			Method: "GET",
			Timeout: endpoint.HttpTimeout,
			Headers: &headers,
		}

		// call a service via http
		resPayload, err := s.doHttpCall(ctx, 
										httpClientParameter)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())	
			return nil, err
		}

		product, err := s.parseProductFromPayload(ctx, resPayload)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())	
			return nil, err
		}
		cartItem.Product = *product
	}

	return resCart, nil
}

// About update cart
func (s * WorkerService) UpdateCart(ctx context.Context, 
									cart *model.Cart) (*model.Cart, error){
	s.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCart").Send()

	// trace
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.UpdateCart", trace.SpanKindInternal)

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer s.workerRepository.DatabasePG.ReleaseTx(conn)

	// handle connection
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
		s.workerRepository.DatabasePG.ReleaseTx(conn)
		span.End()
	}()

	// check cart exists
	_, err = s.workerRepository.GetCart(ctx, cart)
	if err != nil {
		return nil, err
	}

	// business logic
	now := time.Now()
	cart.UpdatedAt = &now

	// Call a service
	row, err := s.workerRepository.UpdateCart(ctx, tx, cart)
	if err != nil {
		return nil, err
	}
	if row == 0 {
		span.RecordError(erro.ErrUpdate) 
        span.SetStatus(codes.Error, erro.ErrUpdate.Error())			
		s.logger.Error().
			Ctx(ctx).
			Err(erro.ErrUpdate).Send()
		return nil, erro.ErrUpdate
	}

	return cart, nil
}

// About update cart
func (s * WorkerService) UpdateCartItem(ctx context.Context, 
										cartItem *model.CartItem) (*model.CartItem, error){
	s.logger.Info().
		Ctx(ctx).
		Str("func","UpdateCartItem").Send()
			
	// trace
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.UpdateCartItem", trace.SpanKindInternal)

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer s.workerRepository.DatabasePG.ReleaseTx(conn)

	// handle connection
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
		s.workerRepository.DatabasePG.ReleaseTx(conn)
		span.End()
	}()

	// check cart item exists
	resCartItem, err := s.workerRepository.GetCartItem(ctx, cartItem)
	if err != nil {
		return nil, err
	}

	// business logic (synchronize timestamps)
	now := time.Now()
	cartItem.CreatedAt = resCartItem.CreatedAt
	cartItem.UpdatedAt = &now

	// Call a service
	row, err := s.workerRepository.UpdateCartItem(ctx, tx, cartItem)
	if err != nil {
		return nil, err
	}
	if row == 0 {
		span.RecordError(erro.ErrUpdate) 
        span.SetStatus(codes.Error, erro.ErrUpdate.Error())
		s.logger.Error().
			Ctx(ctx).
			Err(erro.ErrUpdate).Send()
		return nil, erro.ErrUpdate
	}

	return cartItem, nil
}