package service

import (
	"fmt"
	"time"
	"errors"
	"context"
	"net/http"
	"encoding/json"

	"github.com/rs/zerolog"

	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"

	database "github.com/go-cart/internal/infrastructure/repo/database"

	go_core_http "github.com/eliezerraj/go-core/v2/http"
	go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
)

var tracerProvider go_core_otel_trace.TracerProvider

type WorkerService struct {
	appServer			*model.AppServer
	workerRepository	*database.WorkerRepository
	logger 				*zerolog.Logger
	httpService			*go_core_http.HttpService		 	
}

// about do http call 
func (s *WorkerService) doHttpCall(ctx context.Context,
									httpClientParameter go_core_http.HttpClientParameter) (interface{},error) {
		
	resPayload, statusCode, err := s.httpService.DoHttp(ctx, 
														httpClientParameter)
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, err
	}

	if statusCode != http.StatusOK {
		if statusCode == http.StatusNotFound {
			s.logger.Warn().
					 Ctx(ctx).
					 Err(erro.ErrNotFound).Send()
			return nil, erro.ErrNotFound
		} else {		
			jsonString, err  := json.Marshal(resPayload)
			if err != nil {
				s.logger.Error().
						Ctx(ctx).
						Err(err).Send()
				return nil, errors.New(err.Error())
			}			
			
			message := model.APIError{}
			json.Unmarshal(jsonString, &message)

			newErr := errors.New(fmt.Sprintf("http call error: status code %d - message: %s", message.StatusCode ,message.Msg))
			s.logger.Error().
					Ctx(ctx).
					Err(newErr).Send()
			return nil, newErr
		}
	}

	return resPayload, nil
}

// About new worker service
func NewWorkerService(appServer	*model.AppServer,
					  workerRepository *database.WorkerRepository, 
					  appLogger *zerolog.Logger) *WorkerService {
	logger := appLogger.With().
						Str("package", "domain.service").
						Logger()
	logger.Info().
			Str("func","NewWorkerService").Send()

	httpService := go_core_http.NewHttpService(&logger)					

	return &WorkerService{
		appServer: appServer,
		workerRepository: workerRepository,
		logger: &logger,
		httpService: httpService,
	}
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

	ctx, span := tracerProvider.SpanCtx(ctx, "service.HealthCheck")
	defer span.End()

	// Check database health
	_, spanDB := tracerProvider.SpanCtx(ctx, "DatabasePG.Ping")
	err := s.workerRepository.DatabasePG.Ping()
	if err != nil {
		s.logger.Error().
				Ctx(ctx).
				Err(err).Msg("*** Database HEALTH CHECK FAILED ***")
		return erro.ErrHealthCheck
	}
	spanDB.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","HealthCheck").
			Msg("*** Database HEALTH CHECK SUCCESSFULL ***")

	// ------------------------------------------------------------
	// check service/dependencies 
	headers := map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
	}

	ctxService00, spanService00 := tracerProvider.SpanCtx(ctx, "health.service." + (*s.appServer.Endpoint)[0].HostName )
	httpClientParameter := go_core_http.HttpClientParameter {
		Url:	(*s.appServer.Endpoint)[0].Url + "/health",
		Method:	"GET",
		Timeout: (*s.appServer.Endpoint)[0].HttpTimeout,
		Headers: &headers,
	}

	// call a service via http
	_, err = s.doHttpCall(ctxService00, 
						   httpClientParameter)
	if err != nil {
		s.logger.Error().
				Ctx(ctxService00).
				Err(err).Msgf("*** Service %s HEALTH CHECK FAILED ***", (*s.appServer.Endpoint)[0].HostName )
		return erro.ErrHealthCheck
	}
	spanService00.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","HealthCheck").
			Msgf("*** Service %s HEALTH CHECK SUCCESSFULL ***", (*s.appServer.Endpoint)[0].HostName )

	return nil
}

// About create a cart and cart itens
func (s *WorkerService) AddCart(ctx context.Context, 
								cart *model.Cart) (*model.Cart, error){
	// trace and log 
	ctx, span := tracerProvider.SpanCtx(ctx, "service.AddCart")

	s.logger.Info().
			Ctx(ctx).
			Str("func","AddCart").Send()

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
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
		cart.Status = "CART:POSTED-DEFAULT"
	}
	
	// Create cart
	res_cart, err := s.workerRepository.AddCart(ctx, tx, cart)
	if err != nil {
		return nil, err
	}
	cart.ID = res_cart.ID

	// prepare headers http for calling services
	trace_id := fmt.Sprintf("%v",ctx.Value("trace-request-id"))

	headers := map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"X-Request-Id": trace_id,
	}

	// Create cart itens
    for i := range *cart.CartItem { 
		cartItem := &(*cart.CartItem)[i]

		httpClientParameter := go_core_http.HttpClientParameter {
			Url:  (*s.appServer.Endpoint)[0].Url + "/product/" + cartItem.Product.Sku,
			Method: "GET",
			Timeout: (*s.appServer.Endpoint)[0].HttpTimeout,
			Headers: &headers,
		}
		
		// call a service via http
		resPayload, err := s.doHttpCall(ctx, 
										httpClientParameter)
		if err != nil {
			return nil, err
		}

		jsonString, err  := json.Marshal(resPayload)
		if err != nil {
			s.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, errors.New(err.Error())
		}
		product := model.Product{}
		json.Unmarshal(jsonString, &product)

		// prepare data
		cartItem.CreatedAt = cart.CreatedAt
		cartItem.Status = "CART_ITEM:POSTED"
		cartItem.Product = product

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
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "service.GetCart")
	defer span.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","GetCart").Send()

	// Call a service
	resCart, err := s.workerRepository.GetCart(ctx, cart)
	if err != nil {
		return nil, err
	}

	// prepare headers http for calling services
	trace_id := fmt.Sprintf("%v",ctx.Value("trace-request-id"))
	headers := map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"X-Request-Id": trace_id,
	}

	// Get product details for each cart item
	for i := range *resCart.CartItem {
		cartItem := &(*resCart.CartItem)[i]

		httpClientParameter := go_core_http.HttpClientParameter {
			Url:  fmt.Sprintf("%v%v%v", (*s.appServer.Endpoint)[0].Url, "/productId/", cartItem.Product.ID ),
			Method: "GET",
			Timeout: (*s.appServer.Endpoint)[0].HttpTimeout,
			Headers: &headers,
		}

		// call a service via http
		resPayload, err := s.doHttpCall(ctx, 
										httpClientParameter)
		if err != nil {
			return nil, err
		}

		jsonString, err  := json.Marshal(resPayload)
		if err != nil {
			s.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, errors.New(err.Error())
		}
		product := model.Product{}
		json.Unmarshal(jsonString, &product)
		cartItem.Product = product
	}

	return resCart, nil
}

// About update cart
func (s * WorkerService) UpdateCart(ctx context.Context, 
									cart *model.Cart) (*model.Cart, error){
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "service.UpdateCart")

	s.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCart").Send()

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
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
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "service.UpdateCartItem")

	s.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCartItem").Send()

	// prepare database
	tx, conn, err := s.workerRepository.DatabasePG.StartTx(ctx)
	if err != nil {
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
		s.logger.Error().
				 Ctx(ctx).
				 Err(erro.ErrUpdate).Send()
		return nil, erro.ErrUpdate
	}

	return cartItem, nil
}