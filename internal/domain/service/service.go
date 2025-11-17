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

	go_core_http "github.com/eliezerraj/go-core/http"
	go_core_db_pg "github.com/eliezerraj/go-core/database/postgre"
	go_core_otel_trace "github.com/eliezerraj/go-core/otel/trace"
)

var tracerProvider go_core_otel_trace.TracerProvider

type WorkerService struct {
	appServer			*model.AppServer
	workerRepository	*database.WorkerRepository
	logger 				*zerolog.Logger
	httpService			*go_core_http.HttpService		 	
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
			Str("func","Stat").Send()

	return s.workerRepository.Stat(ctx)
}

// About check health service
func (s * WorkerService) HealthCheck(ctx context.Context) error {
	s.logger.Info().
			Str("func","HealthCheck").Send()

	// Check database health
	err := s.workerRepository.DatabasePG.Ping()
	if err != nil {
		s.logger.Error().
				Err(err).Msg("*** Database HEALTH FAILED ***")
		return erro.ErrHealthCheck
	}

	s.logger.Info().
			Str("func","HealthCheck").
			Msg("*** Database HEALTH SUCCESSFULL ***")

	return nil
}

// About create a cart and cart itens
func (s *WorkerService) AddCart(ctx context.Context, 
								cart *model.Cart) (*model.Cart, error){
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "service.AddCart")
	defer span.End()

	s.logger.Info().
			Ctx(ctx).
			Str("func","AddCart").Send()

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
		span.End()
	}()

	// prepare data
	cart.CreatedAt = time.Now()

	// Create cart
	res_cart, err := s.workerRepository.AddCart(ctx, tx, cart)
	if err != nil {
		return nil, err
	}
	cart.ID = res_cart.ID

	// Prepare the http headers
	trace_id := fmt.Sprintf("%v",ctx.Value("trace-request-id"))

	headers := map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"X-Request-Id": trace_id,
		//"Host": s.apiService[0].HostName,
	}

	// Call service inventory for product
	var httpClientParameter go_core_http.HttpClientParameter

	// Create cart itens
    for i := range *cart.CartItem { 
		cartItem := &(*cart.CartItem)[i]

		// prepare data
		cartItem.CreatedAt = cart.CreatedAt
		cartItem.Status = "BASKET"

		httpClientParameter = go_core_http.HttpClientParameter {
			Url:  (*s.appServer.Endpoint)[0].Url + "/product/" + cartItem.Product.Sku,
			Method: (*s.appServer.Endpoint)[0].Method,
			Timeout: (*s.appServer.Endpoint)[0].HttpTimeout,
			Headers: &headers,
		}
		
		res_payload, statusCode, err := s.httpService.DoHttp(ctx, 
															httpClientParameter)

		if err != nil {
			s.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, err
		}
		if statusCode != http.StatusOK {
			if statusCode == http.StatusNotFound {
				return nil, erro.ErrNotFound
			} else {
				return nil, erro.ErrBadRequest 
			}
		}

		jsonString, err  := json.Marshal(res_payload)
		if err != nil {
			s.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, errors.New(err.Error())
		}
		product := model.Product{}
		json.Unmarshal(jsonString, &product)

		cartItem.Product = product

    	res_cart_item, err := s.workerRepository.AddCartItem(ctx,
															 tx,
															 cart, 
															 cartItem)
		if err != nil {
			s.logger.Error().
					Ctx(ctx).
					Err(err).Send()
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
								
	return resCart, nil
}