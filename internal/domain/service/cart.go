package service

import (
	"fmt"
	"time"
	"context"
	"encoding/json"

	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"

	go_core_http "github.com/eliezerraj/go-core/v2/http"
)

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