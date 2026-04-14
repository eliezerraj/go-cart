package service

import (
	"context"
	"github.com/go-cart/internal/domain/model"

	go_core_http "github.com/eliezerraj/go-core/v2/http"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"
)

func (s * WorkerService) ListCartItemWindow(ctx context.Context,
											windowSize int,
								   			cartItem *model.CartItem) (*[]model.CartItem, error){

	s.logger.Info().
		Ctx(ctx).
		Str("func","ListCartItemWindow").Send()

	// trace and log
	ctx, span := s.tracerProvider.SpanCtx(ctx, "service.ListCartItemWindow", trace.SpanKindServer)
	defer span.End()
					
	headers := s.buildHeaders(ctx)
	
	// Get service endpoint
	endpoint, err := s.getServiceEndpoint(0)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())	
		return nil, err
	}

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

	cartItem.Product = *product

	cartItems, err := s.workerRepository.ListCartItemWindow(ctx, windowSize, cartItem)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	
	return cartItems, nil
}