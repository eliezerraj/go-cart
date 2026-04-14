package database

import (
	"context"
	"fmt"

	"github.com/go-cart/internal/domain/model"
	"github.com/go-cart/shared/erro"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/codes"
)


// About a windowed data from given product. It´s used from stats inference how to know the velocity of inventory
func (w *WorkerRepository) ListCartItemWindow(  ctx context.Context,
												windowSize int,
												cartItem *model.CartItem) (*[]model.CartItem, error) {
	w.logger.Info().
			Ctx(ctx).
			Str("func","ListCartItemWindow").Send()

	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.ListCartItemWindow", trace.SpanKindInternal)
	defer span.End()
	
	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("FAILED to acquire database connection: %w", err)
	}
	defer w.DatabasePG.Release(conn)

	// Query and Execute
	query := `select * from (select 	ci.fk_product_id, 
									ci.quantity,
									ci.price, 
									ci.discount,
									ci.status,
									ci.created_at   
							from cart c,
								cart_item ci 
							where c.id = ci.fk_cart_id 
							and ci.fk_product_id = $1
							order by c.created_at desc
							limit $2 ) order by created_at asc`

	rows, err := conn.Query(ctx, 
							query, 
							cartItem.Product.ID,
							windowSize)
	if err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("FAILED to query cartitem: %w", err)
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		span.RecordError(err) 
        span.SetStatus(codes.Error, err.Error())
		w.logger.Error().
				Ctx(ctx).
				Err(err).Msg("error iterating cartitem rows")
        return nil, fmt.Errorf("error iterating cartitem rows: %w", err)
    }

	carItems := []model.CartItem{}
	resProduct := model.Product{}
	resCartItem := model.CartItem{}

	for rows.Next() {
		err := rows.Scan(	&resProduct.ID, 
							&resCartItem.Quantity,
							&resCartItem.Price,
							&resCartItem.Discount,
							&resCartItem.Status,
							&resCartItem.CreatedAt,
						)
		if err != nil {
			span.RecordError(err) 
			span.SetStatus(codes.Error, err.Error())
			w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
			return nil, fmt.Errorf("FAILED to scan row: %w", err)
        }

		resCartItem.Product = cartItem.Product
		carItems = append(carItems, resCartItem)
	}

	if len(carItems) == 0 {
		w.logger.Warn().
			Ctx(ctx).
			Err(erro.ErrNotFound).
			Interface("cartItem.Product",cartItem.Product).Send()

		return nil, erro.ErrNotFound
	}
		
	return &carItems, nil
}