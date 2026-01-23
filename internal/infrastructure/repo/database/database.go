package database

import (
	"fmt"
	"time"
	"context"
	"database/sql"

	"github.com/rs/zerolog"
	"github.com/jackc/pgx/v5"

	"github.com/go-cart/shared/erro"
	"github.com/go-cart/internal/domain/model"
	"go.opentelemetry.io/otel/trace"

	go_core_otel_trace "github.com/eliezerraj/go-core/v2/otel/trace"
	go_core_db_pg "github.com/eliezerraj/go-core/v2/database/postgre"
)

type WorkerRepository struct {
	DatabasePG 		*go_core_db_pg.DatabasePGServer
	logger			*zerolog.Logger
	tracerProvider 	*go_core_otel_trace.TracerProvider
}

// Above new worker
func NewWorkerRepository(databasePG *go_core_db_pg.DatabasePGServer,
						appLogger *zerolog.Logger,
						tracerProvider *go_core_otel_trace.TracerProvider) *WorkerRepository{
	logger := appLogger.With().
						Str("package", "repo.database").
						Logger()
	logger.Info().
			Str("func","NewWorkerRepository").Send()

	return &WorkerRepository{
		DatabasePG: databasePG,
		logger: &logger,
		tracerProvider: tracerProvider,
	}
}

// Helper function to convert nullable time to pointer
func (w *WorkerRepository) pointerTime(nt sql.NullTime) *time.Time {
	if nt.Valid {
		return &nt.Time
	}
	return nil
}

// Helper function to scan cart from rows iterator
func (w *WorkerRepository) scanCartFromRow(rows pgx.Rows) (*model.Cart, error) {
	cart := model.Cart{}
	var nullUpdatedAt sql.NullTime
	
	err := rows.Scan(&cart.ID, 
					&cart.UserId,
					&cart.Status,
					&cart.CreatedAt,
					&nullUpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to scan cart from rows: %w", err)
	}
	
	cart.UpdatedAt = w.pointerTime(nullUpdatedAt)
	return &cart, nil
}

// Helper function to scan cart item from rows iterator
func (w *WorkerRepository) scanCartItemFromRow(rows pgx.Rows) (*model.CartItem, error) {
	cartItem := model.CartItem{}
	var nullUpdatedAt sql.NullTime
	
	err := rows.Scan(&cartItem.ID,
					&cartItem.Status, 
					&cartItem.Quantity, 
					&cartItem.Currency, 
					&cartItem.Price,
					&cartItem.Discount, 
					&cartItem.CreatedAt,
					&nullUpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to scan cart item from rows: %w", err)
	}
	
	cartItem.UpdatedAt = w.pointerTime(nullUpdatedAt)
	return &cartItem, nil
}

// Above get stats from database
func (w *WorkerRepository) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	w.logger.Info().
			Ctx(ctx).
			Str("func","Stat").Send()
	
	stats := w.DatabasePG.Stat()

	resPoolStats := go_core_db_pg.PoolStats{
		AcquireCount:         stats.AcquireCount(),
		AcquiredConns:        stats.AcquiredConns(),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
		ConstructingConns:    stats.ConstructingConns(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		IdleConns:            stats.IdleConns(),
		MaxConns:             stats.MaxConns(),
		TotalConns:           stats.TotalConns(),
	}

	return resPoolStats
}

// About create a cart
func (w* WorkerRepository) AddCart(ctx context.Context, 
									tx pgx.Tx, 
									cart *model.Cart) (*model.Cart, error){
	w.logger.Info().
			Ctx(ctx).
			Str("func","AddCart").Send()
	
	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.AddCart", trace.SpanKindInternal)
	defer span.End()

	//Prepare
	var id int

	// Query Execute
	query := `INSERT INTO cart ( user_id,
								 status,	
								 created_at) 
				VALUES($1, $2, $3) RETURNING id`

	row := tx.QueryRow(	ctx, 
						query,
						cart.UserId,
						cart.Status,
						cart.CreatedAt)
						
	if err := row.Scan(&id); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("failed to scan cart ID: %w", err)
	}

	// Set PK
	cart.ID = id
	
	return cart , nil
}

// About create a cart_item
func (w* WorkerRepository) AddCartItem(ctx context.Context, 
										tx pgx.Tx, 
										cart *model.Cart,
										cartItem *model.CartItem) (*model.CartItem, error){
	w.logger.Info().
			Ctx(ctx).
			Str("func","AddCartItem").Send()

	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.AddCartItem", trace.SpanKindInternal)
	defer span.End()

	//Prepare
	var id int

	// Query Execute
	query := `INSERT INTO cart_item ( 	fk_cart_id,
										fk_product_id,
										status,
										currency,
										quantity,
										price, 
										created_at)
				VALUES($1, $2, $3, $4, $5, $6, $7) RETURNING id`

	row := tx.QueryRow(	ctx, 
						query,
						cart.ID,
						cartItem.Product.ID,
						cartItem.Status,
						cartItem.Currency,
						cartItem.Quantity,	
						cartItem.Price,
						cartItem.CreatedAt)
						
	if err := row.Scan(&id); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("failed to scan cart item ID: %w", err)
	}

	// Set PK
	cartItem.ID = id
	
	return cartItem , nil
}

// About get a cart_item
func (w *WorkerRepository) GetCart(ctx context.Context,
								   cart *model.Cart) (*model.Cart, error) {
	w.logger.Info().
			Ctx(ctx).
			Str("func","GetCart").Send()

	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.GetCart", trace.SpanKindInternal)
	defer span.End()
	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("failed to acquire database connection: %w", err)
	}
	defer w.DatabasePG.Release(conn)

	// Query and Execute
	query := `select ca.id,
					ca.user_id,
					ca.status,
					ca.created_at,
					ca.updated_at,
					ca_it.fk_product_id,
					ca_it.id,
					ca_it.status,
					ca_it.quantity,
					ca_it.currency,
					ca_it.price,
					ca_it.discount,
					ca_it.created_at,
					ca_it.updated_at
				from cart ca,
					cart_item ca_it
				where ca.id = ca_it.fk_cart_id
				and ca.id = $1`

	rows, err := conn.Query(ctx, 
							query, 
							cart.ID)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("failed to query cart: %w", err)
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Msg("error iterating cart rows")
        return nil, fmt.Errorf("error iterating cart rows: %w", err)
    }

	resCart := model.Cart{}
	resProduct := model.Product{}
	resCartItem := model.CartItem{}
	listCartItem := []model.CartItem{}

	var nullCartUpdatedAt sql.NullTime
	var nullCartItemUpdatedAt sql.NullTime

	for rows.Next() {
		err := rows.Scan(	&resCart.ID, 
							&resCart.UserId,
							&resCart.Status,
							&resCart.CreatedAt,
							&nullCartUpdatedAt,
							&resProduct.ID, 
							&resCartItem.ID,
							&resCartItem.Status, 
							&resCartItem.Quantity, 
							&resCartItem.Currency, 							
							&resCartItem.Price,
							&resCartItem.Discount, 
							&resCartItem.CreatedAt,
							&nullCartItemUpdatedAt,
						)
		if err != nil {
			w.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, fmt.Errorf("failed to scan cart row: %w", err)
        }

		resCart.UpdatedAt = w.pointerTime(nullCartUpdatedAt)
		resCartItem.UpdatedAt = w.pointerTime(nullCartItemUpdatedAt)
		resCartItem.Product = resProduct
		listCartItem = append(listCartItem, resCartItem)

		resCart.CartItem = &listCartItem
	}

	if resCart == (model.Cart{}) {
		w.logger.Warn().
				Ctx(ctx).
				Msg("cart not found")
		return nil, erro.ErrNotFound
	}
		
	return &resCart, nil
}

// About update cart 
func (w *WorkerRepository) UpdateCart(ctx context.Context, 
									  tx pgx.Tx, 
									  cart *model.Cart) (int64, error){
	w.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCart").Send()

	// trace and log
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.UpdateCart", trace.SpanKindInternal)
	defer span.End()

	// Query Execute
	query := `UPDATE cart
				SET status = $3,
					updated_at = $2
				WHERE id = $1`

	row, err := tx.Exec(ctx, 
						query,	
						cart.ID,
						cart.UpdatedAt,
						cart.Status,		
					)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Str("func","UpdateCart").
				Err(err).Send()
		return 0, fmt.Errorf("failed to update cart: %w", err)
	}

	return row.RowsAffected(), nil
}

// About get a cart_item
func (w *WorkerRepository) GetCartItem(ctx context.Context,
									   cartItem *model.CartItem) (*model.CartItem, error) {

	w.logger.Info().
			Ctx(ctx).
			Str("func","GetCartItem").Send()

	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.GetCartItem", trace.SpanKindInternal)
	defer span.End()

	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("failed to acquire database connection: %w", err)
	}
	defer w.DatabasePG.Release(conn)

	// Query and Execute
	query := `select ca_it.id,
					 ca_it.status,
					 ca_it.quantity,
					 ca_it.currency,
					 ca_it.price,
					 ca_it.discount,
					 ca_it.created_at,
					 ca_it.updated_at
				from cart_item ca_it
				where ca_it.id = $1`

	rows, err := conn.Query(ctx, 
							query, 
							cartItem.ID)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, fmt.Errorf("failed to query cart item: %w", err)
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Msg("error iterating cart item rows")
        return nil, fmt.Errorf("error iterating cart item rows: %w", err)
    }

	var resCartItem *model.CartItem
	for rows.Next() {
		item, err := w.scanCartItemFromRow(rows)
		if err != nil {
			w.logger.Error().
					Ctx(ctx).
					Err(err).Send()
			return nil, err
        }
		resCartItem = item
	}

	if resCartItem == nil {
		w.logger.Warn().
				Ctx(ctx).
				Msg("cart item not found")
		return nil, erro.ErrNotFound
	}
		
	return resCartItem, nil
}

// About update cart-item 
func (w *WorkerRepository) UpdateCartItem(ctx context.Context, 
									  	  tx pgx.Tx, 
									  	  cartItem *model.CartItem) (int64, error){
	w.logger.Info().
			Ctx(ctx).
			Str("func","UpdateCartItem").Send()
	
	// trace
	ctx, span := w.tracerProvider.SpanCtx(ctx, "database.UpdateCartItem", trace.SpanKindInternal)
	defer span.End()

	// Query Execute
	query := `UPDATE cart_item
				SET status = $3,
					updated_at = $2
				WHERE id = $1`

	row, err := tx.Exec(ctx, 
						query,	
						cartItem.ID,
						cartItem.UpdatedAt,
						cartItem.Status,		
					)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Str("func","UpdateCartItem").
				Err(err).Send()
		return 0, fmt.Errorf("failed to update cart item: %w", err)
	}

	return row.RowsAffected(), nil
}