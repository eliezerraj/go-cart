package database

import (
		"context"
		"errors"
		"database/sql"

		"github.com/rs/zerolog"
		"github.com/jackc/pgx/v5"

		"github.com/go-cart/shared/erro"
		"github.com/go-cart/internal/domain/model"

		go_core_otel_trace "github.com/eliezerraj/go-core/otel/trace"
		go_core_db_pg "github.com/eliezerraj/go-core/database/postgre"
)

var tracerProvider go_core_otel_trace.TracerProvider

type WorkerRepository struct {
	DatabasePG *go_core_db_pg.DatabasePGServer
	logger		*zerolog.Logger
}

// Above new worker
func NewWorkerRepository(databasePG *go_core_db_pg.DatabasePGServer,
						appLogger *zerolog.Logger) *WorkerRepository{
	logger := appLogger.With().
						Str("package", "repo.database").
						Logger()
	logger.Info().
			Str("func","NewWorkerRepository").Send()

	return &WorkerRepository{
		DatabasePG: databasePG,
		logger: &logger,
	}
}

// Above get stats from database
func (w *WorkerRepository) Stat(ctx context.Context) (go_core_db_pg.PoolStats){
	w.logger.Info().
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

// About create a card
func (w* WorkerRepository) AddCart(ctx context.Context, 
									tx pgx.Tx, 
									cart *model.Cart) (*model.Cart, error){
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "database.AddCart")
	defer span.End()

	w.logger.Info().
			Ctx(ctx).
			Str("func","AddCart").Send()

	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer w.DatabasePG.Release(conn)

	//Prepare
	var id int

	// Query Execute
	query := `INSERT INTO cart ( user_id, 
								created_at) 
				VALUES($1, $2) RETURNING id`

	row := tx.QueryRow(	ctx, 
						query,
						cart.UserId,
						cart.CreatedAt)
						
	if err := row.Scan(&id); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
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
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "database.AddCartItem")
	defer span.End()

	w.logger.Info().
			Ctx(ctx).
			Str("func","AddCartItem").Send()

	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer w.DatabasePG.Release(conn)

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
		return nil, errors.New(err.Error())
	}

	// Set PK
	cartItem.ID = id
	
	return cartItem , nil
}

// About get a cart_item
func (w *WorkerRepository) GetCart(ctx context.Context,
									cart *model.Cart) (*model.Cart, error) {
	// trace
	ctx, span := tracerProvider.SpanCtx(ctx, "database.GetCart")
	defer span.End()

	w.logger.Info().
			Ctx(ctx).
			Str("func","GetCart").Send()

	// db connection
	conn, err := w.DatabasePG.Acquire(ctx)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer w.DatabasePG.Release(conn)

	// Query and Execute
	query := `select ca.id,
					ca.user_id,
					ca.created_at,
					ca.updated_at,
					p.id,
					p.sku,
					p.type,
					p.name,
					p.status,
					p.created_at,
					p.updated_at,
					ca_it.id,
					ca_it.status,
					ca_it.quantity,
					ca_it.currency,
					ca_it.price,
					ca_it.discount,
					ca_it.created_at,
					ca_it.updated_at
				from cart ca,
					cart_item ca_it,
					product p
				where ca.id = ca_it.fk_cart_id
				and p.id = ca_it.fk_product_id
				and ca.id = $1`

	rows, err := conn.Query(ctx, 
							query, 
							cart.ID)
	if err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Send()
		return nil, errors.New(err.Error())
	}
	defer rows.Close()
	
    if err := rows.Err(); err != nil {
		w.logger.Error().
				Ctx(ctx).
				Err(err).Msg("fatal error closing rows")
        return nil, errors.New(err.Error())
    }

	resCart := model.Cart{}
	resProduct := model.Product{}
	resCartItem := model.CartItem{}
	listCartItem := []model.CartItem{}

	var nullCartUpdatedAt sql.NullTime
	var nullProductUpdatedAt sql.NullTime
	var nullCartItemUpdatedAt sql.NullTime

	for rows.Next() {
		err := rows.Scan(	&resCart.ID, 
							&resCart.UserId,
							&resCart.CreatedAt,
							&nullCartUpdatedAt,

							&resProduct.ID, 
							&resProduct.Sku, 
							&resProduct.Type,
							&resProduct.Name,
							&resProduct.Status,
							&resProduct.CreatedAt,
							&nullProductUpdatedAt,

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
			return nil, errors.New(err.Error())
        }

		if nullCartUpdatedAt.Valid {
        	resCart.UpdatedAt = &nullProductUpdatedAt.Time
    	} else {
			resCart.UpdatedAt = nil
		}
		if nullProductUpdatedAt.Valid {
        	resProduct.UpdatedAt = &nullProductUpdatedAt.Time
    	} else {
			resProduct.UpdatedAt = nil
		}
		if nullCartItemUpdatedAt.Valid {
        	resCartItem.UpdatedAt = &nullCartItemUpdatedAt.Time
    	} else {
			resCartItem.UpdatedAt = nil
		}

		resCartItem.Product = resProduct
		listCartItem = append(listCartItem, resCartItem)

		resCart.CartItem = &listCartItem
	}

	if resCart == (model.Cart{}) {
		w.logger.Warn().
				Ctx(ctx).
				Err(err).Send()
		return nil, erro.ErrNotFound
	}
		
	return &resCart, nil
}