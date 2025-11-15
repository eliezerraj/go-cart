package database

import (
		"context"
		"errors"
		//"database/sql"

		"github.com/rs/zerolog"
		"github.com/jackc/pgx/v5"

		//"github.com/go-cart/shared/erro"
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

	cartItem.Product.ID = 1

	// Query Execute
	query := `INSERT INTO cart_item ( 	fk_cart_id,
										fk_product_id,
										quantity,
										price, 
										created_at)
				VALUES($1, $2, $3, $4, $5) RETURNING id`

	row := tx.QueryRow(	ctx, 
						query,
						cartItem.Product.ID,
						cartItem.Product.ID,
						cartItem.Price,
						cartItem.Quantity,					
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
