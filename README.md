# go-cart

    workload for POC purpose

# tables

    CREATE TABLE public.cart (
        id 			BIGSERIAL 	NOT NULL,
        user_id 	VARCHAR(100) NOT NULL,
        status		VARCHAR(100) NOT NULL,
        created_at	timestamptz NOT NULL,
        updated_at	timestamptz NULL,  
        CONSTRAINT cart_pkey PRIMARY KEY (id)
    );

    CREATE TABLE public.cart_item (
        id 				BIGSERIAL	NOT NULL,
        fk_cart_id		BIGSERIAL	NOT NULL,
        fk_product_id	BIGSERIAL	NOT NULL,
        status			VARCHAR(100)	NULL,
        currency		VARCHAR(100)	NULL,
        quantity 		INT 		NOT null DEFAULT 0,
        discount 		DECIMAL(10,2) NOT null DEFAULT 0,
        price 			DECIMAL(10,2) NOT null DEFAULT 0,
        created_at		timestamptz 	NOT NULL,
        updated_at		timestamptz 	NULL,  
        CONSTRAINT cart_item_pkey PRIMARY KEY (id)
    );

    ALTER TABLE public.cart_item ADD constraint cart_item_fk_cart_id_fkey 
    FOREIGN KEY (fk_cart_id) REFERENCES public.cart(id);

    ALTER TABLE public.cart_item ADD CONSTRAINT cart_item_fk_product_id_fkey 
    FOREIGN KEY (fk_product_id) REFERENCES public.product(id);

