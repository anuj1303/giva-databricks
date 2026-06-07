-- BrickJewels Lakebase schema (exported from live)

CREATE TABLE IF NOT EXISTS brickjewels_users (
    user_id INTEGER NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    country_code TEXT DEFAULT '+91'::text,
    mobile TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    date_of_birth DATE,
    anniversary_date DATE,
    milestone_label TEXT,
    milestone_date DATE
);

CREATE TABLE IF NOT EXISTS brickjewels_orders (
    order_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_email TEXT,
    items JSONB NOT NULL,
    total_amount BIGINT NOT NULL,
    status TEXT DEFAULT 'confirmed'::text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    user_id INTEGER
);

CREATE TABLE IF NOT EXISTS brickjewels_user_data_scd2 (
    data_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    cart JSONB DEFAULT '[]'::jsonb,
    wishlist JSONB DEFAULT '[]'::jsonb,
    is_active BOOLEAN DEFAULT true,
    effective_from TIMESTAMP WITH TIME ZONE DEFAULT now(),
    effective_to TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS brickjewels_metal_prices (
    id INTEGER NOT NULL,
    metal TEXT NOT NULL,
    currency TEXT DEFAULT 'INR'::text NOT NULL,
    price_gram_24k NUMERIC,
    price_gram_22k NUMERIC,
    price_gram_21k NUMERIC,
    price_gram_20k NUMERIC,
    price_gram_18k NUMERIC,
    price_gram_16k NUMERIC,
    price_gram_14k NUMERIC,
    price_gram_10k NUMERIC,
    price_per_gram NUMERIC,
    price_per_oz NUMERIC,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    effective_from TIMESTAMP WITH TIME ZONE DEFAULT now(),
    effective_to TIMESTAMP WITH TIME ZONE,
    pct_change_24k NUMERIC DEFAULT 0,
    pct_change_22k NUMERIC DEFAULT 0,
    pct_change_21k NUMERIC DEFAULT 0,
    pct_change_20k NUMERIC DEFAULT 0,
    pct_change_18k NUMERIC DEFAULT 0,
    pct_change_16k NUMERIC DEFAULT 0,
    pct_change_14k NUMERIC DEFAULT 0,
    pct_change_10k NUMERIC DEFAULT 0
);

CREATE TABLE IF NOT EXISTS brickjewels_product_prices (
    id INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    karat TEXT,
    metal_type TEXT,
    weight_grams NUMERIC,
    category TEXT,
    metal_rate_per_gram NUMERIC DEFAULT 0,
    metal_cost NUMERIC DEFAULT 0,
    diamond_cost NUMERIC DEFAULT 0,
    making_pct NUMERIC DEFAULT 0,
    making_cost NUMERIC DEFAULT 0,
    discount_pct NUMERIC DEFAULT 0,
    discount_value NUMERIC DEFAULT 0,
    gst_pct_metal_diamond NUMERIC DEFAULT 3,
    gst_cost_metal_diamond NUMERIC DEFAULT 0,
    gst_pct_making NUMERIC DEFAULT 28,
    gst_cost_making NUMERIC DEFAULT 0,
    total_before_gst NUMERIC DEFAULT 0,
    total_gst NUMERIC DEFAULT 0,
    final_price NUMERIC DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    effective_from TIMESTAMP WITH TIME ZONE NOT NULL,
    effective_to TIMESTAMP WITH TIME ZONE,
    computed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS brickjewels_nudges (
    nudge_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    nudge_type TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    discount_type TEXT,
    discount_value NUMERIC DEFAULT 0,
    discount_code TEXT,
    min_cart_value NUMERIC DEFAULT 0,
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    is_dismissed BOOLEAN DEFAULT false,
    is_redeemed BOOLEAN DEFAULT false,
    redeemed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    target_category TEXT
);

CREATE TABLE IF NOT EXISTS brickjewels_nudge_emails (
    email_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    nudge_id INTEGER,
    recipient_email TEXT NOT NULL,
    sender_email TEXT DEFAULT 'noreply@example.com'::text NOT NULL,
    sender_name TEXT DEFAULT 'BrickJewels Offers'::text NOT NULL,
    subject TEXT NOT NULL,
    body_html TEXT,
    status TEXT DEFAULT 'pending'::text,
    error_message TEXT,
    sent_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS brickjewels_chat_sessions (
    session_id INTEGER NOT NULL,
    user_id INTEGER,
    title TEXT DEFAULT 'New Conversation'::text,
    messages JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS brickjewels_genie_history (
    id INTEGER NOT NULL,
    conversation_id TEXT NOT NULL,
    title TEXT DEFAULT 'New Chat'::text NOT NULL,
    messages JSONB DEFAULT '[]'::jsonb,
    admin_email TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
