-- ====================================================================
-- SCHEMA: Raw Sales Data Table
-- ====================================================================
-- Purpose: Store raw sales transactions from CSV
-- Database: PostgreSQL 17
-- Project: Mini Pipeline Local - Analytics Engineer
-- ====================================================================

-- Drop table if exists (para re-ejecuciones limpias)
DROP TABLE IF EXISTS raw_sales;

-- Create raw_sales table
CREATE TABLE raw_sales (
    -- Primary Key (auto-incremental)
    id SERIAL PRIMARY KEY,
    -- Order Information
    order_number INTEGER NOT NULL,
    order_line_number INTEGER NOT NULL,
    order_date DATE NOT NULL,
    status VARCHAR(50),
    -- Product Information
    product_code VARCHAR(50) NOT NULL,
    product_line VARCHAR(100),
    -- Customer Information
    customer_name VARCHAR(255),
    country VARCHAR(100),
    -- Transaction Details
    quantity INTEGER CHECK (quantity > 0),
    unit_price DECIMAL(10,2) CHECK (unit_price >= 0),
    total_amount DECIMAL(10,2) CHECK (total_amount >= 0),
    -- Business Classification
    deal_size VARCHAR(20),
    -- Audit Fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Unique constraint to prevent duplicate order lines
    UNIQUE (order_number, order_line_number)
);

-- ====================================================================
-- INDEXES para mejorar queries analíticas
-- ====================================================================

-- Index en order_date (para queries de rango temporal)
CREATE INDEX idx_raw_sales_order_date ON raw_sales(order_date);

-- Index en product_line (para agregaciones por producto)
CREATE INDEX idx_raw_sales_product_line ON raw_sales(product_line);

-- Index en country (para análisis geográfico)
CREATE INDEX idx_raw_sales_country ON raw_sales(country);

-- Index en status (para filtrar por estado de orden)
CREATE INDEX idx_raw_sales_status ON raw_sales(status);

-- ====================================================================
-- VALIDACIÓN DEL SCHEMA
-- ====================================================================

-- Verificar que la tabla fue creada
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'raw_sales'
ORDER BY ordinal_position;

-- ====================================================================
-- NOTAS TÉCNICAS
-- ====================================================================
-- 1. SERIAL PRIMARY KEY: Auto-incremental, simplifica ingesta
-- 2. UNIQUE constraint: Previene duplicados de (order_number, order_line_number)
-- 3. CHECK constraints: Valida integridad de datos numéricos
-- 4. Indexes: Optimizan queries analíticas comunes
-- 5. created_at: Auditoría de cuándo se insertó el registro
-- ====================================================================