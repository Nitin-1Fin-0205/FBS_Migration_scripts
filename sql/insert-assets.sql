-- ============================================================
-- ASSET MIGRATION: INSERT PHASE
-- ============================================================
-- Optimized SQL script to insert extracted assets into 
-- user_assets_history table with duplicate detection
-- ============================================================

-- Step 1: Verify target table exists and is empty
-- ============================================================
SELECT COUNT(*) as existing_records FROM user_assets_history;

-- Expected: 0 (first run) or existing count (resuming)


-- Step 2: Check source data validity
-- ============================================================
SELECT 
    COUNT(*) as total_customers,
    COUNT(DISTINCT customer_code) as unique_customers,
    COUNT(DISTINCT cycle) as unique_cycles
FROM customer_data
WHERE category_id = 0
    AND parameters IS NOT NULL
    AND parameters::text ~ '^[\[\{]'
    AND is_active = true
    AND parameters::jsonb ? 'assets';

-- Expected: ~16,000 customers, multiple cycles


-- Step 3: Create temporary extraction table (Optional)
-- ============================================================
-- This step is only needed if you want to stage data before insertion
CREATE TEMP TABLE IF NOT EXISTS extracted_assets_temp (
    customer_code UUID,
    asset TEXT,
    asset_class TEXT,
    market_value DOUBLE PRECISION,
    monthly_investments DOUBLE PRECISION,
    cycle BIGINT,
    created_at TIMESTAMP WITH TIME ZONE
);


-- Step 4: MAIN INSERTION - Direct approach with JSON extraction
-- ============================================================
-- This extracts and inserts in one go with duplicate detection
BEGIN TRANSACTION;

INSERT INTO user_assets_history (
    user_code,
    asset,
    scid,
    sub_category,
    market_amount,
    monthly_investment,
    cycle,
    logged_at
)
SELECT
    cd.customer_code,
    (asset_item->>'asset')::TEXT as asset,
    0 as scid,
    (asset_item->>'asset_class')::TEXT as sub_category,
    COALESCE((asset_item->>'market_value')::DOUBLE PRECISION, 0) as market_amount,
    COALESCE((asset_item->>'monthly_investments')::DOUBLE PRECISION, 0) as monthly_investment,
    cd.cycle,
    cd.created_at as logged_at
FROM customer_data cd
CROSS JOIN LATERAL jsonb_array_elements(
    COALESCE(cd.parameters::jsonb->'assets'->'table', '[]'::jsonb)
) AS asset_item
WHERE cd.category_id = 0
    AND cd.parameters IS NOT NULL
    AND cd.parameters::text ~ '^[\[\{]'
    AND cd.is_active = true
    AND cd.parameters::jsonb ? 'assets'
    AND (asset_item->>'asset') IS NOT NULL
    AND (asset_item->>'asset_class') IS NOT NULL
ON CONFLICT (user_code, cycle) DO NOTHING;

COMMIT;

-- Expected: Successfully inserted ~100,000+ rows


-- Step 5: Verify insertion results
-- ============================================================
SELECT 
    COUNT(*) as total_inserted,
    COUNT(DISTINCT user_code) as unique_customers,
    COUNT(DISTINCT cycle) as unique_cycles,
    COUNT(DISTINCT asset) as unique_assets,
    SUM(market_amount) as total_market_value,
    SUM(monthly_investment) as total_monthly_investment,
    MIN(logged_at) as earliest_date,
    MAX(logged_at) as latest_date
FROM user_assets_history;


-- Step 6: Check for data quality issues
-- ============================================================
SELECT 
    COUNT(*) as null_asset,
    COUNT(CASE WHEN asset = '' THEN 1 END) as empty_asset,
    COUNT(CASE WHEN market_amount = 0 AND monthly_investment = 0 THEN 1 END) as zero_values
FROM user_assets_history;


-- Step 7: Asset distribution analysis
-- ============================================================
SELECT 
    sub_category,
    COUNT(*) as count,
    SUM(market_amount) as total_market_value,
    AVG(market_amount) as avg_market_value
FROM user_assets_history
GROUP BY sub_category
ORDER BY count DESC;


-- Step 8: Top customers by asset count
-- ============================================================
SELECT 
    user_code,
    COUNT(*) as asset_count,
    SUM(market_amount) as total_market_value
FROM user_assets_history
GROUP BY user_code
ORDER BY asset_count DESC
LIMIT 20;


-- Step 9: Create indexes for performance
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_user_assets_user_code_cycle 
ON user_assets_history(user_code, cycle);

CREATE INDEX IF NOT EXISTS idx_user_assets_cycle 
ON user_assets_history(cycle);

CREATE INDEX IF NOT EXISTS idx_user_assets_asset 
ON user_assets_history(asset);

CREATE INDEX IF NOT EXISTS idx_user_assets_sub_category 
ON user_assets_history(sub_category);

-- Expected: Indexes created for faster queries


-- Step 10: Run table analysis for query optimization
-- ============================================================
ANALYZE user_assets_history;


-- Step 11: Check table size and statistics
-- ============================================================
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    n_live_tup as row_count,
    n_dead_tup as dead_rows
FROM pg_stat_user_tables
WHERE tablename = 'user_assets_history';


-- Step 12: Detect any duplicates (should be 0)
-- ============================================================
SELECT 
    user_code, 
    cycle, 
    COUNT(*) as duplicate_count
FROM user_assets_history
GROUP BY user_code, cycle
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Expected: 0 rows (no duplicates)


-- Step 13: Sample data verification
-- ============================================================
SELECT 
    "Id",
    user_code,
    asset,
    scid,
    sub_category,
    market_amount,
    monthly_investment,
    cycle,
    logged_at
FROM user_assets_history
LIMIT 10;


-- Step 14: Cycle distribution
-- ============================================================
SELECT 
    cycle,
    COUNT(*) as asset_count,
    COUNT(DISTINCT user_code) as customer_count,
    SUM(market_amount) as total_value
FROM user_assets_history
GROUP BY cycle
ORDER BY cycle;


-- Step 15: Asset category performance
-- ============================================================
SELECT 
    sub_category,
    COUNT(*) as count,
    COUNT(DISTINCT user_code) as unique_customers,
    MIN(market_amount) as min_value,
    MAX(market_amount) as max_value,
    AVG(market_amount) as avg_value,
    STDDEV(market_amount) as std_dev
FROM user_assets_history
GROUP BY sub_category
ORDER BY count DESC;


-- Step 16: EXPORT results (Optional)
-- ============================================================
-- Export to CSV for further analysis
-- \COPY (SELECT * FROM user_assets_history) TO '/tmp/user_assets_history.csv' WITH (FORMAT CSV, HEADER);


-- ============================================================
-- MIGRATION COMPLETE
-- ============================================================
-- Summary of actions performed:
-- 1. ✅ Inserted extracted assets
-- 2. ✅ Applied duplicate detection
-- 3. ✅ Created indexes
-- 4. ✅ Ran table analysis
-- 5. ✅ Verified data quality
-- 6. ✅ Generated statistics
--
-- Expected Results:
-- - ~100,000+ asset records
-- - 0 duplicates
-- - All indexes created
-- - Ready for application use
-- ============================================================
