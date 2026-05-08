# JSON Extraction Script

This script extracts asset data from `customer_data` JSON parameters in PostgreSQL.

## Overview

- **Processes**: ~15,800 customer records with JSON asset data
- **Source**: `customer_data.parameters` (JSON field)
- **Extraction**: Parses JSON `assets.table` array
- **Output**: JSON files with extracted asset data

## Features

✅ JSON data extraction and validation
✅ Safe numeric parsing (empty strings → 0)
✅ Data quality checks
✅ Progress tracking & logging
✅ Error handling & recovery
✅ Detailed extraction reports
✅ Saves extracted data to JSON file

## Setup

### Installation

```bash
# Install dependencies
npm install

# Copy environment file
cp .env.example .env

# Edit .env with your database credentials
nano .env
```

### Running the Script

```bash
# Start extraction
npm start

# Or directly run
node migrate-assets.js
```

### Output

- Console logs with real-time progress
- Log file: `extraction-log-YYYY-MM-DD.txt`
- Extracted JSON: `extracted-assets-YYYY-MM-DD.json`
- Summary with total customers and assets processed

---

## Environment Configuration

Create a `.env` file with your database credentials:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_secure_password
DB_NAME=your_database
```

**Important**: Never commit `.env` file with credentials to version control!

---

## Migration Process

### Step 1: Pre-Migration Validation (Database)

Run these SQL queries to validate data:

```sql
-- Check total records
SELECT COUNT(*) FROM customer_data 
WHERE category_id = 0 
  AND parameters IS NOT NULL 
  AND parameters::jsonb ? 'assets';

-- Check table structure
\d user_assets_history

-- Verify empty table (first run)
SELECT COUNT(*) FROM user_assets_history;
```

### Step 2: Run Migration Script

```bash
npm start    # Node.js
# OR
python3 migrate_assets.py  # Python
```

### Step 3: Monitor Progress

Watch the console output for:
- ✅ Successfully inserted records
- ⏭️ Duplicates skipped
- ❌ Errors encountered
- 📊 Processing rate (records/sec)

### Step 4: Post-Migration Verification

Run these SQL queries to verify results:

```sql
-- Total migrated records
SELECT COUNT(*) as total_records FROM user_assets_history;

-- Unique customers
SELECT COUNT(DISTINCT user_code) as unique_customers 
FROM user_assets_history;

-- Total market value
SELECT SUM(market_amount) as total_market_value 
FROM user_assets_history;

-- Check for any duplicates (should return 0)
SELECT user_code, cycle, COUNT(*) as duplicate_count
FROM user_assets_history
GROUP BY user_code, cycle
HAVING COUNT(*) > 1;

-- Sample data
SELECT * FROM user_assets_history LIMIT 10;
```

---

## Data Quality

### Validations Performed

1. ✅ JSON structure validation (must be valid JSON)
2. ✅ Required fields check (asset, asset_class)
3. ✅ Numeric value parsing (market_value, monthly_investments)
4. ✅ Empty string handling (defaults to 0)
5. ✅ Duplicate detection (user_code + cycle)
6. ✅ UUID validation (user_code)

### Error Handling

- Invalid numeric values → converted to 0.0
- Empty strings → converted to 0.0
- Missing required fields → record skipped
- Duplicate records → skipped (not re-inserted)
- Database errors → logged and continued

---

## Performance Metrics

### Expected Results

- **Records to Process**: ~15,800
- **Processing Speed**: 100-500 records/sec (depends on system)
- **Batch Size**: 500 records
- **Expected Duration**: 30-160 seconds (~1-3 minutes)

### Optimization Tips

1. **Increase Batch Size** (if memory allows):
   - Node.js: Edit `BATCH_SIZE` constant in `migrate-assets.js`
   - Python: Edit `self.batch_size` in `migrate_assets.py`

2. **Disable Duplicate Checks** (if you know data is clean):
   - Comment out `checkDuplicate()` calls

3. **Parallel Processing** (Node.js only):
   - Modify to use `Promise.all()` for concurrent inserts

---

## Troubleshooting

### Connection Error
```
Error: connect ECONNREFUSED 127.0.0.1:5432
```
**Solution**: Check if PostgreSQL is running and credentials are correct in `.env`

### Table Not Found
```
Error: relation "user_assets_history" does not exist
```
**Solution**: Run the CREATE TABLE statement first

### Invalid JSON
```
Error: invalid input syntax for type json
```
**Solution**: Check if `parameters` column contains valid JSON

### Out of Memory
```
JavaScript heap out of memory
```
**Solution**: Reduce `BATCH_SIZE` or add `--max-old-space-size` flag:
```bash
node --max-old-space-size=4096 migrate-assets.js
```

### Permission Denied
```
Error: permission denied for schema public
```
**Solution**: Ensure database user has INSERT permission on `user_assets_history`

---

## Log Files

Migration logs are saved to:
- `migration-log-YYYY-MM-DD.txt`

Contains:
- Timestamp of each operation
- Processing progress
- Errors and warnings
- Final summary

---

## Rollback Instructions

If you need to rollback the migration:

```sql
-- Delete all migrated records
DELETE FROM user_assets_history 
WHERE logged_at >= '2024-MM-DD'::timestamp;

-- Or completely empty the table
TRUNCATE user_assets_history;

-- Verify
SELECT COUNT(*) FROM user_assets_history;
```

---

## Database Table Schema

```sql
CREATE TABLE user_assets_history (
    "Id" bigint NOT NULL,
    user_code uuid NOT NULL,
    asset text,
    scid bigint NOT NULL,
    sub_category text,
    market_amount double precision,
    monthly_investment double precision,
    cycle bigint NOT NULL,
    logged_at timestamp with time zone DEFAULT now() NOT NULL
);
```

---

## Next Steps After Migration

1. ✅ Run verification queries
2. ✅ Review migration logs
3. ✅ Create indexes for performance:
   ```sql
   CREATE INDEX idx_user_code_cycle ON user_assets_history(user_code, cycle);
   CREATE INDEX idx_logged_at ON user_assets_history(logged_at DESC);
   ```
4. ✅ Analyze table:
   ```sql
   ANALYZE user_assets_history;
   ```
5. ✅ Archive migration logs
6. ✅ Clean up staging/temporary tables

---

## Support

For issues or questions, check:
1. Migration log files
2. Database error logs
3. Verify environment variables
4. Check database connectivity

---

## License

ISC
# FBS_Migration_scripts
