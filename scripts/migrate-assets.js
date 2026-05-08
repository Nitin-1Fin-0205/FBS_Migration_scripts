const { sourcePool, destPool } = require('../utils/db-config');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const STREAM_LIMIT = 1000;

// ============================================================
// 1. CREATE STAGING TABLE
// ============================================================

async function createStagingTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS user_assets_history_staged (
            id BIGSERIAL PRIMARY KEY,
            user_code UUID,
            asset TEXT,
            scid BIGINT,
            sub_category TEXT,
            market_amount DOUBLE PRECISION,
            monthly_investment DOUBLE PRECISION,
            cycle BIGINT,
            logged_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    `;

    try {
        await destPool.query(createTableQuery);
        console.log('✅ Staging table created/verified');
    } catch (error) {
        console.error('❌ Error creating staging table:', error.message);
        throw error;
    }
}

// ============================================================
// 2. STREAM CUSTOMER DATA (Generator Function)
// ============================================================

async function* streamCustomerData() {
    let offset = 0;

    while (true) {
        try {
            const query = `
                SELECT 
                    id,
                    customer_code,
                    cycle,
                    parameters,
                    created_at,
                    is_active
                FROM customer_data
                WHERE category_id = 0
                    AND parameters IS NOT NULL
                ORDER BY id ASC
                LIMIT ${STREAM_LIMIT}
                OFFSET ${offset}
            `;

            const result = await sourcePool.query(query);

            if (result.rows.length === 0) break;

            yield result.rows;
            offset += result.rows.length;
        } catch (error) {
            console.error('❌ Error streaming from database:', error.message);
            throw error;
        }
    }
}

// ============================================================
// 2. EXTRACT ASSETS FROM JSON
// ============================================================

function extractAssetsFromJson(customerId, customerCode, cycle, parameters, createdAt) {
    try {
        // Parse parameters if it's a string
        let parsedParams = parameters;
        if (typeof parameters === 'string') {
            parsedParams = JSON.parse(parameters);
        }

        if (!parsedParams || !parsedParams.assets || !parsedParams.assets.table || !Array.isArray(parsedParams.assets.table)) {
            return [];
        }

        const assets = [];

        for (const item of parsedParams.assets.table) {
            try {
                // Validate required fields
                if (!item.asset || !item.asset_class) continue;

                // Parse numeric values safely
                const marketValue = parseFloat(item.market_value) || 0;
                const monthlyInvestment = parseFloat(item.monthly_investments) || 0;

                // Data quality check
                const assetName = (item.asset || '').trim();
                const assetClass = (item.asset_class || '').trim();

                if (!assetName || !assetClass) continue;

                assets.push({
                    user_code: customerCode,
                    asset: assetName,
                    scid: 0,
                    sub_category: assetClass,
                    market_amount: marketValue,
                    monthly_investment: monthlyInvestment,
                    cycle: cycle || 1,
                    logged_at: createdAt || new Date(),
                });
            } catch (e) {
                // Skip individual item errors
                continue;
            }
        }

        return assets;
    } catch (error) {
        console.error(`Error extracting assets for customer ${customerId}: ${error.message}`);
        return [];
    }
}

// ============================================================
// 3. INSERT BATCH INTO STAGING TABLE
// ============================================================

async function insertAssetsBatch(assets) {
    if (assets.length === 0) return;

    const values = assets.map((asset, index) => {
        const offset = index * 8;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8})`;
    }).join(',');

    const flatValues = assets.flatMap(asset => [
        asset.user_code,
        asset.asset,
        asset.scid,
        asset.sub_category,
        asset.market_amount,
        asset.monthly_investment,
        asset.cycle,
        asset.logged_at,
    ]);

    const insertQuery = `
        INSERT INTO user_assets_history_staged 
        (user_code, asset, scid, sub_category, market_amount, monthly_investment, cycle, logged_at)
        VALUES ${values}
        ON CONFLICT DO NOTHING;
    `;

    try {
        await destPool.query(insertQuery, flatValues);
    } catch (error) {
        console.error('Error inserting batch:', error.message);
        throw error;
    }
}

// ============================================================
// 4. MAIN MIGRATION FUNCTION
// ============================================================

async function migrateAssets() {
    let totalCustomers = 0;
    let totalAssets = 0;
    let batchCount = 0;
    const startTime = Date.now();

    try {
        console.log('🚀 Starting asset migration with staging...\n');

        // Step 1: Create staging table
        await createStagingTable();

        let assetsBatch = [];

        // Step 2: Stream and process customer data
        for await (const customerBatch of streamCustomerData()) {
            totalCustomers += customerBatch.length;

            for (const customer of customerBatch) {
                try {
                    // Step 3: Extract assets from JSON
                    const assets = extractAssetsFromJson(
                        customer.id,
                        customer.customer_code,
                        customer.cycle,
                        customer.parameters,
                        customer.created_at
                    );

                    totalAssets += assets.length;
                    assetsBatch = assetsBatch.concat(assets);

                    // Step 4: Insert when batch reaches size
                    if (assetsBatch.length >= BATCH_SIZE) {
                        await insertAssetsBatch(assetsBatch);
                        batchCount++;
                        assetsBatch = [];
                    }

                    // Progress logging
                    if (totalCustomers % 1000 === 0) {
                        console.log(`📈 Processed: ${totalCustomers} customers | Extracted: ${totalAssets} assets`);
                    }
                } catch (error) {
                    console.error(`⚠️  Error processing customer ${customer.id}: ${error.message}`);
                }
            }
        }

        // Insert remaining assets
        if (assetsBatch.length > 0) {
            await insertAssetsBatch(assetsBatch);
            batchCount++;
        }

        // Summary
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log('\n' + '='.repeat(60));
        console.log('✅ MIGRATION TO STAGING COMPLETE');
        console.log('='.repeat(60));
        console.log(`✅ Total customers processed: ${totalCustomers}`);
        console.log(`✅ Total assets extracted: ${totalAssets}`);
        console.log(`⏱️  Total time: ${elapsed}s`);
        console.log(`📦 Batches inserted: ${batchCount}`);
        console.log('='.repeat(60));

    } catch (error) {
        console.error(`❌ Migration failed: ${error.message}`);
        process.exit(1);
    } finally {
        await sourcePool.end();
        await destPool.end();
    }
}

// ============================================================
// EXPORTS
// ============================================================

module.exports = {
    createStagingTable,
    streamCustomerData,
    extractAssetsFromJson,
    insertAssetsBatch,
    migrateAssets,
};
