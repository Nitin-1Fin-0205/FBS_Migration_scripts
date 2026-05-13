const { sourcePool, destPool } = require('../utils/db-config');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const STREAM_LIMIT = 1000;

// ============================================================
// 1. CREATE STAGING TABLE
// ============================================================

async function createStagingTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS mutual_funds_holdings_history_staged (
            id BIGSERIAL PRIMARY KEY,
            user_code UUID NOT NULL,
            isin TEXT,
            scheme_name TEXT,
            plan TEXT,
            category TEXT,
            scheme_type TEXT,
            current_value DOUBLE PRECISION,
            score DOUBLE PRECISION,
            quality TEXT,
            excess_annual_commissions DOUBLE PRECISION,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE NOT NULL,
            cycle BIGINT NOT NULL,
            logged_at TIMESTAMP WITH TIME ZONE NOT NULL,
            fetched_source TEXT,
            sip DOUBLE PRECISION,
            UNIQUE (user_code, scheme_name, cycle)
        );
    `;

    try {
        await destPool.query(createTableQuery);
        console.log('✅ Mutual funds holdings staging table created/verified');
    } catch (error) {
        console.error('❌ Error creating mutual funds holdings staging table:', error.message);
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
// 3. EXTRACT MUTUAL FUNDS FROM JSON
// ============================================================

function extractMutualFundsFromJson(customerId, customerCode, cycle, parameters, createdAt, isActive) {
    try {
        // Parse parameters if it's a string
        let parsedParams = parameters;
        if (typeof parameters === 'string') {
            parsedParams = JSON.parse(parameters);
        }

        if (!parsedParams || !parsedParams.mf_holding_evaluation || !parsedParams.mf_holding_evaluation.table || !Array.isArray(parsedParams.mf_holding_evaluation.table)) {
            return [];
        }

        const funds = [];

        for (const item of parsedParams.mf_holding_evaluation.table) {
            try {
                // Validate required fields
                if (!item.scheme_name) continue;

                // Parse numeric values safely
                const currentValue = parseFloat(item.current_value) || 0;
                const score = parseFloat(item.fund_evaluation_score) || 0;
                const excessAnnualExpense = parseFloat(item.excess_annual_commissions) || 0;

                // Data quality check
                const schemeName = (item.scheme_name || '').trim();
                const plan = (item.plan || '').trim();
                const category = (item.category || '').trim();
                const schemeType = (item.scheme_type || '').trim();
                const quality = (item.fund_evaluation_quality || '').trim();

                if (!schemeName) continue;

                funds.push({
                    user_code: customerCode,
                    isin: item.isin || null,
                    scheme_name: schemeName,
                    plan: plan,
                    category: category,
                    scheme_type: schemeType,
                    current_value: currentValue,
                    score: score,
                    quality: quality || null,
                    excess_annual_commissions: excessAnnualExpense,
                    created_at: createdAt || new Date(),
                    is_active: isActive || true,
                    cycle: cycle || 1,
                    logged_at: createdAt || new Date(),
                    fetched_source: null,
                    sip: null,
                });
            } catch (e) {
                // Skip individual item errors
                continue;
            }
        }

        if (funds.length > 0) {
            console.log(`[Customer ${customerId}] ✅ Extracted ${funds.length} mutual fund holdings`);
        }

        return funds;
    } catch (error) {
        console.error(`Error extracting mutual funds for customer ${customerId}: ${error.message}`);
        return [];
    }
}

// ============================================================
// 4. INSERT BATCH INTO STAGING TABLE
// ============================================================

async function insertMutualFundsBatch(funds) {
    if (funds.length === 0) return;

    const values = funds.map((fund, index) => {
        const offset = index * 16;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14}, $${offset + 15}, $${offset + 16})`;
    }).join(',');

    const flatValues = funds.flatMap(fund => [
        fund.user_code,
        fund.isin,
        fund.scheme_name,
        fund.plan,
        fund.category,
        fund.scheme_type,
        fund.current_value,
        fund.score,
        fund.quality,
        fund.excess_annual_commissions,
        fund.created_at,
        fund.is_active,
        fund.cycle,
        fund.logged_at,
        fund.fetched_source,
        fund.sip,
    ]);

    const insertQuery = `
        INSERT INTO mutual_funds_holdings_history_staged 
        (user_code, isin, scheme_name, plan, category, scheme_type, current_value, score, quality, excess_annual_commissions, created_at, is_active, cycle, logged_at, fetched_source, sip)
        VALUES ${values}
        ON CONFLICT (user_code, scheme_name, cycle) DO NOTHING;
    `;

    try {
        await destPool.query(insertQuery, flatValues);
    } catch (error) {
        console.error('Error inserting batch:', error.message);
        throw error;
    }
}

// ============================================================
// 5. MAIN MIGRATION FUNCTION
// ============================================================

async function migrateMutualFunds() {
    let totalCustomers = 0;
    let totalFunds = 0;
    let batchCount = 0;
    const startTime = Date.now();

    try {
        console.log('🚀 Starting mutual funds migration with staging...\n');

        // Step 1: Create staging table
        await createStagingTable();

        let fundsBatch = [];

        // Step 2: Stream and process customer data
        for await (const customerBatch of streamCustomerData()) {
            totalCustomers += customerBatch.length;

            for (const customer of customerBatch) {
                try {
                    // Step 3: Extract mutual funds from JSON
                    const funds = extractMutualFundsFromJson(
                        customer.id,
                        customer.customer_code,
                        customer.cycle,
                        customer.parameters,
                        customer.created_at,
                        customer.is_active
                    );

                    totalFunds += funds.length;

                    // Add to batch
                    fundsBatch.push(...funds);

                    // Insert when batch reaches BATCH_SIZE
                    if (fundsBatch.length >= BATCH_SIZE) {
                        await insertMutualFundsBatch(fundsBatch);
                        batchCount++;
                        console.log(`✅ Batch ${batchCount} inserted (${fundsBatch.length} records)`);
                        fundsBatch = [];
                    }
                } catch (error) {
                    console.error(`Error processing customer ${customer.id}:`, error.message);
                    continue;
                }
            }
        }

        // Step 4: Insert remaining records
        if (fundsBatch.length > 0) {
            await insertMutualFundsBatch(fundsBatch);
            batchCount++;
            console.log(`✅ Final batch ${batchCount} inserted (${fundsBatch.length} records)`);
        }

        const duration = ((Date.now() - startTime) / 1000).toFixed(2);

        console.log('\n' + '='.repeat(60));
        console.log('✅ Mutual Funds Migration Completed Successfully');
        console.log('='.repeat(60));
        console.log(`📊 Total Customers Processed: ${totalCustomers}`);
        console.log(`📊 Total Mutual Fund Holdings Extracted: ${totalFunds}`);
        console.log(`📊 Total Batches Inserted: ${batchCount}`);
        console.log(`⏱️  Duration: ${duration} seconds`);
        console.log('='.repeat(60) + '\n');

    } catch (error) {
        console.error('❌ Mutual funds migration failed:', error.message);
        throw error;
    } finally {
        await sourcePool.end();
        await destPool.end();
    }
}

// ============================================================
// 6. RUN MIGRATION
// ============================================================

migrateMutualFunds()
    .then(() => {
        console.log('✅ Migration completed');
        process.exit(0);
    })
    .catch((error) => {
        console.error('❌ Migration failed:', error);
        process.exit(1);
    });

module.exports = { migrateMutualFunds };
