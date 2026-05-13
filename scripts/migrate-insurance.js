const { sourcePool, destPool, LackmasterDB } = require('../utils/db-config');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const STREAM_LIMIT = 1000;

// Category mapping cache
let INSURANCE_CATEGORY_MAPPING = [];

// ============================================================
// 1. CREATE STAGING TABLE
// ============================================================

async function createStagingTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS user_insurance_history_staged (
            id BIGSERIAL PRIMARY KEY,
            user_code UUID NOT NULL,
            cid BIGINT NOT NULL,
            insurance TEXT,
            scid BIGINT NOT NULL,
            sub_category TEXT,
            pending_tenure BIGINT,
            payment_frequency TEXT,
            periodic_premium_amt DOUBLE PRECISION,
            coverage DOUBLE PRECISION,
            cycle BIGINT NOT NULL,
            logged_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (user_code, scid, cycle)
        );
    `;

    try {
        await destPool.query(createTableQuery);
        console.log('✅ Insurance staging table created/verified');
    } catch (error) {
        console.error('❌ Error creating insurance staging table:', error.message);
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
// 3. GET CATEGORY MAPPING BY PLAN TYPE
// ============================================================

async function getCategoryMapping(planType) {
    try {
        const query = `
            SELECT 
                id as scid,
                name,
                parent_category_id as cid
            FROM category
            WHERE is_active = true
                AND name ILIKE $1;
        `;

        const result = await LackmasterDB.query(query, [`%${planType}%`]);

        if (result.rows.length > 0) {
            return {
                scid: result.rows[0].scid,
                cid: result.rows[0].cid,
                name: result.rows[0].name
            };
        }

        // Return null if not found
        return null;
    } catch (error) {
        console.error(`Error getting category mapping for "${planType}":`, error.message);
        // Return null on error
        return null;
    }
}

// ============================================================
// 4. EXTRACT INSURANCE FROM JSON
// ============================================================

async function extractInsuranceFromJson(customerId, customerCode, cycle, parameters, createdAt) {
    try {
        let parsedParams = parameters;
        if (typeof parameters === 'string') {
            parsedParams = JSON.parse(parameters);
        }

        const insuranceTable = parsedParams?.insurance_policy_evaluation?.table;
        if (!Array.isArray(insuranceTable) || insuranceTable.length === 0) {
            return [];
        }

        const entries = [];

        for (const item of insuranceTable) {
            try {
                // Validate required fields
                if (!item.policy_name || !item.plan_type) continue;

                // Parse numeric values safely
                const annualPremium = parseFloat(item.annual_premium) || 0;
                const lifeCover = parseFloat(item.life_cover) || 0;
                const policyTenure = (item.policy_tenure || '').trim();

                // Extract number of years from policy_tenure (e.g., "20 yrs" -> 20)
                const tenureMatch = policyTenure.match(/(\d+)/);
                const tenureYears = tenureMatch ? parseInt(tenureMatch[1]) : 0;

                const planType = (item.plan_type || '').trim();

                // Get category mapping from database by plan type
                const categoryMapping = await getCategoryMapping(planType);

                if (!categoryMapping) {
                    console.log(`[Customer ${customerId}] ⚠️  Insurance: Category not found for: "${planType}"`);
                    continue;
                }

                // Determine payment frequency (default to annual based on annual_premium field)
                const paymentFrequency = 'Annual';

                entries.push({
                    user_code: customerCode,
                    cid: categoryMapping.cid,
                    insurance: 'INSURANCE',
                    scid: categoryMapping.scid,
                    sub_category: planType,
                    pending_tenure: tenureYears,
                    payment_frequency: paymentFrequency,
                    periodic_premium_amt: annualPremium,
                    coverage: lifeCover,
                    cycle: cycle ?? 0,
                    logged_at: createdAt || new Date(),
                });
            } catch (e) {
                console.error(`[Customer ${customerId}] Error processing insurance item: ${e.message}`);
                continue;
            }
        }

        if (entries.length > 0) {
            console.log(`[Customer ${customerId}] ✅ Extracted ${entries.length} insurance entries`);
        }

        return entries;
    } catch (error) {
        console.error(`Error extracting insurance for customer ${customerId}: ${error.message}`);
        return [];
    }
}

// ============================================================
// 5. INSERT BATCH INTO STAGING TABLE
// ============================================================

async function insertInsuranceBatch(insurances) {
    if (insurances.length === 0) return;

    const values = insurances.map((insurance, index) => {
        const offset = index * 11;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11})`;
    }).join(',');

    const flatValues = insurances.flatMap(insurance => [
        insurance.user_code,
        insurance.cid,
        insurance.insurance,
        insurance.scid,
        insurance.sub_category,
        insurance.pending_tenure,
        insurance.payment_frequency,
        insurance.periodic_premium_amt,
        insurance.coverage,
        insurance.cycle,
        insurance.logged_at,
    ]);

    const insertQuery = `
        INSERT INTO user_insurance_history_staged 
        (user_code, cid, insurance, scid, sub_category, pending_tenure, payment_frequency, periodic_premium_amt, coverage, cycle, logged_at)
        VALUES ${values}
        ON CONFLICT (user_code, scid, cycle) DO NOTHING;
    `;

    try {
        await destPool.query(insertQuery, flatValues);
    } catch (error) {
        console.error('Error inserting batch:', error.message);
        throw error;
    }
}

// ============================================================
// 6. MAIN MIGRATION FUNCTION
// ============================================================

async function migrateInsurance() {
    let totalCustomers = 0;
    let totalInsurances = 0;
    let batchCount = 0;
    const startTime = Date.now();

    try {
        console.log('🚀 Starting insurance migration with staging...\n');

        // Step 1: Create staging table
        console.log('Step 1: Creating staging table...');
        await createStagingTable();

        let insuranceBatch = [];

        // Step 2: Stream and process customer data
        console.log('Step 2: Streaming customer data...\n');
        for await (const customerBatch of streamCustomerData()) {
            totalCustomers += customerBatch.length;

            for (const customer of customerBatch) {
                try {
                    // Step 3: Extract insurance from JSON
                    const insurances = await extractInsuranceFromJson(
                        customer.id,
                        customer.customer_code,
                        customer.cycle,
                        customer.parameters,
                        customer.created_at
                    );

                    totalInsurances += insurances.length;

                    // Add to batch
                    insuranceBatch.push(...insurances);

                    // Insert when batch reaches BATCH_SIZE
                    if (insuranceBatch.length >= BATCH_SIZE) {
                        await insertInsuranceBatch(insuranceBatch);
                        batchCount++;
                        console.log(`✅ Batch ${batchCount} inserted (${insuranceBatch.length} records)`);
                        insuranceBatch = [];
                    }
                } catch (error) {
                    console.error(`Error processing customer ${customer.id}:`, error.message);
                    continue;
                }
            }
        }

        // Step 4: Insert remaining records
        if (insuranceBatch.length > 0) {
            await insertInsuranceBatch(insuranceBatch);
            batchCount++;
            console.log(`✅ Final batch ${batchCount} inserted (${insuranceBatch.length} records)`);
        }

        const duration = ((Date.now() - startTime) / 1000).toFixed(2);

        console.log('\n' + '='.repeat(60));
        console.log('✅ Insurance Migration Completed Successfully');
        console.log('='.repeat(60));
        console.log(`📊 Total Customers Processed: ${totalCustomers}`);
        console.log(`📊 Total Insurance Records Extracted: ${totalInsurances}`);
        console.log(`📊 Total Batches Inserted: ${batchCount}`);
        console.log(`⏱️  Duration: ${duration} seconds`);
        console.log('='.repeat(60) + '\n');

    } catch (error) {
        console.error('❌ Insurance migration failed:', error.message);
        throw error;
    } finally {
        await sourcePool.end();
        await destPool.end();
        await LackmasterDB.end();
    }
}

// ============================================================
// 7. RUN MIGRATION
// ============================================================

migrateInsurance()
    .then(() => {
        console.log('✅ Migration completed');
        process.exit(0);
    })
    .catch((error) => {
        console.error('❌ Migration failed:', error);
        process.exit(1);
    });

module.exports = { migrateInsurance };
