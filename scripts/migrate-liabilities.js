const { sourcePool, destPool } = require('../utils/db-config');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const STREAM_LIMIT = 1000;

// ============================================================
// 1. CREATE STAGING TABLE
// ============================================================

async function createStagingTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS user_liabilities_history_staged (
            "Id" BIGSERIAL PRIMARY KEY,
            user_code UUID NOT NULL,
            cid BIGINT NOT NULL,
            liability TEXT,
            scid BIGINT NOT NULL,
            sub_category TEXT,
            pending_tenure_amount NUMERIC,
            outstanding_amount DOUBLE PRECISION,
            monthly_emi_amount DOUBLE PRECISION,
            coverage DOUBLE PRECISION,
            account_age BIGINT,
            interest_rate NUMERIC,
            cycle BIGINT NOT NULL,
            logged_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (user_code, scid, cycle)
        );
    `;

    try {
        await destPool.query(createTableQuery);
        console.log('✅ Liabilities staging table created/verified');
    } catch (error) {
        console.error('❌ Error creating liabilities staging table:', error.message);
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
// 3. EXTRACT LIABILITIES FROM JSON
// ============================================================

const LIABILITIES_CATEGORY_MAPPING = [
    { id: 55,  name: 'Auto Loan',           parent_category_id: 15 },
    { id: 56,  name: 'Property Loan',       parent_category_id: 15 },
    { id: 57,  name: 'Housing Loan',        parent_category_id: 15 },
    { id: 58,  name: 'Car Loan',            parent_category_id: 15 },
    { id: 59,  name: 'Personal Loan',       parent_category_id: 15 },
    { id: 60,  name: 'Two-Wheeler Loan',    parent_category_id: 15 },
    { id: 61,  name: 'Consumer Loan',       parent_category_id: 15 },
    { id: 62,  name: 'Credit Card',         parent_category_id: 16 },
    { id: 104, name: 'Credit Card Loan',    parent_category_id: 15 },
    { id: 142, name: 'None',                parent_category_id: 15 },
    { id: 175, name: 'Education Loan',      parent_category_id: 15 },
    { id: 208, name: 'Gold Loan',           parent_category_id: 15 },
    { id: 209, name: 'Other Loan',          parent_category_id: 15 },
    { id: 360, name: 'Housing Loan Top-Up', parent_category_id: 15 },
];

function extractLiabilitiesFromJson(customerId, customerCode, cycle, parameters, createdAt) {
    try {
        let parsedParams = parameters;
        if (typeof parameters === 'string') {
            parsedParams = JSON.parse(parameters);
        }

        const liabilitiesTable = parsedParams?.liabilities?.table;
        if (!Array.isArray(liabilitiesTable) || liabilitiesTable.length === 0) {
            return [];
        }

        const entries = [];

        for (const item of liabilitiesTable) {
            const liabilityName = (item.liability || '').trim();
            const match = LIABILITIES_CATEGORY_MAPPING.find(
                m => m.name.trim().toLowerCase() === liabilityName.toLowerCase()
            );

            if (!match) {
                console.log(`[Customer ${customerId}] ⚠️  Liability: Category not found for: "${liabilityName}"`);
            }

            entries.push({
                user_code: customerCode,
                cid: match?.parent_category_id || 15,
                liability: liabilityName.toLowerCase().includes('credit') ? 'CREDIT CARD' : 'LOAN',
                scid: match?.id || 142,
                sub_category: liabilityName,
                pending_tenure_amount: item.pending_months || 0,
                outstanding_amount: parseFloat(item.outstanding_amount) || 0,
                monthly_emi_amount: parseFloat(item.emi) || 0,
                coverage: null,
                account_age: item.account_age_in_months || 0,
                interest_rate: item.interest_rate || 0,
                cycle: cycle ?? 0,
                logged_at: createdAt || new Date(),
            });
        }

        if (entries.length > 0) {
            console.log(`[Customer ${customerId}] ✅ Extracted ${entries.length} liability entries`);
        }

        return entries;
    } catch (error) {
        console.error(`Error extracting liabilities for customer ${customerId}: ${error.message}`);
        return [];
    }
}

// ============================================================
// 4. INSERT BATCH INTO STAGING TABLE
// ============================================================

async function insertLiabilitiesBatch(liabilities) {
    if (liabilities.length === 0) return;

    const values = liabilities.map((liability, index) => {
        const offset = index * 13;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13})`;
    }).join(',');

    const flatValues = liabilities.flatMap(liability => [
        liability.user_code,
        liability.cid,
        liability.liability,
        liability.scid,
        liability.sub_category,
        liability.pending_tenure_amount,
        liability.outstanding_amount,
        liability.monthly_emi_amount,
        liability.coverage,
        liability.account_age,
        liability.interest_rate,
        liability.cycle,
        liability.logged_at,
    ]);

    const insertQuery = `
        INSERT INTO user_liabilities_history_staged
        (user_code, cid, liability, scid, sub_category, pending_tenure_amount, outstanding_amount,
         monthly_emi_amount, coverage, account_age, interest_rate, cycle, logged_at)
        VALUES ${values}
        ON CONFLICT (user_code, scid, cycle) DO NOTHING;
    `;

    try {
        await destPool.query(insertQuery, flatValues);
    } catch (error) {
        console.error('Error inserting liabilities batch:', error.message);
        throw error;
    }
}

// ============================================================
// 5. MAIN MIGRATION FUNCTION
// ============================================================

async function migrateLiabilities() {
    let totalCustomers = 0;
    let totalLiabilities = 0;
    let batchCount = 0;
    const startTime = Date.now();

    try {
        console.log('🚀 Starting liabilities migration with staging...\n');

        // Step 1: Create staging table
        await createStagingTable();

        let liabilitiesBatch = [];

        // Step 2: Stream and process customer data
        for await (const customerBatch of streamCustomerData()) {
            totalCustomers += customerBatch.length;

            for (const customer of customerBatch) {
                try {
                    // Step 3: Extract liabilities from JSON
                    const liabilities = extractLiabilitiesFromJson(
                        customer.id,
                        customer.customer_code,
                        customer.cycle,
                        customer.parameters,
                        customer.created_at
                    );

                    totalLiabilities += liabilities.length;
                    liabilitiesBatch = liabilitiesBatch.concat(liabilities);

                    // Step 4: Insert when batch reaches size
                    if (liabilitiesBatch.length >= BATCH_SIZE) {
                        await insertLiabilitiesBatch(liabilitiesBatch);
                        batchCount++;
                        liabilitiesBatch = [];
                    }

                    // Progress logging
                    if (totalCustomers % 1000 === 0) {
                        console.log(`📈 Processed: ${totalCustomers} customers | Extracted: ${totalLiabilities} liabilities`);
                    }
                } catch (error) {
                    console.error(`⚠️  Error processing customer ${customer.id}: ${error.message}`);
                }
            }
        }

        // Insert remaining liabilities
        if (liabilitiesBatch.length > 0) {
            await insertLiabilitiesBatch(liabilitiesBatch);
            batchCount++;
        }

        // Summary
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log('\n' + '='.repeat(60));
        console.log('✅ LIABILITIES MIGRATION TO STAGING COMPLETE');
        console.log('='.repeat(60));
        console.log(`✅ Total customers processed: ${totalCustomers}`);
        console.log(`✅ Total liabilities extracted: ${totalLiabilities}`);
        console.log(`⏱️  Total time: ${elapsed}s`);
        console.log(`📦 Batches inserted: ${batchCount}`);
        console.log('='.repeat(60));

    } catch (error) {
        console.error(`❌ Liabilities migration failed: ${error.message}`);
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
    extractLiabilitiesFromJson,
    insertLiabilitiesBatch,
    migrateLiabilities,
};
