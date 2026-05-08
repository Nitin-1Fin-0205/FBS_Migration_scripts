const { sourcePool, destPool } = require('../utils/db-config');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const STREAM_LIMIT = 1000;

// ============================================================
// 1. CREATE STAGING TABLE
// ============================================================

async function createStagingTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS user_financial_scorecard_history_staged
        (
            id                BIGSERIAL PRIMARY KEY,
            scoring_metric    TEXT,
            category_id       BIGINT NOT NULL,
            ratio             TEXT,
            financial_score   DOUBLE PRECISION,
            ideal_per         DOUBLE PRECISION,
            ideal_value       DOUBLE PRECISION,
            user_per          DOUBLE PRECISION,
            user_value        DOUBLE PRECISION,
            variance_per      DOUBLE PRECISION,
            variance_value    DOUBLE PRECISION,
            guidance          TEXT,
            is_active         BOOLEAN DEFAULT TRUE NOT NULL,
            created_at        TIMESTAMP NOT NULL,
            user_code         UUID NOT NULL,
            less_than_per     DOUBLE PRECISION,
            more_than_per     DOUBLE PRECISION,
            ideal_range_per   TEXT,
            less_than_value   DOUBLE PRECISION DEFAULT 0.0 NOT NULL,
            more_than_value   DOUBLE PRECISION DEFAULT 0.0 NOT NULL,
            ideal_range_value TEXT,
            fbs_comments      TEXT,
            revision_factor   NUMERIC(6, 4),
            cycle             INTEGER DEFAULT 0 NOT NULL,
            UNIQUE (user_code, category_id, cycle)
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
// 3. EXTRACT FUNCTIONS FOR EACH JSON SECTION
// ============================================================


const r4 = (v) => v != null ? Math.round(v * 10000) / 10000 : null;

const ratios_category_mapping = [
    {
        "ratio": "Health Insurance",
        "category_id": 84
    },
    {
        "ratio": "Life Insurance",
        "category_id": 85
    },
    {
        "ratio": "Investments-to-Income",
        "category_id": 77
    },
    {
        "ratio": "Debt",
        "category_id": 81
    },
    {
        "ratio": "Good Liabilities-to-Total Assets",
        "category_id": 72
    },
    {
        "ratio": "Expense-to-Income",
        "category_id": 74
    },
    {
        "ratio": "Good Liability Linked EMI-to-Income",
        "category_id": 75
    },
    {
        "ratio": "Equity",
        "category_id": 78
    },
    {
        "ratio": "Alternative Investments",
        "category_id": 82
    },
    {
        "ratio": "Bad Liability Linked EMI-to-Income",
        "category_id": 76
    },
    {
        "ratio": "Real Estate",
        "category_id": 79
    },
    {
        "ratio": "Passive Income Assets",
        "category_id": 80
    },
    {
        "ratio": "Commodity",
        "category_id": 80
    },
    {
        "ratio": "Bad Liabilities-to-Total Assets",
        "category_id": 73
    },
    {
        "ratio": "Emergency Funds",
        "category_id": 83
    }
];

/**
 * Extract Emergency section data
 */
function extractEmergencyData(emergencyObj, customerId, userCode, cycle, createdAt, oneviewFbs) {
    const entries = [];

    if (!emergencyObj) return entries;

    for (const [key, data] of Object.entries(emergencyObj)) {
        if (data && typeof data === 'object' && data.title) {
            // Match by title, not by key
            const categoryId = ratios_category_mapping.find(item => item.ratio.trim().toLowerCase() === data.title.trim().toLowerCase())?.category_id;

            if (!categoryId) {
                console.log(`[Customer ${customerId}] ⚠️  Emergency: Category ID not found for title: "${data.title}"`);
            }

            const userVal = r4(parseFloat(data.total) || 0);
            const idealVal = r4(parseFloat(data.ideal_range) || 0);

            entries.push({
                scoring_metric: 'Emergency Planning',
                category_id: categoryId || 83,
                ratio: data.title,
                financial_score: oneviewFbs || 0,
                user_value: userVal,
                ideal_value: idealVal,
                variance_value: r4(idealVal - userVal),
                guidance: data.color || 'neutral',
                fbs_comments: data.comment || '',
                user_code: userCode,
                created_at: createdAt,
                cycle: cycle ?? 0,
                is_active: true
            });
        }
    }

    return entries;
}

/**
 * Extract Ratios section data
 * Ratio totals are decimals (e.g. 0.3598 = 35.98%); ideal_range is in percent strings.
 */
function extractRatiosData(ratiosObj, customerId, userCode, cycle, createdAt, oneviewFbs) {
    const entries = [];

    if (!ratiosObj) return entries;

    for (const [key, data] of Object.entries(ratiosObj)) {
        if (data && typeof data === 'object' && data.title) {
            const idealRange = data.ideal_range || '';
            let lessPer = null, morePer = null;

            // Ideal range is in "%" strings — divide by 100 to store as decimal fractions
            // "Up to X%" → only an upper bound; "X% - Y%" → both bounds
            const upToMatch = idealRange.match(/^up\s+to\s+([\d.]+)/i);
            if (upToMatch) {
                morePer = r4(parseFloat(upToMatch[1]) / 100);
            } else {
                const rangeMatch = idealRange.match(/[\d.]+/g);
                if (rangeMatch && rangeMatch.length >= 2) {
                    lessPer = r4(parseFloat(rangeMatch[0]) / 100);
                    morePer = r4(parseFloat(rangeMatch[rangeMatch.length - 1]) / 100);
                }
            }

            const idealRangePerStr = lessPer !== null
                ? `${lessPer} - ${morePer}`
                : morePer !== null ? `0 - ${morePer}` : '';

            const categoryId = ratios_category_mapping.find(item => item.ratio.trim().toLowerCase() === data.title.trim().toLowerCase())?.category_id;

            if (!categoryId) {
                console.log(`[Customer ${customerId}] ⚠️  Ratios: Category ID not found for title: "${data.title}"`);
            }

            const userPer = r4(parseFloat(data.total) || 0);
            // ideal_per: upper bound for "Up to X%", midpoint for "X% - Y%"
            const idealPer = r4(lessPer !== null ? (lessPer + morePer) / 2 : morePer);

            entries.push({
                scoring_metric: 'Expense and Liability Management',
                category_id: categoryId || 74,
                ratio: data.title,
                financial_score: oneviewFbs || 0,
                ideal_per: idealPer || null,
                user_per: userPer,
                variance_per: idealPer != null ? r4(idealPer - userPer) : null,
                less_than_per: lessPer,
                more_than_per: morePer,
                ideal_range_per: idealRangePerStr,
                guidance: data.color || 'neutral',
                fbs_comments: data.comment || '',
                user_code: userCode,
                created_at: createdAt,
                cycle: cycle ?? 0,
                is_active: true
            });
        }
    }

    return entries;
}

/**
 * Extract Asset Allocation section data
 */
function extractAssetAllocationData(assetAllocObj, customerId, userCode, cycle, createdAt, oneviewFbs) {
    const entries = [];

    if (!assetAllocObj) return entries;

    for (const [key, data] of Object.entries(assetAllocObj)) {
        if (data && typeof data === 'object' && data.title) {
            const idealRange = data.ideal_range || '';
            let lessPer = null, morePer = null;

            // Ideal range is in "%" strings — divide by 100 to store as decimal fractions
            // "Up to X%" → only an upper bound; "X% - Y%" → both bounds
            const upToMatchAlloc = idealRange.match(/^up\s+to\s+([\d.]+)/i);
            if (upToMatchAlloc) {
                morePer = r4(parseFloat(upToMatchAlloc[1]) / 100);
            } else {
                const percentMatch = idealRange.match(/[\d.]+/g);
                if (percentMatch && percentMatch.length >= 2) {
                    lessPer = r4(parseFloat(percentMatch[0]) / 100);
                    morePer = r4(parseFloat(percentMatch[percentMatch.length - 1]) / 100);
                }
            }

            const idealRangePerStr = lessPer !== null
                ? `${lessPer} - ${morePer}`
                : morePer !== null ? `0 - ${morePer}` : '';

            const categoryId = ratios_category_mapping.find(item => item.ratio.trim().toLowerCase() === data.title.trim().toLowerCase())?.category_id;

            if (!categoryId) {
                console.log(`[Customer ${customerId}] ⚠️  Asset Allocation: Category ID not found for title: "${data.title}"`);
            }

            const userPercentage = r4(parseFloat(data.total) || 0);
            // ideal_per: upper bound for "Up to X%", midpoint for "X% - Y%"
            const idealPerAlloc = r4(lessPer !== null ? (lessPer + morePer) / 2 : morePer);

            entries.push({
                scoring_metric: 'Recommended Asset Allocation',
                category_id: categoryId || 99,
                ratio: data.title,
                financial_score: oneviewFbs || 0,
                ideal_per: idealPerAlloc || null,
                user_per: userPercentage,
                variance_per: idealPerAlloc != null ? r4(idealPerAlloc - userPercentage) : null,
                less_than_per: lessPer,
                more_than_per: morePer,
                ideal_range_per: idealRangePerStr,
                guidance: data.color || 'neutral',
                fbs_comments: data.comment || '',
                user_code: userCode,
                created_at: createdAt,
                cycle: cycle ?? 0,
                is_active: true
            });
        }
    }

    return entries;
}

/**
 * Main extraction function that calls all sub-functions
 */
function extractFinancialScorecardFromJson(customerId, customerCode, cycle, parameters, createdAt) {
    try {
        // Parse parameters if it's a string
        let parsedParams = parameters;
        if (typeof parameters === 'string') {
            parsedParams = JSON.parse(parameters);
        }

        if (!parsedParams) {
            return [];
        }

        const allEntries = [];
        const oneviewFbs = parsedParams.oneview?.fbs || 0; // Get FBS score from oneview

        // Extract from emergency section
        if (parsedParams.emergency) {
            allEntries.push(...extractEmergencyData(parsedParams.emergency, customerId, customerCode, cycle, createdAt, oneviewFbs));
        }

        // Extract from ratios section
        if (parsedParams.ratios) {
            allEntries.push(...extractRatiosData(parsedParams.ratios, customerId, customerCode, cycle, createdAt, oneviewFbs));
        }

        // Extract from asset_allocation section
        if (parsedParams.asset_allocation) {
            allEntries.push(...extractAssetAllocationData(parsedParams.asset_allocation, customerId, customerCode, cycle, createdAt, oneviewFbs));
        }

        if (allEntries.length > 0) {
            console.log(`[Customer ${customerId}] ✅ Extracted ${allEntries.length} financial scorecard entries`);
        }

        return allEntries;
    } catch (error) {
        console.error(`Error extracting financial scorecard for customer ${customerId}: ${error.message}`);
        return [];
    }
}

// ============================================================
// 4. INSERT BATCH INTO STAGING TABLE
// ============================================================

async function insertFinancialScorecardBatch(scorecards) {
    if (scorecards.length === 0) return;

    const values = scorecards.map((scorecard, index) => {
        const offset = index * 20;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14}, $${offset + 15}, $${offset + 16}, $${offset + 17}, $${offset + 18}, $${offset + 19}, $${offset + 20})`;
    }).join(',');

    const flatValues = scorecards.flatMap(scorecard => [
        scorecard.scoring_metric,
        scorecard.category_id,
        scorecard.ratio,
        scorecard.financial_score,
        scorecard.ideal_per || null,
        scorecard.ideal_value || null,
        scorecard.user_per || null,
        scorecard.user_value || null,
        scorecard.variance_per || null,
        scorecard.variance_value || null,
        scorecard.guidance,
        scorecard.is_active,
        scorecard.created_at,
        scorecard.user_code,
        scorecard.less_than_per || null,
        scorecard.more_than_per || null,
        scorecard.ideal_range_per || null,
        scorecard.ideal_range_value || null,
        scorecard.fbs_comments || '',
        scorecard.cycle ?? 0,
    ]);

    const insertQuery = `
        INSERT INTO user_financial_scorecard_history_staged
        (scoring_metric, category_id, ratio, financial_score, ideal_per, ideal_value, user_per, user_value,
         variance_per, variance_value, guidance, is_active, created_at, user_code, less_than_per, more_than_per,
         ideal_range_per, ideal_range_value, fbs_comments, cycle)
        VALUES ${values}
        ON CONFLICT (user_code, category_id, cycle) DO NOTHING;
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

async function migrateFinancialScorecard() {
    let totalCustomers = 0;
    let totalScorecards = 0;
    let batchCount = 0;
    const startTime = Date.now();

    try {
        console.log('🚀 Starting financial scorecard migration with staging...\n');

        // Step 1: Create staging table
        await createStagingTable();

        let scorecardBatch = [];

        // Step 2: Stream and process customer data
        for await (const customerBatch of streamCustomerData()) {
            totalCustomers += customerBatch.length;

            for (const customer of customerBatch) {
                try {
                    // Step 3: Extract scorecards from JSON
                    const scorecards = extractFinancialScorecardFromJson(
                        customer.id,
                        customer.customer_code,
                        customer.cycle,
                        customer.parameters,
                        customer.created_at
                    );

                    totalScorecards += scorecards.length;
                    scorecardBatch = scorecardBatch.concat(scorecards);

                    // Step 4: Insert when batch reaches size
                    if (scorecardBatch.length >= BATCH_SIZE) {
                        await insertFinancialScorecardBatch(scorecardBatch);
                        batchCount++;
                        scorecardBatch = [];
                    }

                    // Progress logging
                    if (totalCustomers % 1000 === 0) {
                        console.log(`📈 Processed: ${totalCustomers} customers | Extracted: ${totalScorecards} scorecards`);
                    }
                } catch (error) {
                    console.error(`⚠️  Error processing customer ${customer.id}: ${error.message}`);
                }
            }
        }

        // Insert remaining scorecards
        if (scorecardBatch.length > 0) {
            await insertFinancialScorecardBatch(scorecardBatch);
            batchCount++;
        }

        // Summary
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log('\n' + '='.repeat(60));
        console.log('✅ FINANCIAL SCORECARD MIGRATION TO STAGING COMPLETE');
        console.log('='.repeat(60));
        console.log(`✅ Total customers processed: ${totalCustomers}`);
        console.log(`✅ Total scorecards extracted: ${totalScorecards}`);
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
    extractFinancialScorecardFromJson,
    insertFinancialScorecardBatch,
    migrateFinancialScorecard,
};
