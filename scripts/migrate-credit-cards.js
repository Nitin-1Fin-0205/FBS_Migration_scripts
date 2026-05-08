const { sourcePool, destPool, LackmasterDB } = require('../utils/db-config');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const STREAM_LIMIT = 1000;

// ============================================================
// 1. CREATE STAGING TABLE
// ============================================================

async function createStagingTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS customer_credit_cards_history_staged (
            id BIGSERIAL PRIMARY KEY,
            user_code UUID NOT NULL,
            card_id BIGINT NOT NULL,
            card_name TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            annual_fee TEXT,
            best_suited TEXT,
            reward_conversion TEXT,
            category JSONB,
            complimentary_access JSONB,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            bank_name TEXT,
            not_suited TEXT,
            rewards_points_redemption JSONB,
            cycle BIGINT NOT NULL,
            logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (user_code, card_id, cycle)
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
// 3. EXTRACT CREDIT CARDS FROM JSON
// ============================================================

async function extractCreditCardsFromJson(customerId, customerCode, cycle, parameters, createdAt, updated_at) {
    try {
        // Parse parameters if it's a string
        let parsedParams = parameters;
        if (typeof parameters === 'string') {
            parsedParams = JSON.parse(parameters);
        }

        // Credit cards are inside featured_list.credit_card
        let creditCardArray = [];
        if (parsedParams?.featured_list?.credit_card) {
            if (Array.isArray(parsedParams.featured_list.credit_card)) {
                creditCardArray = parsedParams.featured_list.credit_card;
            } else {
                console.log(`[Customer ${customerId}] ⚠️  featured_list.credit_card exists but is NOT an array, type:`, typeof parsedParams.featured_list.credit_card);
            }
        } else {
            // No cards found for this customer
        }
        if (creditCardArray.length === 0) {
            return [];
        }

        const creditCards = [];

        for (const card of creditCardArray) {
            try {

                // Validate required fields
                if (!card.card_name) {
                    continue;
                }

                // Query customer_credit_cards table to get card_id and other details
                const cardQuery = `
                    SELECT 
                        id as card_id,
                        bank_name,
                        category,
                        complimentary_access,
                        rewards_points_redemption
                    FROM customer_credit_cards
                    WHERE 
                        TRIM(LOWER(card_name)) = TRIM(LOWER($1))
                        AND is_active = true
                    LIMIT 1
                `;

                let cardResult;
                try {
                    cardResult = await LackmasterDB.query(cardQuery, [card.card_name]);
                } catch (queryErr) {
                    cardResult = { rows: [] };
                }

                let generatedCardId = 0;


                let cardInfo = {
                    card_id: generatedCardId,
                    bank_name: '',
                    category: null,
                    complimentary_access: null,
                    rewards_points_redemption: null,
                    not_suited: ''
                };

                if (cardResult.rows && cardResult.rows.length > 0) {
                    cardInfo = cardResult.rows[0];
                    if (!cardInfo.card_id) {
                        cardInfo.card_id = generatedCardId;
                    }
                }


                const cardObj = {
                    user_code: customerCode,
                    card_id: cardInfo.card_id,
                    card_name: (card.card_name || '').trim(),
                    is_active: true,
                    annual_fee: (card.annual_fee || '').trim(),
                    best_suited: (card.best_suited_for || '').trim(),
                    reward_conversion: (card.best_reward_points_conversion_rate || '').trim(),
                    category: cardInfo.category || null,
                    complimentary_access: cardInfo.complimentary_access || null,
                    created_at: createdAt || new Date(),
                    updated_at: updated_at || new Date(),
                    bank_name: cardInfo.bank_name || '',
                    not_suited: (cardInfo.not_suited || (card.weakness || []).join(', ')).trim(),
                    rewards_points_redemption: cardInfo.rewards_points_redemption || null,
                    cycle: cycle || 1,
                };

                creditCards.push(cardObj);
            } catch (e) {
                // Skip individual card errors
                continue;
            }
        }

        return creditCards;
    } catch (error) {
        console.error(`Error extracting credit cards for customer ${customerId}: ${error.message}`);
        return [];
    }
}

// ============================================================
// 4. INSERT BATCH INTO STAGING TABLE
// ============================================================

async function insertCreditCardsBatch(creditCards) {
    if (creditCards.length === 0) return;

    const values = creditCards.map((card, index) => {
        const offset = index * 15;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14}, $${offset + 15})`;
    }).join(',');

    const flatValues = creditCards.flatMap(card => [
        card.user_code,
        card.card_id,
        card.card_name,
        card.is_active,
        card.annual_fee,
        card.best_suited,
        card.reward_conversion,
        card.category,
        card.complimentary_access,
        card.created_at,
        card.updated_at,
        card.bank_name,
        card.not_suited,
        card.rewards_points_redemption,
        card.cycle,
    ]);

    const insertQuery = `
        INSERT INTO customer_credit_cards_history_staged 
        (user_code, card_id, card_name, is_active, annual_fee, best_suited, reward_conversion, category, 
         complimentary_access, created_at, updated_at, bank_name, not_suited, rewards_points_redemption, cycle)
        VALUES ${values}
        ON CONFLICT (user_code, card_id, cycle) DO NOTHING;
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

async function migrateCreditCards() {
    let totalCustomers = 0;
    let totalCards = 0;
    let batchCount = 0;
    const startTime = Date.now();

    try {
        console.log('🚀 Starting credit cards migration with staging...\n');

        // Step 1: Create staging table
        await createStagingTable();

        let cardsBatch = [];

        // Step 2: Stream and process customer data
        for await (const customerBatch of streamCustomerData()) {
            totalCustomers += customerBatch.length;

            for (const customer of customerBatch) {
                try {
                    // Step 3: Extract credit cards from JSON
                    const cards = await extractCreditCardsFromJson(
                        customer.id,
                        customer.customer_code,
                        customer.cycle,
                        customer.parameters,
                        customer.created_at,
                        new Date()
                    );

                    if (cards.length > 0) {
                        totalCards += cards.length;
                    }

                    cardsBatch = cardsBatch.concat(cards);

                    // Step 4: Insert when batch reaches size
                    if (cardsBatch.length >= BATCH_SIZE) {
                        await insertCreditCardsBatch(cardsBatch);
                        batchCount++;
                        cardsBatch = [];
                    }

                    // Progress logging
                    if (totalCustomers % 1000 === 0) {
                        console.log(`📈 Processed: ${totalCustomers} customers | Extracted: ${totalCards} credit cards`);
                    }
                } catch (error) {
                    console.error(`⚠️  Error processing customer ${customer.id}: ${error.message}`);
                }
            }
        }

        // Insert remaining cards
        if (cardsBatch.length > 0) {
            await insertCreditCardsBatch(cardsBatch);
            batchCount++;
        }

        // Summary
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log('\n' + '='.repeat(60));
        console.log('✅ CREDIT CARDS MIGRATION TO STAGING COMPLETE');
        console.log('='.repeat(60));
        console.log(`✅ Total customers processed: ${totalCustomers}`);
        console.log(`✅ Total credit cards extracted: ${totalCards}`);
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
    extractCreditCardsFromJson,
    insertCreditCardsBatch,
    migrateCreditCards,
};
