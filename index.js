const { migrateAssets } = require('./scripts/migrate-assets');
const { migrateLiabilities } = require('./scripts/migrate-liabilities');
const { migrateInsurance } = require('./scripts/migrate-insurance');
const { migrateMutualFunds } = require('./scripts/migrate-mutual-funds');
const { migrateCreditCards } = require('./scripts/migrate-credit-cards');
const { migrateFinancialScorecard } = require('./scripts/migrate-financial-scorecard');

// ============================================================
// MIGRATION ORCHESTRATOR
// ============================================================

async function runMigration() {
    const migationType = process.argv[2] || 'assets';

    console.log('\n' + '='.repeat(60));
    console.log('Customer Data Migration');
    console.log('='.repeat(60) + '\n');

    try {
        console.log('Step 1: Importing migration modules...');

        if (migationType === 'assets') {
            console.log('✅ Asset migration\n');
            console.log('Step 2: Starting asset migration...');
            await migrateAssets();
        } else if (migationType === 'liabilities') {
            console.log('✅ Liabilities migration\n');
            console.log('Step 2: Starting liabilities migration...');
            await migrateLiabilities();
        } else if (migationType === 'insurance') {
            console.log('✅ Insurance migration\n');
            console.log('Step 2: Starting insurance migration...');
            await migrateInsurance();
        } else if (migationType === 'mutual-funds') {
            console.log('✅ Mutual funds migration\n');
            console.log('Step 2: Starting mutual funds migration...');
            await migrateMutualFunds();
        } else if (migationType === 'credit-cards') {
            console.log('✅ Credit cards migration\n');
            console.log('Step 2: Starting credit cards migration...');
            await migrateCreditCards();
        } else if (migationType === 'financial-scorecard') {
            console.log('✅ Financial scorecard migration\n');
            console.log('Step 2: Starting financial scorecard migration...');
            await migrateFinancialScorecard();
        } else {
            console.error(`Unknown migration type: ${migationType}`);
            console.log('  npm start                               # Run assets migration (default)');
            console.log('  npm start liabilities                   # Run liabilities migration');
            console.log('  npm start insurance                     # Run insurance migration');
            console.log('  npm start mutual-funds                  # Run mutual funds migration');
            console.log('  npm start credit-cards                  # Run credit cards migration');
            console.log('  npm start financial-scorecard           # Run financial scorecard migration');
            process.exit(1);
        }

        console.log('\nStep 3: Migration complete!');
        console.log('✅ All data successfully migrated to staging table\n');

    } catch (error) {
        console.error('\n❌ Migration failed:', error.message);
        process.exit(1);
    }
}

// ============================================================
// RUN MIGRATION
// ============================================================

runMigration().catch(error => {
    console.error('Fatal error:', error.message);
    process.exit(1);
});
