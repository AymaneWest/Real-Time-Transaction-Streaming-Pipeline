db = db.getSiblingDB('transactions_db');

db.createCollection('transactions');

db.transactions.createIndex({ timestamp: -1 });
db.transactions.createIndex({ source: 1 });
db.transactions.createIndex({ transaction_id: 1 }, { unique: true });

print('MongoDB initialized: transactions_db.transactions collection ready.');