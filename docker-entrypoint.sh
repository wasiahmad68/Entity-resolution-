#!/bin/bash
set -e

echo "Fixing database permissions..."

mkdir -p /app/data

# ✅ SQLite-safe permissions
chmod -R 777 /app/data

echo "🚀 Starting ER Engine..."

DB_PATH="/app/data/er_engine.db"

if [ ! -f "$DB_PATH" ]; then
    echo "📦 Database not found."
    echo "🛠 Creating database and tables..."

python - <<EOF
from er_engine.database.session import init_db, Base

init_db()

print("✅ Tables created:")
for table in Base.metadata.tables.keys():
    print(f"   - {table}")
EOF

    echo "✅ Database initialization completed."
else
    echo "✅ Database already exists."
fi

exec tail -f /dev/null