#!/bin/bash
set -e

echo "Fixing database permissions..."

mkdir -p /app/data
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

echo "🌐 Starting Streamlit..."

exec streamlit run ui/app.py \
  --server.port=8503 \
  --server.address=0.0.0.0