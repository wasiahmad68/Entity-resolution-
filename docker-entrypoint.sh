#!/bin/bash

echo "🚀 Starting ER Engine..."

echo "📦 Initializing database..."
python init_db.py

echo "✅ Database Ready"

# Keep container alive
tail -f /dev/null