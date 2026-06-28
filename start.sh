#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# Cascade RV Solar Solutions — AI Receptionist Startup Script
# ─────────────────────────────────────────────────────────────────

echo "🌞 Starting Cascade RV Solar Solutions AI Receptionist..."
echo ""

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  No .env file found. Copying from .env.example..."
    cp .env.example .env
    echo "   Please edit .env with your API keys before continuing."
    echo "   Then run this script again."
    exit 1
fi

# Check for required packages
python3 -c "import fastapi, uvicorn, twilio, openai" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "📦 Installing dependencies..."
    pip install -r requirements.txt
fi

echo "🚀 Server starting on http://0.0.0.0:8000"
echo "   Dashboard: http://localhost:8000"
echo "   Leads:     http://localhost:8000/leads"
echo ""
echo "   Press Ctrl+C to stop"
echo ""

python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
