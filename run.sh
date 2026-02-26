#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "ยังไม่ได้ติดตั้ง — กรุณารัน ./install.sh ก่อน"
    exit 1
fi

echo "กำลังเปิด Credit Card Analyzer..."
echo "เปิด browser ที่ http://localhost:8501"
echo "กด Ctrl+C เพื่อปิดโปรแกรม"
echo ""

"$VENV_DIR/bin/streamlit" run "$SCRIPT_DIR/app.py" \
    --server.headless false \
    --browser.gatherUsageStats false \
    --theme.base light