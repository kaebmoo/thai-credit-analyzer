#!/bin/bash
set -e

echo "=========================================="
echo " ติดตั้ง Credit Card Analyzer"
echo "=========================================="
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "ไม่พบ Python3 — กรุณาติดตั้ง Python จาก https://python.org ก่อน"
    exit 1
fi

PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "พบ Python $PYTHON_VER"

# Create virtual environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo ""
    echo "กำลังสร้าง virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Install dependencies
echo ""
echo "กำลังติดตั้ง dependencies..."
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"

echo ""
echo "=========================================="
echo " ติดตั้งเสร็จแล้ว!"
echo " รันโปรแกรมด้วยคำสั่ง: ./run.sh"
echo "=========================================="