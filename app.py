"""
Credit Card Analyzer
วิเคราะห์ค่าใช้จ่ายบัตรเครดิต - ข้อมูลทั้งหมดเก็บบนเครื่องตัวเอง
"""

import streamlit as st
import sqlite3
import json
import io
import os
from datetime import datetime
from pathlib import Path

from google import genai
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image
import fitz  # PyMuPDF
from dotenv import load_dotenv
import hashlib

# ─── Config ───────────────────────────────────────────────────────────────────

# Database location: default = project folder, set USE_HOME_DIR=true to use home directory
if os.environ.get("USE_HOME_DIR", "").lower() == "true":
    DB_DIR = Path.home() / ".credit_analyzer"
else:
    DB_DIR = Path(__file__).parent / "data"

DB_DIR.mkdir(exist_ok=True)
DB_PATH   = DB_DIR / "data.db"
CONFIG_PATH = DB_DIR / "config.json"

# .env ในโฟลเดอร์โปรแกรม (ถ้ามี) จะถูก load อัตโนมัติ
# Priority: session_state (UI) > .env > config.json
_APP_DIR = Path(__file__).parent
load_dotenv(_APP_DIR / ".env", override=False)

CATEGORIES = [
    "ประกัน",
    "Subscription/ดิจิทัล",
    "ร้านสะดวกซื้อ",
    "ช้อปปิ้งออนไลน์",
    "ทางด่วน/ขนส่ง",
    "อาหาร/เครื่องดื่ม",
    "ซุปเปอร์มาร์เก็ต",
    "โทรศัพท์/อินเทอร์เน็ต",
    "เดินทาง",
    "บริการรถยนต์",
    "สุขภาพ",
    "ช้อปปิ้ง",
    "อื่นๆ",
]

SUBCATEGORIES = {
    "อาหาร/เครื่องดื่ม": ["ร้านอาหาร", "คาเฟ่", "Food Delivery", "ซื้อของกิน"],
    "ช้อปปิ้งออนไลน์": ["Shopee", "Lazada", "Amazon", "อื่นๆ"],
    "ทางด่วน/ขนส่ง": ["ทางด่วน", "แท็กซี่/Grab", "รถไฟฟ้า", "น้ำมัน"],
    "เดินทาง": ["โรงแรม", "ตั๋วเครื่องบิน", "ทัวร์", "ที่เที่ยว", "รถเช่า"],
    "Subscription/ดิจิทัล": ["Netflix/Streaming", "Spotify/Music", "เกม", "Cloud/Software"],
    "ร้านสะดวกซื้อ": ["CJ", "7-11", "Family Mart", "Lotus Go", "อื่นๆ"],
    "ซุปเปอร์มาร์เก็ต": ["Lotus", "Big C", "Tops", "Villa Market", "Makro"],
    "โทรศัพท์/อินเทอร์เน็ต": ["AIS", "True", "DTAC", "NT", "3BB/Fiber"],
    "ประกัน": ["รถยนต์", "สุขภาพ", "ชีวิต", "ทรัพย์สิน"],
    "บริการรถยนต์": ["ยาง/เบรก", "น้ำมันเชื้อเพลิง", "แบตเตอรี่", "ซ่อมบำรุง", "ล้างรถ"],
    "สุขภาพ": ["โรงพยาบาล", "คลินิก", "ร้านขายยา", "ทันตกรรม", "ตรวจสุขภาพ"],
    "ช้อปปิ้ง": ["กีฬา", "เสื้อผ้า", "ของใช้บ้าน", "ห้างสรรพสินค้า", "อิเล็กทรอนิกส์"],
}

# ─── Database ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS statements (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filename    TEXT,
            bank        TEXT,
            period      TEXT,
            imported_at TEXT,
            tx_count    INTEGER,
            cutoff_day  INTEGER,
            file_hash   TEXT
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id  INTEGER,
            trans_date    TEXT,
            posting_date  TEXT,
            description   TEXT,
            amount        REAL,
            category      TEXT,
            subcategory   TEXT,
            bank          TEXT,
            FOREIGN KEY (statement_id) REFERENCES statements(id)
        );
    """)

    # Migration: add missing columns for existing databases (won't run on fresh install)
    stmt_cols = {row["name"] for row in conn.execute("PRAGMA table_info(statements)").fetchall()}
    tx_cols   = {row["name"] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
    if "cutoff_day" not in stmt_cols:
        conn.execute("ALTER TABLE statements ADD COLUMN cutoff_day INTEGER")
    if "file_hash" not in stmt_cols:
        conn.execute("ALTER TABLE statements ADD COLUMN file_hash TEXT")
    if "subcategory" not in tx_cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN subcategory TEXT")

    conn.commit()
    conn.close()


def save_transactions(bank: str, filenames: list, transactions: list,
                      cutoff_day: int = None, file_hashes: list = None):
    conn = get_db()
    try:
        dates = [t["trans_date"] for t in transactions if t.get("trans_date")]
        period = max(dates)[:7] if dates else datetime.now().strftime("%Y-%m")

        hash_str = ",".join(file_hashes) if file_hashes else None
        cur = conn.execute(
            "INSERT INTO statements (filename, bank, period, imported_at, tx_count, cutoff_day, file_hash) VALUES (?,?,?,?,?,?,?)",
            (", ".join(filenames), bank, period, datetime.now().isoformat(), len(transactions), cutoff_day, hash_str),
        )
        stmt_id = cur.lastrowid

        for t in transactions:
            conn.execute(
                """INSERT INTO transactions
                   (statement_id, trans_date, posting_date, description, amount, category, subcategory, bank)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    stmt_id,
                    t.get("trans_date", ""),
                    t.get("posting_date", ""),
                    t.get("description", ""),
                    float(t.get("amount", 0)),
                    t.get("category", "อื่นๆ"),
                    t.get("subcategory"),
                    bank,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def delete_statement(stmt_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM transactions WHERE statement_id=?", (stmt_id,))
        conn.execute("DELETE FROM statements WHERE id=?", (stmt_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_transactions(period: str = "all") -> pd.DataFrame:
    conn = get_db()
    df = pd.read_sql(
        """SELECT t.id, t.trans_date, t.description, t.amount, t.category, t.subcategory, t.bank,
                  s.period, s.id as statement_id, s.cutoff_day
           FROM transactions t
           JOIN statements s ON t.statement_id = s.id
           ORDER BY t.trans_date DESC""",
        conn,
    )
    conn.close()
    if df.empty:
        return df
    df["trans_date"] = pd.to_datetime(df["trans_date"], errors="coerce")

    # Filter by period
    if period == "current_month":
        now = pd.Timestamp.now()
        df = df[(df["trans_date"].dt.year == now.year) &
                (df["trans_date"].dt.month == now.month)]
    elif period == "last_month":
        last_month = pd.Timestamp.now() - pd.DateOffset(months=1)
        df = df[(df["trans_date"].dt.year == last_month.year) &
                (df["trans_date"].dt.month == last_month.month)]
    elif period in ["3_months", "6_months"]:
        # Get all unique months in data, sorted newest first
        all_months = sorted(df["trans_date"].dt.to_period("M").dropna().unique(), reverse=True)
        n = 3 if period == "3_months" else 6
        recent_months = all_months[:n]
        df = df[df["trans_date"].dt.to_period("M").isin(recent_months)]
    # else: period == "all" -> no filter

    return df


def load_statements() -> pd.DataFrame:
    conn = get_db()
    df = pd.read_sql("SELECT * FROM statements ORDER BY imported_at DESC", conn)
    conn.close()
    return df


def get_previous_banks() -> list[str]:
    """ดึงรายชื่อธนาคาร/บัตรที่เคยนำเข้ามาแล้ว เรียงตามล่าสุดก่อน (ไม่ซ้ำกัน)"""
    conn = get_db()
    rows = conn.execute(
        "SELECT bank, MAX(imported_at) as last_import "
        "FROM statements WHERE bank IS NOT NULL AND bank != '' "
        "GROUP BY bank ORDER BY last_import DESC"
    ).fetchall()
    conn.close()
    return [row["bank"].strip() for row in rows if row["bank"].strip()]


def compute_file_hash(raw: bytes) -> str:
    """คำนวณ SHA-256 hash ของไฟล์ เพื่อตรวจสอบไฟล์ซ้ำ"""
    return hashlib.sha256(raw).hexdigest()


def find_duplicate_statement(file_hash: str) -> dict | None:
    """ตรวจสอบว่าไฟล์นี้เคยนำเข้าแล้วหรือไม่ โดยเทียบ SHA-256 hash
    คืนค่าข้อมูล statement เดิมถ้าพบ ไม่งั้นคืน None"""
    if not file_hash:
        return None
    conn = get_db()
    # file_hash อาจเก็บหลาย hash คั่นด้วย "," (กรณีนำเข้าหลายไฟล์พร้อมกัน)
    rows = conn.execute(
        "SELECT id, filename, bank, period, imported_at, file_hash FROM statements WHERE file_hash IS NOT NULL"
    ).fetchall()
    conn.close()
    for row in rows:
        stored = [h.strip() for h in (row["file_hash"] or "").split(",")]
        if file_hash in stored:
            return dict(row)
    return None


def find_similar_statement(bank: str, period: str, total_amount: float,
                           tolerance: float = 0.05) -> list[dict]:
    """ตรวจสอบ statement ที่ period ตรงกัน และยอดรวมใกล้เคียง (±tolerance)
    ตรวจทั้งแบบ exact (period+bank) และ soft (period อย่างเดียว)
    คืน list ของ statement ที่น่าสงสัยว่าเป็นข้อมูลซ้ำ"""
    conn = get_db()
    # ดึง statement ทุกอันที่ period ตรงกัน (ไม่จำกัด bank เพื่อรองรับชื่อที่ OCR อ่านต่างกัน)
    rows = conn.execute(
        """
        SELECT s.id, s.filename, s.bank, s.period, s.imported_at,
               COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END), 0) as total_amount
        FROM statements s
        LEFT JOIN transactions t ON t.statement_id = s.id
        WHERE s.period = ?
        GROUP BY s.id
        """,
        (period,),
    ).fetchall()
    conn.close()

    results = []
    for row in rows:
        existing_total = row["total_amount"]
        if existing_total == 0 and total_amount == 0:
            results.append({**dict(row), "diff_ratio": 0.0, "bank_match": row["bank"] == bank})
            continue
        if existing_total == 0:
            continue
        diff_ratio = abs(total_amount - existing_total) / max(abs(existing_total), 1)
        if diff_ratio <= tolerance:
            results.append({**dict(row), "diff_ratio": diff_ratio, "bank_match": row["bank"] == bank})
    return results


def find_overlapping_transactions(transactions: list[dict]) -> dict:
    """ตรวจสอบว่ารายการใน transactions ซ้ำกับที่อยู่ใน DB มากแค่ไหน
    ใช้ 2 วิธีคู่กัน:
      - exact: trans_date + description + amount (±1) — แม่นยำ แต่อาจพลาดถ้า OCR ต่างกัน
      - soft : trans_date + amount (±1) เท่านั้น — ตรวจจับได้แม้ description ต่างกัน
    คืน dict: {exact_count, soft_count, overlap_ratio, total}"""
    if not transactions:
        return {"exact_count": 0, "soft_count": 0, "overlap_ratio": 0.0, "total": 0}

    conn = get_db()
    exact_count = 0
    soft_count = 0
    for t in transactions:
        date_str = t.get("trans_date", "")
        desc = t.get("description", "")
        amount = float(t.get("amount", 0))
        if amount <= 0:
            continue  # ข้าม cashback/negative

        # exact match
        exact = conn.execute(
            """
            SELECT id FROM transactions
            WHERE trans_date = ? AND description = ?
              AND ABS(amount - ?) < 1.0
            LIMIT 1
            """,
            (date_str, desc, amount),
        ).fetchone()
        if exact:
            exact_count += 1

        # soft match (date + amount เท่านั้น)
        soft = conn.execute(
            """
            SELECT id FROM transactions
            WHERE trans_date = ? AND ABS(amount - ?) < 1.0
            LIMIT 1
            """,
            (date_str, amount),
        ).fetchone()
        if soft:
            soft_count += 1

    conn.close()

    # นับเฉพาะรายการ amount > 0 เป็น base
    positive_total = sum(1 for t in transactions if float(t.get("amount", 0)) > 0)
    if positive_total == 0:
        return {"exact_count": 0, "soft_count": 0, "overlap_ratio": 0.0, "total": len(transactions)}

    # ใช้ soft_count เป็น ratio หลัก (ตรวจจับได้ดีกว่าเมื่อมาจากรูปภาพ)
    ratio = soft_count / positive_total
    return {
        "exact_count": exact_count,
        "soft_count": soft_count,
        "overlap_ratio": ratio,
        "total": len(transactions),
        "positive_total": positive_total,
    }

# ─── Gemini ───────────────────────────────────────────────────────────────────

def get_model():
    key = st.session_state.get("api_key", "")
    if not key:
        st.error("กรุณาใส่ Gemini API Key ในหน้า 'ตั้งค่า' ก่อน")
        st.stop()
    return genai.Client(api_key=key)


def extract_from_image(model, image: Image.Image) -> dict:
    prompt = """คุณคือผู้ช่วยอ่าน statement บัตรเครดิต

อ่านรายการธุรกรรมและข้อมูลบิลทั้งหมดในภาพ แล้วส่งออกเป็น JSON object เท่านั้น
ห้ามมีข้อความอื่น ห้ามมี markdown

รูปแบบที่ต้องการ:
{
  "transactions": [
    {
      "trans_date": "2026-01-15",
      "posting_date": "2026-01-16",
      "description": "TMN 7-11 BANGKOK TH",
      "amount": 150.00,
      "is_payment": false
    }
  ],
  "cutoff_day": 20,
  "bank_name": "กสิกรไทย",
  "card_name": "Visa Platinum"
}

กฎ:
- วันที่ให้แปลงเป็น YYYY-MM-DD เสมอ
- ใบแจ้งยอดไทยใช้รูปแบบ dd/mm/yy หรือ dd/mm/yyyy (วันก่อน เดือนหลัง เสมอ)
  เช่น "17/01/26" = วันที่ 17 เดือน 01 (มกราคม) ปี 2026 → 2026-01-17
       "05/02/26" = วันที่ 5 เดือน 02 (กุมภาพันธ์) ปี 2026 → 2026-02-05
- ถ้าเห็นแค่ 2 หลักสุดท้ายของปี (เช่น 26) ให้ใช้ 2026
- ห้ามสลับวันกับเดือน — ตัวเลขแรก (ก่อน /) คือวัน ตัวเลขที่สอง คือเดือน
- amount เป็นตัวเลขล้วน ไม่มี comma
- is_payment = true สำหรับรายการชำระเงิน (Payment, ยอดชำระ, ยอดเรียกเก็บ)
- cutoff_day = วันที่ตัดรอบบิล (เช่น 15, 20, 25) ถ้าไม่เจอให้ใส่ null
- bank_name = ชื่อธนาคารผู้ออกบัตร เช่น "กสิกรไทย", "ไทยพาณิชย์", "กรุงไทย", "กรุงศรี", "ทหารไทยธนชาต", "ซิตี้แบงก์" ถ้าไม่เจอให้ใส่ null
- card_name = ชื่อประเภทบัตร เช่น "Visa Platinum", "World Mastercard", "JCB Precious" ถ้าไม่เจอให้ใส่ null
- ถ้าไม่มีรายการในภาพ ให้ส่ง {"transactions": [], "cutoff_day": null, "bank_name": null, "card_name": null}

สำคัญมาก — หน้าที่ต้องข้ามทั้งหมด (ส่ง {"transactions": [], "cutoff_day": null} กลับ):
- หน้า "วิธีการชำระเงิน" / "Methods of Payment"
- หน้า "ตัวอย่างการคำนวณดอกเบี้ย" / "Example of Interest Calculation"
- หน้าที่มีหัวข้อหรือข้อความ "ตัวอย่าง" / "Example" เด่นชัด ซึ่งรายการในนั้นเป็นเพียงการอธิบาย ไม่ใช่รายการจริง
- หน้าแบบฟอร์มการชำระเงิน (Pay-in Slip) ที่ไม่มีรายการค่าใช้จ่าย
- หน้าที่รายการส่วนใหญ่มีปี ค.ศ. ต่างจากปีหลัก (statement) เกิน 2 ปี → ข้ามทั้งหน้า
  (เช่น statement ปี 2026 แต่รายการในหน้านั้นมีปี 2020, 2021 = ตัวอย่างดอกเบี้ย → ไม่ใช่รายการจริง)
"""
    response = model.models.generate_content(
        model="gemini-2.0-flash",
        contents=[prompt, image],
    )
    text = response.text.strip()
    # Strip markdown fences if any
    for fence in ["```json", "```"]:
        if fence in text:
            text = text.split(fence)[1].split("```")[0].strip()
            break
    return json.loads(text)


def get_few_shot_examples(limit_per_cat: int = 3) -> list[dict]:
    """ดึงตัวอย่าง transaction จาก DB ที่มี category+subcategory แล้ว
    เลือก limit_per_cat รายการต่อ category เพื่อให้ครอบคลุม"""
    conn = get_db()
    rows = conn.execute("""
        SELECT description, category, subcategory
        FROM (
            SELECT description, category, subcategory,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY RANDOM()) AS rn
            FROM transactions
            WHERE category IS NOT NULL AND category != '' AND category != 'อื่นๆ'
        )
        WHERE rn <= ?
        ORDER BY category
    """, (limit_per_cat,)).fetchall()
    conn.close()
    return [{"description": r["description"], "category": r["category"],
             "subcategory": r["subcategory"]} for r in rows]


def categorize(model, descriptions: list, examples: list[dict] | None = None) -> list:
    cats = ", ".join(CATEGORIES)

    # สร้าง few-shot block จาก examples ที่มีอยู่ใน DB
    few_shot_block = ""
    if examples:
        few_shot_block = "\nตัวอย่างจากประวัติการใช้จ่ายของบัตรใบนี้ (ใช้เป็น reference):\n"
        for ex in examples:
            few_shot_block += f'- "{ex["description"]}" → {ex["category"]}\n'
        few_shot_block += "\n"

    prompt = f"""จัดหมวดหมู่รายการค่าใช้จ่ายบัตรเครดิตต่อไปนี้

หมวดที่ใช้ได้เท่านั้น: {cats}
{few_shot_block}
รายการ (JSON array):
{json.dumps(descriptions, ensure_ascii=False)}

ส่งออกเป็น JSON array ของ string เท่านั้น ลำดับต้องตรงกับ input
ห้ามมีข้อความอื่น ห้ามมี markdown

ตัวอย่าง output: ["ร้านสะดวกซื้อ", "Subscription/ดิจิทัล"]

แนวทางการจัด (ใช้เมื่อไม่มีตัวอย่างตรงๆ):
- CJ, 7-11, 7-ELEVEN, TMN 7-11 → ร้านสะดวกซื้อ
- Netflix, Spotify, Google, Apple, Anthropic, Claude, Microsoft, LINE, Adobe, ChatGPT, DYN, Moonshot → Subscription/ดิจิทัล
- Shopee, Lazada, Temu, 2C2P, LAZADA → ช้อปปิ้งออนไลน์
- EASYBILLS, Easy Pass, Expressway, BEM, GRABTAXI, GRAB TAXI → ทางด่วน/ขนส่ง
- WWW.GRAB.COM, GRABFOOD, GRAB FOOD, LINE MAN, LPTH*PF_LM, Restaurant, Food, Bistro, Bar, Cafe, Starbucks, Fast Food, ร้านอาหาร → อาหาร/เครื่องดื่ม
- Hospital, Clinic, HOSPITAL, CLINIC, DENTAL, Pharmacy, MED, โรงพยาบาล, คลินิก, ร้านขายยา → สุขภาพ
- TOPS, Lotus, Big C, Makro, Gourmet, Supermarket → ซุปเปอร์มาร์เก็ต
- AIS, TRUE, DTAC, TOT, Internet, Telephone → โทรศัพท์/อินเทอร์เน็ต
- Airline, Hotel, Travel, Agoda, Booking, TRAVEL, ASCENDTRAVEL → เดินทาง
- AIA, Insurance, IPAYAGT, QR-AIA, ประกัน → ประกัน
- B-QUIK, Firestone, Bridgestone, Goodyear, Autocare, ช่างยนต์, ซ่อมรถ, เปลี่ยนยาง, เปลี่ยนถ่ายน้ำมัน → บริการรถยนต์
- DECATHLON, CENTRAL, ROBINSON, THE MALL, IKEA, INDEX, POWER BUY, BANANA IT, ห้างสรรพสินค้า, ร้านค้า offline ทั่วไป → ช้อปปิ้ง
"""
    response = model.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt,
    )
    text = response.text.strip()
    for fence in ["```json", "```"]:
        if fence in text:
            text = text.split(fence)[1].split("```")[0].strip()
            break
    return json.loads(text)


def subcategorize(model, descriptions: list, categories: list,
                  examples: list[dict] | None = None) -> list:
    """จัดหมวดหมู่ย่อยตาม category หลัก - batch โดยกลุ่ม category"""
    # สร้าง result array เต็มด้วย None ก่อน
    results = [None] * len(descriptions)

    # จัดกลุ่มตาม category
    from collections import defaultdict
    groups = defaultdict(list)
    for i, (desc, cat) in enumerate(zip(descriptions, categories)):
        if cat in SUBCATEGORIES:
            groups[cat].append((i, desc))

    # index examples ตาม category เพื่อค้นหาเร็ว
    examples_by_cat: dict[str, list] = defaultdict(list)
    for ex in (examples or []):
        if ex.get("subcategory") and ex["category"] in SUBCATEGORIES:
            examples_by_cat[ex["category"]].append(ex)

    # ประมวลผลแต่ละ category เป็น batch
    for cat, items in groups.items():
        if not items:
            continue

        indices = [i for i, _ in items]
        descs = [d for _, d in items]
        subcats = ", ".join(SUBCATEGORIES[cat])

        # few-shot block เฉพาะ category นี้
        few_shot_block = ""
        if examples_by_cat[cat]:
            few_shot_block = "\nตัวอย่างจากประวัติ:\n"
            for ex in examples_by_cat[cat]:
                few_shot_block += f'- "{ex["description"]}" → {ex["subcategory"]}\n'
            few_shot_block += "\n"

        prompt = f"""จัดหมวดหมู่ย่อยสำหรับรายการค่าใช้จ่ายต่อไปนี้

หมวดหมู่หลัก: {cat}
หมวดหมู่ย่อยที่ใช้ได้เท่านั้น: {subcats}
{few_shot_block}
รายการ (JSON array):
{json.dumps(descs, ensure_ascii=False)}

ส่งออกเป็น JSON array ของ string เท่านั้น ลำดับต้องตรงกับ input
ถ้าไม่แน่ใจให้ใส่ null
ห้ามมีข้อความอื่น ห้ามมี markdown

ตัวอย่าง output: ["7-11", "คาเฟ่", "Food Delivery", null]
"""
        try:
            response = model.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
            )
            text = response.text.strip()
            for fence in ["```json", "```"]:
                if fence in text:
                    text = text.split(fence)[1].split("```")[0].strip()
                    break
            subcats_result = json.loads(text)

            # ใส่กลับใน results array
            for idx, subcat in zip(indices, subcats_result):
                if subcat and subcat in SUBCATEGORIES[cat]:
                    results[idx] = subcat
        except Exception as e:
            st.warning(f"จัดหมวดหมู่ย่อยไม่ได้สำหรับ {cat}: {e}")

    return results


def open_pdf(raw: bytes, password: str = "") -> fitz.Document:
    """เปิด PDF ทั้งแบบปกติและแบบมีรหัสผ่าน"""
    doc = fitz.open(stream=raw, filetype="pdf")
    if doc.needs_pass:
        if not password:
            raise ValueError("PDF_NEEDS_PASSWORD")
        ok = doc.authenticate(password)
        if not ok:
            raise ValueError("PDF_WRONG_PASSWORD")
    return doc


def pdf_to_images(raw: bytes, password: str = "") -> list:
    """แปลงทุกหน้าของ PDF เป็น PIL Image"""
    doc = open_pdf(raw, password)
    images = []
    for page in doc:
        pix = page.get_pixmap(dpi=150)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        images.append(img)
    return images


def process_uploaded_file(raw: bytes, filename: str, model, password: str = "") -> tuple:
    from collections import Counter
    name = filename.lower()

    if name.endswith(".pdf"):
        images = pdf_to_images(raw, password)
    else:
        # JPG, JPEG, PNG — ไม่มีรหัสผ่าน
        images = [Image.open(io.BytesIO(raw))]

    all_txns = []
    cutoff_days = []  # เก็บ cutoff_day จากทุกหน้า
    bank_names = []   # เก็บชื่อธนาคารจากทุกหน้า
    card_names = []   # เก็บชื่อบัตรจากทุกหน้า
    for img in images:
        try:
            result = extract_from_image(model, img)
            all_txns.extend(result.get("transactions", []))
            if result.get("cutoff_day"):
                cutoff_days.append(result["cutoff_day"])
            if result.get("bank_name"):
                bank_names.append(result["bank_name"])
            if result.get("card_name"):
                card_names.append(result["card_name"])
        except Exception as e:
            st.warning(f"อ่านบางหน้าไม่ได้: {e}")

    # เลือก cutoff_day ที่เจอบ่อยที่สุด (ถ้ามี)
    cutoff_day = None
    if cutoff_days:
        cutoff_day = Counter(cutoff_days).most_common(1)[0][0]

    # Remove payment rows
    expenses = [t for t in all_txns if not t.get("is_payment", False)]

    # กรองรายการที่มีวันที่เก่าเกินจริง (รายการจาก "หน้าตัวอย่าง" ที่หลุดมา)
    # อนุญาตย้อนหลังได้สูงสุด 3 ปีจากปีปัจจุบัน
    current_year = datetime.now().year

    def _is_recent_year(date_str: str) -> bool:
        if not date_str:
            return True
        try:
            y = int(date_str[:4])
            return (current_year - y) <= 3
        except Exception:
            return True  # ถ้า parse ไม่ได้ ให้เก็บไว้ก่อน

    before_filter = len(expenses)
    expenses = [t for t in expenses if _is_recent_year(t.get("trans_date", ""))]
    filtered_out = before_filter - len(expenses)
    if filtered_out > 0:
        st.info(f"ℹ️ กรอง {filtered_out} รายการที่มีวันที่เก่าเกิน 3 ปีออก (อาจเป็นรายการตัวอย่างในเอกสาร)")

    # โหลด few-shot examples จาก DB (ถ้ายังไม่มีข้อมูลเก่าก็ได้ [] กลับมา — ทำงานได้ปกติ)
    examples = get_few_shot_examples(limit_per_cat=3)

    # Categorize
    if expenses:
        descs = [t["description"] for t in expenses]
        try:
            cats = categorize(model, descs, examples=examples)
            for t, c in zip(expenses, cats):
                t["category"] = c if c in CATEGORIES else "อื่นๆ"
        except Exception as e:
            st.warning(f"จัดหมวดหมู่ไม่ได้: {e}")
            for t in expenses:
                t["category"] = "อื่นๆ"

        # Subcategorize
        try:
            categories = [t["category"] for t in expenses]
            subcats = subcategorize(model, descs, categories, examples=examples)
            for t, sc in zip(expenses, subcats):
                t["subcategory"] = sc
        except Exception as e:
            st.warning(f"จัดหมวดหมู่ย่อยไม่ได้: {e}")
            for t in expenses:
                t["subcategory"] = None

    # สร้าง suggested_bank จากชื่อธนาคาร/บัตรที่อ่านได้จาก statement
    suggested_bank = None
    if bank_names or card_names:
        bank_count = Counter(b for b in bank_names if b)
        card_count = Counter(c for c in card_names if c)
        top_bank = bank_count.most_common(1)[0][0] if bank_count else ""
        top_card = card_count.most_common(1)[0][0] if card_count else ""
        if top_bank and top_card:
            suggested_bank = f"{top_bank} {top_card}"
        else:
            suggested_bank = top_bank or top_card or None

    return expenses, cutoff_day, suggested_bank

# ─── Pages ────────────────────────────────────────────────────────────────────

def page_import():
    st.header("นำเข้า Statement")

    # ── แสดง banner เมื่อ save สำเร็จ (ค้างไว้ 1 render แล้วล้างทิ้ง) ──────────
    if st.session_state.get("_import_success"):
        msg = st.session_state.pop("_import_success")
        st.success(msg)
        st.balloons()

    # ── Auto-fill ชื่อธนาคารที่ระบบตรวจพบจาก statement (ถ้ายังไม่ได้กรอก) ──────
    if st.session_state.get("_detected_bank"):
        if not st.session_state.get("_bank_input", "").strip():
            st.session_state["_bank_input"] = st.session_state["_detected_bank"]
        del st.session_state["_detected_bank"]

    # ── Previous bank quick-select ─────────────────────────────────────────────
    prev_banks = get_previous_banks()
    if prev_banks:
        st.caption("💡 เลือกจากที่เคยนำเข้า:")
        btn_cols = st.columns(min(len(prev_banks), 5))
        for i, b in enumerate(prev_banks[:5]):
            if btn_cols[i].button(b, key=f"_prev_bank_{i}", use_container_width=True):
                st.session_state["_bank_input"] = b
                st.rerun()

    bank = st.text_input(
        "ชื่อธนาคาร / บัตร",
        placeholder="เช่น KTB Visa, SCB Platinum, KBANK",
        help="ใส่ชื่อเพื่อแยกแยะในภายหลัง — ระบบจะพยายามตรวจจับอัตโนมัติหลังอ่าน statement",
        key="_bank_input",
    )

    # key เปลี่ยนทุกครั้งที่ save → บังคับ file_uploader reset เป็นว่างเปล่า
    upload_key = f"uploader_{st.session_state.get('_upload_rev', 0)}"
    uploaded = st.file_uploader(
        "อัปโหลด Statement (รองรับ PDF, JPG, JPEG, PNG — หลายไฟล์พร้อมกันได้)",
        type=["pdf", "jpg", "jpeg", "png"],
        accept_multiple_files=True,
        key=upload_key,
    )

    # ── PDF Password Section ─────────────────────────────────────────────────
    has_pdf = uploaded and any(f.name.lower().endswith(".pdf") for f in uploaded)
    pdf_password = ""
    if has_pdf:
        with st.expander("PDF มีรหัสผ่านหรือไม่?", expanded=False):
            st.markdown("""
            ธนาคารหลายแห่งล็อก PDF ด้วยข้อมูลส่วนตัว เช่น:
            - **KTB / SCB / KBANK** — วันเดือนปีเกิด เช่น `01Jan1990`
            - **TTB** — เลข 4 ตัวท้ายบัตร หรือวันเกิด เช่น `0101`
            - บางธนาคารอาจใช้เลขบัตรประชาชน หรือรูปแบบอื่น

            รหัสผ่านจะใช้เฉพาะตอนเปิด PDF บนเครื่องคุณเท่านั้น ไม่ถูกเก็บหรือส่งออก
            """)
            use_password = st.checkbox("PDF นี้มีรหัสผ่าน")
            if use_password:
                pdf_password = st.text_input(
                    "รหัสผ่าน PDF",
                    type="password",
                    placeholder="เช่น 01Jan1990",
                    help="ใช้เฉพาะเปิด PDF — ไม่ถูกบันทึก",
                )

    if uploaded:
        if st.button("วิเคราะห์", type="primary", use_container_width=True):
            model = get_model()
            all_txns = []
            filenames = []
            file_hashes = []
            errors = []
            cutoff_days = []
            suggested_banks = []
            progress = st.progress(0, text="กำลังอ่านไฟล์...")

            for i, f in enumerate(uploaded):
                with st.spinner(f"กำลังอ่าน: {f.name}"):
                    try:
                        raw = f.read()
                        file_hash = compute_file_hash(raw)

                        # ตรวจสอบไฟล์ซ้ำด้วย SHA-256 hash
                        dup = find_duplicate_statement(file_hash)
                        if dup:
                            errors.append(
                                f"**{f.name}** — ⚠️ เคยนำเข้าแล้ว "
                                f"(ธนาคาร: {dup['bank']}, งวด: {dup['period']}, "
                                f"นำเข้าเมื่อ: {dup['imported_at'][:10]}) — "
                                "หากต้องการนำเข้าใหม่ กรุณาลบรายการเดิมในหน้า 'ประวัติการนำเข้า' ก่อน"
                            )
                        else:
                            password = pdf_password if f.name.lower().endswith(".pdf") else ""
                            txns, cutoff_day, suggested_bank = process_uploaded_file(raw, f.name, model, password)
                            for t in txns:
                                t["bank"] = bank
                            all_txns.extend(txns)
                            filenames.append(f.name)
                            file_hashes.append(file_hash)
                            if cutoff_day:
                                cutoff_days.append(cutoff_day)
                            if suggested_bank:
                                suggested_banks.append(suggested_bank)
                    except ValueError as e:
                        if "PDF_NEEDS_PASSWORD" in str(e):
                            errors.append(f"**{f.name}** — PDF นี้มีรหัสผ่าน กรุณาเปิด 'PDF มีรหัสผ่าน' แล้วใส่รหัส")
                        elif "PDF_WRONG_PASSWORD" in str(e):
                            errors.append(f"**{f.name}** — รหัสผ่านไม่ถูกต้อง")
                        else:
                            errors.append(f"**{f.name}** — {e}")
                    except Exception as e:
                        errors.append(f"**{f.name}** — เกิดข้อผิดพลาด: {e}")
                progress.progress((i + 1) / len(uploaded), text=f"อ่านแล้ว {i+1}/{len(uploaded)} ไฟล์")

            for err in errors:
                st.error(err)

            # ถ้าระบุชื่อธนาคารยังไม่ได้ → auto-suggest จากที่อ่านได้
            if suggested_banks and not st.session_state.get("_bank_input", "").strip():
                from collections import Counter as _Counter
                best_bank = _Counter(suggested_banks).most_common(1)[0][0]
                st.session_state["_detected_bank"] = best_bank

            if all_txns:
                # เลือก cutoff_day ที่เจอบ่อยที่สุด (ถ้ามี)
                final_cutoff = None
                if cutoff_days:
                    from collections import Counter
                    final_cutoff = Counter(cutoff_days).most_common(1)[0][0]

                st.session_state["pending"] = all_txns
                st.session_state["pending_bank"] = bank
                st.session_state["pending_files"] = filenames
                st.session_state["pending_cutoff_day"] = final_cutoff
                st.session_state["pending_file_hashes"] = file_hashes

                success_msg = f"พบ {len(all_txns)} รายการ"
                success_msg += " — กรุณาตรวจสอบด้านล่างก่อนบันทึก"
                st.success(success_msg)
                st.rerun()
            elif not errors:
                st.error("ไม่พบรายการค่าใช้จ่ายในไฟล์ที่อัปโหลด")

    # ── Review pending ──────────────────────────────────────────────────────
    if st.session_state.get("pending"):
        txns = st.session_state["pending"]
        df_pending = pd.DataFrame(txns)[["trans_date", "description", "amount", "category", "subcategory"]]

        st.subheader("ตรวจสอบรายการ (แก้ไขได้ก่อนบันทึก)")
        edited = st.data_editor(
            df_pending,
            column_config={
                "trans_date":   st.column_config.TextColumn("วันที่ใช้บัตร"),
                "description":  st.column_config.TextColumn("รายการ", width="large"),
                "amount":       st.column_config.NumberColumn("จำนวนเงิน (บาท)", format="%.2f"),
                "category":     st.column_config.SelectboxColumn("หมวดหมู่", options=CATEGORIES),
                "subcategory":  st.column_config.TextColumn("หมวดหมู่ย่อย", help="แก้ไขได้ตามหมวดหมู่หลัก หรือเว้นว่างได้"),
            },
            use_container_width=True,
            num_rows="dynamic",
            key="editor",
        )

        total = edited["amount"].sum()
        st.metric("รวมทั้งหมด", f"{total:,.2f} บาท")

        def _do_save(bank: str, final_rows: list):
            """บันทึกรายการและล้าง session state ที่เกี่ยวข้อง"""
            save_transactions(
                bank,
                st.session_state["pending_files"],
                final_rows,
                st.session_state.get("pending_cutoff_day"),
                st.session_state.get("pending_file_hashes"),
            )
            n = len(final_rows)
            for key in ["pending", "pending_bank", "pending_files",
                        "pending_cutoff_day", "pending_file_hashes",
                        "_dup_warnings", "_dup_pending_final", "_dup_pending_bank"]:
                st.session_state.pop(key, None)
            st.session_state["_import_success"] = (
                f"✅ นำเข้าสำเร็จ! บันทึก {n} รายการ ({bank}) เรียบร้อยแล้ว"
            )
            st.session_state["_upload_rev"] = st.session_state.get("_upload_rev", 0) + 1
            st.rerun()

        # ── แสดง duplicate warning + ปุ่มยืนยัน (ถ้ามี) ──────────────────────
        if st.session_state.get("_dup_warnings"):
            warnings = st.session_state["_dup_warnings"]
            st.warning("⚠️ **พบข้อมูลที่อาจซ้ำกับที่นำเข้าแล้ว** — กรุณาตรวจสอบก่อนบันทึก")
            with st.expander("📋 รายละเอียดการตรวจพบความซ้ำ", expanded=True):
                for w in warnings:
                    st.markdown(w)
            confirm = st.checkbox("ฉันเข้าใจและต้องการบันทึกซ้ำ (มีเหตุผลที่ต่างจากรายการเดิม)")
            col_force, col_cancel_dup = st.columns(2)
            with col_force:
                if st.button("บันทึกซ้ำ", type="primary",
                             disabled=not confirm, use_container_width=True):
                    _do_save(
                        st.session_state["_dup_pending_bank"],
                        st.session_state["_dup_pending_final"],
                    )
            with col_cancel_dup:
                if st.button("ยกเลิก / แก้ไข", use_container_width=True):
                    for key in ["_dup_warnings", "_dup_pending_final", "_dup_pending_bank"]:
                        st.session_state.pop(key, None)
                    st.rerun()
        else:
            c1, c2 = st.columns(2)
            with c1:
                if st.button("บันทึก", type="primary", use_container_width=True):
                    all_rows = edited.to_dict("records")

                    current_bank = st.session_state.get("_bank_input", "").strip()
                    if not current_bank:
                        st.warning("กรุณาระบุชื่อธนาคาร / บัตร ก่อนบันทึก")
                        st.stop()

                    # กรองบรรทัดว่างออก
                    final = [
                        row for row in all_rows
                        if str(row.get("description") or "").strip()
                        and row.get("amount") is not None
                        and not (isinstance(row.get("amount"), float) and pd.isna(row["amount"]))
                    ]

                    if not final:
                        st.warning("ไม่มีรายการที่จะบันทึก กรุณาตรวจสอบรายการในตาราง")
                    else:
                        # Merge back posting_date from original
                        orig_map = {t["description"]: t for t in txns}
                        for row in final:
                            orig = orig_map.get(row["description"], {})
                            row["posting_date"] = orig.get("posting_date", "")

                        # ── ตรวจสอบซ้ำ 2 ระดับ ───────────────────────────────
                        total_amount = sum(r["amount"] for r in final
                                          if (r.get("amount") or 0) > 0)
                        dates = [r["trans_date"] for r in final if r.get("trans_date")]
                        est_period = max(dates)[:7] if dates else ""

                        dup_warnings = []

                        # Level 1: period + amount-sum (±5%) — ไม่ filter ด้วย bank
                        if est_period:
                            similar = find_similar_statement(current_bank, est_period, total_amount)
                            for s in similar:
                                diff_pct = s.get("diff_ratio", 0) * 100
                                bank_note = "✅ ธนาคารเดียวกัน" if s.get("bank_match") else f"❓ ธนาคาร: **{s['bank']}**"
                                dup_warnings.append(
                                    f"⚠️ **[ระดับ Statement]** พบ statement ที่คล้ายกัน: "
                                    f"งวด **{s['period']}** {bank_note} "
                                    f"(นำเข้าเมื่อ {s['imported_at'][:10]}) — "
                                    f"ยอดรวมต่างกัน **{diff_pct:.1f}%**"
                                )

                        # Level 2: transaction-level overlap (≥50% soft match = date+amount)
                        overlap = find_overlapping_transactions(final)
                        pos_total = overlap.get("positive_total", overlap.get("total", 1))
                        if overlap["overlap_ratio"] >= 0.5:
                            exact = overlap.get("exact_count", 0)
                            soft  = overlap.get("soft_count", 0)
                            dup_warnings.append(
                                f"⚠️ **[ระดับรายการ]** พบรายการที่ตรงกับ DB แล้ว: "
                                f"ตรงทั้ง desc+amount **{exact}/{pos_total}** รายการ | "
                                f"ตรงเฉพาะวัน+amount **{soft}/{pos_total}** รายการ "
                                f"(**{overlap['overlap_ratio']*100:.0f}%**)"
                            )


                        if dup_warnings:
                            st.session_state["_dup_warnings"] = dup_warnings
                            st.session_state["_dup_pending_final"] = final
                            st.session_state["_dup_pending_bank"] = current_bank
                            st.rerun()
                        else:
                            _do_save(current_bank, final)

            with c2:
                if st.button("ยกเลิก", use_container_width=True):
                    for key in ["pending", "pending_bank", "pending_files"]:
                        st.session_state.pop(key, None)
                    st.rerun()



def page_dashboard():
    st.header("Dashboard")

    period_options = {
        "current_month": "เดือนนี้",
        "last_month": "เดือนที่แล้ว",
        "3_months": "3 เดือนล่าสุด",
        "6_months": "6 เดือนล่าสุด",
        "all": "ทั้งหมด"
    }
    period = st.selectbox(
        "ช่วงเวลา",
        options=list(period_options.keys()),
        format_func=lambda x: period_options[x],
        index=1,  # default = เดือนที่แล้ว
    )

    period_label = period_options[period]

    # toggle: group by trans_date month vs billing period from statement
    group_mode = st.radio(
        "แสดงตาม",
        ["เดือนที่ใช้จ่ายจริง", "งวดบิล"],
        horizontal=True,
        help=(
            "'เดือนที่ใช้จ่ายจริง' = นับตามวันที่รูดบัตร  |  "
            "'งวดบิล' = นับตามงวดใบแจ้งหนี้ของธนาคาร "
            "(เช่น KBank ก.พ. + KTB ก.พ. = งวด ก.พ. เดียวกัน ไม่ต้องตั้งวันตัดรอบ)"
        ),
    )

    # โหลดข้อมูลทั้งหมด แล้ว filter ทีหลังตามโหมดที่เลือก
    df = load_transactions("all")

    if df.empty:
        st.info("ยังไม่มีข้อมูล — กรุณานำเข้า Statement ในเมนู 'นำเข้า Statement' ก่อน")
        return

    if group_mode == "งวดบิล":
        # ใช้ period ของ statement โดยตรง (YYYY-MM) — ไม่ต้องคำนวณวันตัดรอบ
        # ทุก statement มี period อยู่แล้ว → KBank ก.พ. + KTB ก.พ. = period "2026-02" เดียวกัน
        if period == "current_month":
            target = pd.Timestamp.now().strftime("%Y-%m")
            df = df[df["period"] == target]
        elif period == "last_month":
            target = (pd.Timestamp.now() - pd.DateOffset(months=1)).strftime("%Y-%m")
            df = df[df["period"] == target]
        elif period in ["3_months", "6_months"]:
            n = 3 if period == "3_months" else 6
            all_periods = sorted(df["period"].dropna().unique(), reverse=True)
            recent = all_periods[:n]
            df = df[df["period"].isin(recent)]
        # else "all": no filter

        if df.empty:
            st.warning(f"ไม่มีข้อมูลงวดบิล {period_label}")
            return

        df["month_sort"]  = df["period"]
        df["month_label"] = pd.to_datetime(df["period"] + "-01").dt.strftime("งวด %b %Y")

    else:
        # เดือนที่ใช้จ่ายจริง: filter โดย trans_date
        if period == "current_month":
            now = pd.Timestamp.now()
            df = df[(df["trans_date"].dt.year == now.year) &
                    (df["trans_date"].dt.month == now.month)]
        elif period == "last_month":
            lm = pd.Timestamp.now() - pd.DateOffset(months=1)
            df = df[(df["trans_date"].dt.year == lm.year) &
                    (df["trans_date"].dt.month == lm.month)]
        elif period in ["3_months", "6_months"]:
            n = 3 if period == "3_months" else 6
            all_months = sorted(df["trans_date"].dt.to_period("M").dropna().unique(), reverse=True)
            recent_months = all_months[:n]
            df = df[df["trans_date"].dt.to_period("M").isin(recent_months)]

        if df.empty:
            st.warning(f"ไม่มีข้อมูลในช่วง {period_label}")
            return

        df["month_sort"]  = df["trans_date"].dt.strftime("%Y-%m")
        df["month_label"] = df["trans_date"].dt.strftime("%b %Y")

    # sort mapping: month_label → month_sort (for ordering)
    label_order = (
        df[["month_sort", "month_label"]]
        .drop_duplicates()
        .sort_values("month_sort")["month_label"]
        .tolist()
    )

    # ── KPI ─────────────────────────────────────────────────────────────────
    total = df["amount"].sum()
    num_months = df["month_sort"].nunique()
    avg_monthly = df.groupby("month_sort")["amount"].sum().mean()
    top_cat = df.groupby("category")["amount"].sum().idxmax()

    c1, c2, c3 = st.columns(3)
    c1.metric(f"รวม ({period_label})", f"{total:,.0f} บาท")
    if num_months > 1:
        c2.metric(f"เฉลี่ย/เดือน ({num_months} เดือน)", f"{avg_monthly:,.0f} บาท")
    else:
        c2.metric("เฉลี่ย/เดือน", f"{avg_monthly:,.0f} บาท")
    c3.metric("ใช้มากที่สุด", top_cat)

    st.divider()

    # ── Charts ──────────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("สัดส่วนตามหมวดหมู่")

        # สร้าง path สำหรับ sunburst: category > subcategory
        # กรองเฉพาะรายการที่ยอดเงิน > 0 เพื่อไม่ให้กราฟ Sunburst มีปัญหาติดลบ
        df_chart = df[df["amount"] > 0].copy()
        df_chart["display_path"] = df_chart.apply(
            lambda row: f"{row['category']} > {row['subcategory']}"
                if pd.notna(row['subcategory']) and str(row['subcategory']).strip() != ""
                else row['category'],
            axis=1
        )

        # Group by path และรวม amount
        path_df = df_chart.groupby("display_path")["amount"].sum().reset_index()

        # สร้าง go.Sunburst nodes แบบ explicit
        # รองรับกรณี category มีทั้งรายการที่มี subcategory และไม่มี (px.sunburst ทำไม่ได้)
        cat_totals: dict = {}   # cat → ยอดรวมทั้งหมด
        cat_subs: dict = {}     # cat → {sub → amount}

        for _, row in path_df.iterrows():
            parts = row["display_path"].split(" > ", 1)
            cat = parts[0]
            amt = row["amount"]
            cat_totals[cat] = cat_totals.get(cat, 0) + amt
            if len(parts) == 2:
                if cat not in cat_subs:
                    cat_subs[cat] = {}
                cat_subs[cat][parts[1]] = cat_subs[cat].get(parts[1], 0) + amt

        ids, labels, parents, values = [], [], [], []

        # Category nodes (ชั้นแรก — รากของแต่ละหมวด)
        for cat, total in cat_totals.items():
            ids.append(cat)
            labels.append(cat)
            parents.append("")
            values.append(total)

        # Subcategory nodes (ชั้นที่สอง)
        for cat, subs in cat_subs.items():
            sub_total = sum(subs.values())
            remainder = cat_totals[cat] - sub_total

            for sub, amt in subs.items():
                ids.append(f"{cat}/{sub}")
                labels.append(sub)
                parents.append(cat)
                values.append(amt)

            # รายการที่ไม่มี subcategory → รวมเป็น "อื่นๆ" เพื่อไม่ให้มีช่องว่างขาว
            if remainder > 0.01:
                ids.append(f"{cat}/_other")
                labels.append("อื่นๆ")
                parents.append(cat)
                values.append(remainder)

        fig = go.Figure(go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            textinfo="label+value+percent parent",
            texttemplate="%{label}<br>฿%{value:,.0f}<br>%{percentParent:.1%}",
            insidetextorientation="radial",
            hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<br>%{percentParent:.1%}<extra></extra>",
        ))
        fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("ค่าใช้จ่ายรายเดือน")
        month_df = df.groupby("month_label")["amount"].sum().reset_index()
        fig = px.bar(month_df, x="month_label", y="amount",
                     text_auto=".0f",
                     category_orders={"month_label": label_order},
                     color_discrete_sequence=["#4C78A8"])
        fig.update_layout(height=380, xaxis_title="", yaxis_title="บาท",
                          xaxis={"type": "category", "categoryorder": "array",
                                 "categoryarray": label_order},
                          margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("หมวดหมู่แยกรายเดือน")
    pivot = df.groupby(["month_label", "category"])["amount"].sum().reset_index()
    fig = px.bar(pivot, x="month_label", y="amount", color="category",
                 barmode="stack",
                 category_orders={"month_label": label_order},
                 color_discrete_sequence=px.colors.qualitative.Set3)
    fig.update_layout(height=400, xaxis_title="", yaxis_title="บาท",
                      xaxis={"type": "category", "categoryorder": "array",
                             "categoryarray": label_order},
                      legend_title="หมวดหมู่")
    st.plotly_chart(fig, use_container_width=True)

    # ── Table ───────────────────────────────────────────────────────────────
    st.subheader("รายการทั้งหมด")

    # Filter by category
    selected_cats = st.multiselect(
        "กรองตามหมวดหมู่",
        options=CATEGORIES,
        default=[],
        placeholder="ทุกหมวดหมู่",
    )
    show_df = df.copy()
    if selected_cats:
        show_df = show_df[show_df["category"].isin(selected_cats)]

    # ── Summary row ──────────────────────────────────────────────────────────
    filtered_total = show_df["amount"].sum()
    filtered_count = len(show_df)
    label = f"กรอง: {', '.join(selected_cats)}" if selected_cats else "ทุกหมวดหมู่"
    m1, m2 = st.columns(2)
    m1.metric(f"รวม ({label})", f"฿{filtered_total:,.2f}")
    m2.metric("จำนวนรายการ", f"{filtered_count} รายการ")

    show_df = show_df.sort_values("trans_date", ascending=False)
    show_df["trans_date"] = show_df["trans_date"].dt.strftime("%d/%m/%Y")

    st.dataframe(
        show_df[["trans_date", "bank", "description", "amount", "category", "subcategory"]],
        column_config={
            "trans_date":   st.column_config.TextColumn("วันที่"),
            "bank":         st.column_config.TextColumn("ธนาคาร"),
            "description":  st.column_config.TextColumn("รายการ", width="large"),
            "amount":       st.column_config.NumberColumn("จำนวนเงิน", format="฿%.2f"),
            "category":     st.column_config.TextColumn("หมวดหมู่"),
            "subcategory":  st.column_config.TextColumn("หมวดหมู่ย่อย"),
        },
        use_container_width=True,
        hide_index=True,
    )

    # Download CSV
    csv = show_df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        "ดาวน์โหลด CSV",
        data=csv,
        file_name=f"credit_card_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )


def page_history():
    st.header("ประวัติการนำเข้า")
    stmts = load_statements()
    if stmts.empty:
        st.info("ยังไม่มีข้อมูล")
        return

    for _, row in stmts.iterrows():
        with st.expander(f"{row['period']} — {row['bank']} ({row['tx_count']} รายการ)"):
            st.write(f"**ไฟล์:** {row['filename']}")
            st.write(f"**นำเข้าเมื่อ:** {row['imported_at'][:19]}")
            if st.button("ลบชุดข้อมูลนี้", key=f"del_{row['id']}", type="secondary"):
                delete_statement(row["id"])
                st.success("ลบแล้ว")
                st.rerun()


def page_settings():
    st.header("ตั้งค่า")

    st.markdown("""
    **วิธีขอ Gemini API Key (ฟรี):**
    1. ไปที่ [aistudio.google.com](https://aistudio.google.com)
    2. Sign in ด้วย Google account
    3. คลิก **Get API Key** → **Create API key**
    4. Copy key มาใส่ด้านล่าง หรือใส่ใน `.env` ไฟล์
    """)

    # แสดงว่า key มาจากไหน
    env_key = os.environ.get("GEMINI_API_KEY", "")
    if env_key:
        st.success("พบ GEMINI_API_KEY จาก environment variable (.env) — ไม่จำเป็นต้องกรอกด้านล่าง")
        source_label = "มาจาก: .env / environment variable"
    elif CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text())
            if cfg.get("api_key"):
                source_label = "มาจาก: ตั้งค่าผ่าน UI (เก็บใน ~/.credit_analyzer/config.json)"
            else:
                source_label = "ยังไม่ได้ตั้งค่า"
        except Exception:
            source_label = "ยังไม่ได้ตั้งค่า"
    else:
        source_label = "ยังไม่ได้ตั้งค่า"

    st.caption(f"สถานะ API Key: {source_label}")

    st.markdown("""
    **2 วิธีในการตั้งค่า API Key:**

    **วิธีที่ 1 — ผ่าน UI (แนะนำสำหรับใช้ส่วนตัว)**
    กรอก key ในช่องด้านล่าง แล้วกด บันทึก

    **วิธีที่ 2 — ผ่าน .env file (สำหรับคนที่ถนัด)**
    สร้างไฟล์ `.env` ในโฟลเดอร์โปรแกรม แล้วใส่:
    ```
    GEMINI_API_KEY=AIza...
    ```
    """)

    current_key = st.session_state.get("api_key", "")
    new_key = st.text_input(
        "Gemini API Key (ตั้งค่าผ่าน UI)",
        value=current_key if not env_key else "",
        type="password",
        placeholder="AIza...",
        disabled=bool(env_key),
        help="ถ้าใช้ .env ไม่ต้องกรอกที่นี่",
    )

    if not env_key:
        if st.button("บันทึก API Key", type="primary"):
            st.session_state["api_key"] = new_key
            # อ่าน config เดิมก่อน (ถ้ามี) เพื่อเก็บค่าอื่นๆ
            config = {}
            if CONFIG_PATH.exists():
                try:
                    config = json.loads(CONFIG_PATH.read_text())
                except Exception:
                    pass
            config["api_key"] = new_key
            CONFIG_PATH.write_text(json.dumps(config))
            st.success("บันทึกแล้ว — key เก็บใน config.json บนเครื่องคุณเท่านั้น")

    st.divider()
    st.subheader("สถิติข้อมูล")
    conn = get_db()
    n_stmt = conn.execute("SELECT COUNT(*) FROM statements").fetchone()[0]
    n_tx = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    conn.close()
    c1, c2 = st.columns(2)
    c1.metric("Statement ที่นำเข้า", n_stmt)
    c2.metric("รายการทั้งหมด", n_tx)

    st.divider()
    st.caption(f"ฐานข้อมูลอยู่ที่: `{DB_PATH}`")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="วิเคราะห์บัตรเครดิต",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    init_db()

    # Load API key — priority: session_state (UI) > GEMINI_API_KEY env var > config.json
    if "api_key" not in st.session_state:
        key = os.environ.get("GEMINI_API_KEY", "")
        if not key and CONFIG_PATH.exists():
            try:
                cfg = json.loads(CONFIG_PATH.read_text())
                key = cfg.get("api_key", "")
            except Exception:
                pass
        st.session_state["api_key"] = key

    # Sidebar
    st.sidebar.title("วิเคราะห์บัตรเครดิต")

    if not st.session_state.get("api_key"):
        st.sidebar.warning("ยังไม่ได้ตั้งค่า API Key")

    page = st.sidebar.radio(
        "เมนู",
        ["Dashboard", "นำเข้า Statement", "ประวัติการนำเข้า", "ตั้งค่า"],
    )

    if page == "Dashboard":
        page_dashboard()
    elif page == "นำเข้า Statement":
        page_import()
    elif page == "ประวัติการนำเข้า":
        page_history()
    elif page == "ตั้งค่า":
        page_settings()


if __name__ == "__main__":
    main()