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
from PIL import Image
import fitz  # PyMuPDF
from dotenv import load_dotenv

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
    "โทรศัพท์/อินเทอร์เน็ต": ["AIS", "True", "DTAC", "3BB/Fiber"],
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
            cutoff_day  INTEGER
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
    if "subcategory" not in tx_cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN subcategory TEXT")

    conn.commit()
    conn.close()


def save_transactions(bank: str, filenames: list, transactions: list, cutoff_day: int = None):
    conn = get_db()
    try:
        dates = [t["trans_date"] for t in transactions if t.get("trans_date")]
        period = max(dates)[:7] if dates else datetime.now().strftime("%Y-%m")

        cur = conn.execute(
            "INSERT INTO statements (filename, bank, period, imported_at, tx_count, cutoff_day) VALUES (?,?,?,?,?,?)",
            (", ".join(filenames), bank, period, datetime.now().isoformat(), len(transactions), cutoff_day),
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
  "cutoff_day": 20
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
- ถ้าไม่มีรายการในภาพ ให้ส่ง {"transactions": [], "cutoff_day": null}

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


def process_uploaded_file(uploaded_file, model, password: str = "") -> tuple:
    raw = uploaded_file.read()
    name = uploaded_file.name.lower()

    if name.endswith(".pdf"):
        images = pdf_to_images(raw, password)
    else:
        # JPG, JPEG, PNG — ไม่มีรหัสผ่าน
        images = [Image.open(io.BytesIO(raw))]

    all_txns = []
    cutoff_days = []  # เก็บ cutoff_day จากทุกหน้า
    for img in images:
        try:
            result = extract_from_image(model, img)
            all_txns.extend(result.get("transactions", []))
            if result.get("cutoff_day"):
                cutoff_days.append(result["cutoff_day"])
        except Exception as e:
            st.warning(f"อ่านบางหน้าไม่ได้: {e}")

    # เลือก cutoff_day ที่เจอบ่อยที่สุด (ถ้ามี)
    cutoff_day = None
    if cutoff_days:
        from collections import Counter
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

    return expenses, cutoff_day

# ─── Pages ────────────────────────────────────────────────────────────────────

def page_import():
    st.header("นำเข้า Statement")

    bank = st.text_input(
        "ชื่อธนาคาร / บัตร",
        placeholder="เช่น KTB Visa, SCB Platinum, KBANK",
        help="ใส่ชื่อเพื่อแยกแยะในภายหลัง",
    )

    uploaded = st.file_uploader(
        "อัปโหลด Statement (รองรับ PDF, JPG, JPEG, PNG — หลายไฟล์พร้อมกันได้)",
        type=["pdf", "jpg", "jpeg", "png"],
        accept_multiple_files=True,
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

    if uploaded and bank:
        if st.button("วิเคราะห์", type="primary", use_container_width=True):
            model = get_model()
            all_txns = []
            filenames = []
            errors = []
            cutoff_days = []
            progress = st.progress(0, text="กำลังอ่านไฟล์...")

            for i, f in enumerate(uploaded):
                with st.spinner(f"กำลังอ่าน: {f.name}"):
                    try:
                        password = pdf_password if f.name.lower().endswith(".pdf") else ""
                        txns, cutoff_day = process_uploaded_file(f, model, password)
                        for t in txns:
                            t["bank"] = bank
                        all_txns.extend(txns)
                        filenames.append(f.name)
                        if cutoff_day:
                            cutoff_days.append(cutoff_day)
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

                success_msg = f"พบ {len(all_txns)} รายการ"
                if final_cutoff:
                    success_msg += f" | วันตัดรอบ: {final_cutoff}"
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

        c1, c2 = st.columns(2)
        with c1:
            if st.button("บันทึก", type="primary", use_container_width=True):
                all_rows = edited.to_dict("records")

                # กรองบรรทัดว่างออก (เกิดจาก data_editor เพิ่มแถวว่าง หรือผู้ใช้ลบเนื้อหาในแถว)
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

                    save_transactions(
                        st.session_state["pending_bank"],
                        st.session_state["pending_files"],
                        final,
                        st.session_state.get("pending_cutoff_day"),
                    )
                    for key in ["pending", "pending_bank", "pending_files", "pending_cutoff_day"]:
                        st.session_state.pop(key, None)
                    st.success(f"บันทึกเรียบร้อย! {len(final)} รายการ")
                    st.rerun()

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
        index=0,  # default = เดือนนี้
    )

    period_label = period_options[period]

    # toggle: group by trans_date month vs billing cycle (ย้ายขึ้นมาก่อน load)
    group_mode = st.radio(
        "แสดงตาม",
        ["เดือนที่ใช้จ่ายจริง", "รอบบิล (ตามวันตัดรอบ)"],
        horizontal=True,
        help="'เดือนที่ใช้จ่ายจริง' = นับตามวันที่สวایپบัตร | 'รอบบิล' = นับตามรอบที่ธนาคารเรียกเก็บ",
    )

    # โหลดค่า cutoff_day จาก config
    default_cutoff = 20
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text())
            default_cutoff = cfg.get("cutoff_day", 20)
        except Exception:
            pass

    cutoff_day = default_cutoff
    if group_mode == "รอบบิล (ตามวันตัดรอบ)":
        cutoff_day = st.number_input(
            "วันที่ตัดรอบบิล", min_value=1, max_value=28, value=default_cutoff,
            help="เช่น ถ้าตัดวันที่ 20 ใส่ 20 | ตั้งค่าเริ่มต้นได้ในหน้า 'ตั้งค่า'"
        )

    # Load data - ถ้าเลือก "รอบบิล" + single month → load ทั้งหมดแล้ว filter ทีหลัง
    if group_mode == "รอบบิล (ตามวันตัดรอบ)" and period in ["current_month", "last_month"]:
        df = load_transactions("all")
    else:
        df = load_transactions(period)

    if df.empty:
        st.info("ยังไม่มีข้อมูล — กรุณานำเข้า Statement ในเมนู 'นำเข้า Statement' ก่อน")
        return

    # Adjust month if billing cycle mode
    if group_mode == "รอบบิล (ตามวันตัดรอบ)":
        # ตรวจสอบว่ามี cutoff_day หลายค่าไหม
        unique_cutoffs = df["cutoff_day"].dropna().unique()
        if len(unique_cutoffs) > 1:
            cutoff_list = ", ".join(map(str, sorted(unique_cutoffs)))
            st.info(f"📌 ข้อมูลรวมจากหลายบัตร มี {len(unique_cutoffs)} วันตัดรอบต่างกัน ({cutoff_list}) — ใช้วันตัดรอบของแต่ละ statement ในการคำนวณ")
        elif len(unique_cutoffs) == 0:
            st.info(f"ℹ️ ไม่มีข้อมูลวันตัดรอบใน statement — ใช้ค่าที่ตั้งไว้ ({cutoff_day}) สำหรับทุกรายการ")

        # ใช้ per-transaction cutoff_day ถ้ามี, ไม่งั้นใช้ค่า fallback
        df["effective_cutoff"] = df["cutoff_day"].fillna(cutoff_day)

        # ถ้า trans_date.day > effective_cutoff → นับเป็นรอบถัดไป
        adjusted = df.apply(
            lambda row: row["trans_date"].replace(day=1) + pd.DateOffset(months=1)
                if row["trans_date"].day > row["effective_cutoff"]
                else row["trans_date"].replace(day=1),
            axis=1
        )
        df["adjusted_month"] = adjusted.dt.to_period("M")

        # Filter by period using adjusted month
        if period == "current_month":
            target = pd.Timestamp.now().to_period("M")
            df = df[df["adjusted_month"] == target]
        elif period == "last_month":
            target = (pd.Timestamp.now() - pd.DateOffset(months=1)).to_period("M")
            df = df[df["adjusted_month"] == target]

        # Check if empty after filtering
        if df.empty:
            st.warning(f"ไม่มีข้อมูลในรอบบิล {period_label} (ตัดรอบวันที่ {cutoff_day})")
            return

        # Use adjusted month for grouping
        df["month_sort"] = df["adjusted_month"].astype(str)
        df["month_label"] = pd.to_datetime(df["month_sort"]).dt.strftime("%b %Y")
    else:
        # Use trans_date as-is
        df["month_sort"] = df["trans_date"].dt.strftime("%Y-%m")
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
        df_chart = df.copy()
        df_chart["display_path"] = df_chart.apply(
            lambda row: f"{row['category']} > {row['subcategory']}"
                if pd.notna(row['subcategory']) else row['category'],
            axis=1
        )

        # Group by path
        path_df = df_chart.groupby("display_path")["amount"].sum().reset_index()
        path_df = path_df.sort_values("amount", ascending=False)

        # สร้าง sunburst data
        sunburst_data = []
        for _, row in path_df.iterrows():
            parts = row["display_path"].split(" > ")
            if len(parts) == 2:
                sunburst_data.append({"category": parts[0], "subcategory": parts[1], "amount": row["amount"]})
            else:
                sunburst_data.append({"category": parts[0], "subcategory": "", "amount": row["amount"]})

        sunburst_df = pd.DataFrame(sunburst_data)

        fig = px.sunburst(
            sunburst_df,
            path=["category", "subcategory"],
            values="amount",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig.update_traces(
            textinfo="label+percent parent+value",
            hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<br>%{percentParent}<extra></extra>"
        )
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
    st.subheader("ตั้งค่ารอบบิล")

    # โหลดค่า cutoff_day จาก config
    current_cutoff = 20  # default
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text())
            current_cutoff = cfg.get("cutoff_day", 20)
        except Exception:
            pass

    cutoff_day_setting = st.number_input(
        "วันที่ตัดรอบบิล (ค่าเริ่มต้น)",
        min_value=1,
        max_value=28,
        value=current_cutoff,
        help="ตั้งค่าวันที่ตัดรอบบิลเริ่มต้น จะใช้ในหน้า Dashboard เมื่อเลือกโหมด 'รอบบิล'",
    )

    if st.button("บันทึกวันตัดรอบบิล"):
        # อ่าน config เดิมก่อน
        config = {}
        if CONFIG_PATH.exists():
            try:
                config = json.loads(CONFIG_PATH.read_text())
            except Exception:
                pass
        config["cutoff_day"] = cutoff_day_setting
        CONFIG_PATH.write_text(json.dumps(config))
        st.success(f"บันทึกแล้ว — วันตัดรอบบิล: {cutoff_day_setting}")

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