import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import io
import time

# --- 1. System Config & Auth ---
st.set_page_config(page_title="Advanced WMS Picking System", layout="wide", page_icon="📦")

# --- Master_Pick_Data Headers (exact match to inventory file columns + WMS fields) ---
# Inventory file headers in exact order (per latest notepad):
INVENTORY_HEADERS = [
    'Wh Id', 'Client Code', 'Pallet', 'Invoice Number', 'Location Id', 'Item Number',
    'Description', 'Lot Number', 'Actual Qty', 'Unavailable Qty', 'Uom', 'Status',
    'Mlp', 'Stored Attribute Id', 'Fifo Date', 'Expiration Date', 'Grn Number',
    'Gate Pass Id', 'Cust Dec No', 'Color', 'Size', 'Style', 'Supplier', 'Plant',
    'Client So', 'Client So Line', 'Po Cust Dec', 'Customer Ref Number', 'Item Id',
    'Invoice Number1', 'Transaction', 'Order Type', 'Order Number', 'Store Order Number',
    'Customer Po Number', 'Partial Order Flag', 'Order Date', 'Load Id', 'Asn Number',
    'Po Number', 'Supplier Hu', 'New Item Number', 'Asn Line Number',
    'Received Gross Weight', 'Current Gross Weight', 'Received Net Weight',
    'Current Net Weight', 'Supplier Desc', 'Cbm', 'Container Type', 'Display Item Number',
    'Old Item Number', 'Inventory Type', 'Type Qc', 'Vendor Name', 'Manufacture Date',
    'Suom', 'S Qty', 'Pick Id', 'Downloaded Date',
]

# WMS fields appended after inventory columns
WMS_FIELDS = ['Batch ID', 'SO Number', 'Generated Load ID', 'Country Name', 'Pick Quantity', 'Remark']

# Full Master_Pick_Data headers = inventory columns + WMS fields
MASTER_PICK_HEADERS = INVENTORY_HEADERS + WMS_FIELDS

# Lookup: lowercase stripped header → exact MASTER_PICK_HEADERS name
HEADER_LOWER_MAP = {h.strip().lower(): h for h in MASTER_PICK_HEADERS}

SHEET_HEADERS = {
    "Load_History": ['Batch ID', 'Generated Load ID', 'SO Number', 'Country Name', 'SHIP MODE', 'Date', 'Pick Status'],
    "Summary_Data": ['Batch ID', 'SO Number', 'Load ID', 'UPC', 'Country', 'Ship Mode', 'Requested', 'Picked', 'Variance', 'Status'],
    "Master_Partial_Data": ['Batch ID', 'SO Number', 'Pallet', 'Supplier', 'Load ID', 'Country Name',
                             'Actual Qty', 'Partial Qty', 'Gen Pallet ID', 'Balance Qty',
                             'Location Id', 'Lot Number', 'Color', 'Size', 'Style', 'Customer Po Number'],
    "Master_Pick_Data": MASTER_PICK_HEADERS,
    "Damage_Items": ['Pallet', 'Actual Qty', 'Remark', 'Date Added', 'Added By']
}


def show_confetti():
    st.markdown("""
    <style>
    @keyframes confetti-fall {
        0%   { transform: translateY(-20px) rotate(0deg); opacity:1; }
        100% { transform: translateY(100vh) rotate(720deg); opacity:0; }
    }
    .confetti-piece {
        position: fixed;
        width: 10px; height: 10px;
        top: -20px;
        animation: confetti-fall linear forwards;
        z-index: 9999;
        border-radius: 2px;
    }
    </style>
    <script>
    (function(){
        const colors = ['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6','#1abc9c','#e67e22','#e91e63'];
        const container = document.body;
        for(let i=0;i<80;i++){
            const el = document.createElement('div');
            el.className='confetti-piece';
            el.style.left = Math.random()*100+'vw';
            el.style.background = colors[Math.floor(Math.random()*colors.length)];
            el.style.animationDuration = (1.5+Math.random()*2)+'s';
            el.style.animationDelay = (Math.random()*1.5)+'s';
            el.style.width = (6+Math.random()*8)+'px';
            el.style.height = (6+Math.random()*8)+'px';
            container.appendChild(el);
            setTimeout(()=>el.remove(), 4000);
        }
    })();
    </script>
    """, unsafe_allow_html=True)

# --- Branding Footer CSS ---
def footer_branding():
    st.markdown("""
    <style>
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: transparent;
        color: #888888;
        text-align: center;
        font-size: 13px;
        padding: 10px;
        font-weight: bold;
        z-index: 100;
    }
    </style>
    <div class="footer">Developed by Ishanka Madusanka</div>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def get_master_workbook(retries=3, delay=5):
    """Open workbook — uses APIManager cache (5 min TTL)."""
    return APIManager.get_workbook(retries=retries, delay=delay)

class APIManager:
    """
    Centralized Google Sheets API Management:
    - In-memory sheet cache (TTL-based, 30s)
    - Request throttling (0.35s min interval = ~170/min max, quota safe)
    - Batch read multiple sheets in one pass
    - ws.update() replaces clear()+append_rows() (2 calls → 1)
    """
    _cache: dict = {}
    _cache_ttl: int = 30
    _last_request: float = 0.0
    _min_interval: float = 0.35
    _lock = __import__('threading').Lock()
    _wb_cache = None
    _wb_ts: float = 0.0
    _wb_ttl: int = 300

    @classmethod
    def _throttle(cls):
        with cls._lock:
            elapsed = time.time() - cls._last_request
            if elapsed < cls._min_interval:
                time.sleep(cls._min_interval - elapsed)
            cls._last_request = time.time()

    @classmethod
    def get_workbook(cls, retries=3, delay=5):
        """Cached workbook (5 min TTL)."""
        now = time.time()
        if cls._wb_cache is not None and (now - cls._wb_ts) < cls._wb_ttl:
            return cls._wb_cache
        for attempt in range(retries):
            try:
                cls._throttle()
                client = get_gsheet_client()
                wb = client.open_by_url(st.secrets["general"]["spreadsheet_url"])
                cls._wb_cache = wb
                cls._wb_ts = time.time()
                return wb
            except gspread.exceptions.APIError as e:
                status = getattr(e.response, 'status_code', None)
                if status in (429, 500, 502, 503) and attempt < retries - 1:
                    wait = delay * (attempt + 1)
                    st.warning(f"⏳ Google Sheets API busy (attempt {attempt+1}/{retries}). Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    if status == 403:
                        st.error("❌ Google Sheets access denied (403). Service Account share confirm කරන්න.")
                    elif status == 429:
                        st.error("❌ API quota exceeded. ටිකක් wait කරලා retry කරන්න.")
                    elif status == 404:
                        st.error("❌ Spreadsheet not found (404). spreadsheet_url check කරන්න.")
                    else:
                        st.error(f"❌ Google Sheets API Error ({status}).")
                    raise
            except Exception as e:
                st.error(f"❌ Connection error: {e}")
                raise

    @classmethod
    def invalidate(cls, sheet_name=None):
        if sheet_name:
            cls._cache.pop(sheet_name, None)
        else:
            cls._cache.clear()

    @classmethod
    def _parse_ws_data(cls, data):
        if not data:
            return pd.DataFrame()
        raw_headers = [str(h).strip() for h in data[0]]
        headers, seen = [], {}
        for h in raw_headers:
            if h in seen:
                seen[h] += 1
                headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                headers.append(h)
        return pd.DataFrame(data[1:], columns=headers) if len(data) > 1 else pd.DataFrame(columns=headers)

    @classmethod
    def read_sheet(cls, sh, sheet_name, force=False):
        """Read sheet with TTL cache. Returns DataFrame."""
        now = time.time()
        if not force and sheet_name in cls._cache:
            ts, df = cls._cache[sheet_name]
            if (now - ts) < cls._cache_ttl:
                return df.copy()
        for attempt in range(3):
            try:
                cls._throttle()
                ws = sh.worksheet(sheet_name)
                data = ws.get_all_values()
                df = cls._parse_ws_data(data)
                cls._cache[sheet_name] = (time.time(), df)
                return df.copy()
            except gspread.exceptions.APIError:
                if attempt < 2:
                    time.sleep(3 * (attempt + 1))
                else:
                    if sheet_name in cls._cache:
                        return cls._cache[sheet_name][1].copy()
                    return pd.DataFrame()
            except Exception:
                if sheet_name in cls._cache:
                    return cls._cache[sheet_name][1].copy()
                return pd.DataFrame()

    @classmethod
    def batch_read(cls, sh, sheet_names: list, force=False) -> dict:
        """Read multiple sheets, returns {name: DataFrame}."""
        return {name: cls.read_sheet(sh, name, force=force) for name in sheet_names}

    @classmethod
    def get_or_create_ws(cls, sh, name, headers, retries=3):
        """Get or create worksheet."""
        for attempt in range(retries):
            try:
                cls._throttle()
                ws = sh.worksheet(name)
                vals = ws.get_all_values()
                if not vals:
                    cls._throttle()
                    ws.append_row(headers)
                return ws
            except gspread.exceptions.WorksheetNotFound:
                try:
                    cls._throttle()
                    ws = sh.add_worksheet(title=name, rows="5000", cols=str(max(20, len(headers) + 5)))
                    cls._throttle()
                    ws.append_row(headers)
                    return ws
                except gspread.exceptions.APIError:
                    if attempt < retries - 1:
                        time.sleep(3 * (attempt + 1))
                    else:
                        raise
            except gspread.exceptions.APIError:
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    raise

    @classmethod
    def overwrite_sheet(cls, sh, sheet_name, headers, df, retries=3):
        """
        Overwrite sheet: clear + write all rows in ONE ws.update() call.
        2-3x faster than clear() + append_rows() separately.
        """
        all_rows = [headers]
        if not df.empty:
            all_rows += df.astype(str).replace('nan', '').values.tolist()
        for attempt in range(retries):
            try:
                cls._throttle()
                ws = cls.get_or_create_ws(sh, sheet_name, headers)
                cls._throttle()
                ws.clear()
                cls._throttle()
                ws.update(all_rows, value_input_option='RAW')
                cls.invalidate(sheet_name)
                return True
            except gspread.exceptions.APIError:
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    raise
        return False

    @classmethod
    def append_rows_to_sheet(cls, sh, sheet_name, headers, rows, retries=3):
        """Append rows batch. Invalidates cache."""
        if not rows:
            return True
        for attempt in range(retries):
            try:
                cls._throttle()
                ws = cls.get_or_create_ws(sh, sheet_name, headers)
                cls._throttle()
                ws.append_rows(rows, value_input_option='RAW')
                cls.invalidate(sheet_name)
                return True
            except gspread.exceptions.APIError:
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    raise
        return False


# ── Drop-in replacements ──────────────────────────────────────────
def get_safe_dataframe(sh, sheet_name, retries=3):
    return APIManager.read_sheet(sh, sheet_name)

def get_or_create_sheet(sh, name, headers, retries=3):
    return APIManager.get_or_create_ws(sh, name, headers)

# --- 2. User Management & Login ---
def init_users_sheet(sh):
    ws = get_or_create_sheet(sh, "Users", ["Username", "Password", "Role"])
    users_df = get_safe_dataframe(sh, "Users")
    if users_df.empty:
        ws.append_row(["admin", "admin@123", "admin"])
        ws.append_row(["sys", "sys@123", "SysUser"])
        ws.append_row(["user", "user@123", "user"])
        return get_safe_dataframe(sh, "Users")
    return users_df

def login_section():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if 'role' not in st.session_state:
        st.session_state['role'] = 'user'
    if 'username' not in st.session_state:
        st.session_state['username'] = 'Unknown'

    if not st.session_state['logged_in']:
        st.markdown("""
        <div style="text-align:center; padding: 50px 0 5px 0;">
            <div style="font-family:'Georgia',serif; font-size:34px; font-weight:800; letter-spacing:7px; color:#1a1a1a;">HELEN KAMINSKI</div>
            <div style="font-size:13px; letter-spacing:4px; color:#888; margin-top:5px; font-weight:500;">PICK MANAGEMENT</div>
            <div style="width:80px; height:2px; background:#1a1a1a; margin:14px auto 30px auto;"></div>
        </div>
        """, unsafe_allow_html=True)

        # Centered login form
        col_l, col_mid, col_r = st.columns([1, 1.2, 1])
        with col_mid:
            st.markdown("#### 🔐 Login")
            try:
                sh = get_master_workbook()
                users_df = init_users_sheet(sh)
            except Exception as e:
                st.error("Google Sheets සම්බන්ධ වීමේ දෝෂයක්. Secrets පරීක්ෂා කරන්න.")
                return False

            user = st.text_input("Username", key="login_user")
            pw = st.text_input("Password", type="password", key="login_pw")

            if st.button("Login", type="primary", use_container_width=True):
                user_match = users_df[(users_df['Username'] == user) & (users_df['Password'] == str(pw))]
                if not user_match.empty:
                    st.session_state['logged_in'] = True
                    st.session_state['role'] = user_match.iloc[0]['Role']
                    st.session_state['username'] = user
                    st.rerun()
                else:
                    st.error("වැරදි Username හෝ Password එකක්!")
        return False

    # Sidebar branding
    st.sidebar.markdown("""
    <div style="text-align:center; padding:12px 0 14px 0; border-bottom:1px solid #eee; margin-bottom:8px;">
        <div style="font-family:'Georgia',serif; font-size:14px; font-weight:800; letter-spacing:3px; color:#1a1a1a;">HELEN KAMINSKI</div>
        <div style="font-size:9px; letter-spacing:2px; color:#999; margin-top:2px;">PICK MANAGEMENT</div>
    </div>
    """, unsafe_allow_html=True)
    return True

# --- 3. Inventory Logic ---
def get_damage_pallets(sh):
    """Get all damage pallets that should be excluded from picking."""
    try:
        dmg_df = get_safe_dataframe(sh, "Damage_Items")
        if not dmg_df.empty and 'Pallet' in dmg_df.columns and 'Actual Qty' in dmg_df.columns:
            dmg_df['Actual Qty'] = pd.to_numeric(dmg_df['Actual Qty'], errors='coerce').fillna(0)
            dmg_summary = dmg_df.groupby('Pallet')['Actual Qty'].sum().reset_index()
            dmg_summary.columns = ['Pallet', 'Damage_Qty']
            return dmg_summary
    except:
        pass
    return pd.DataFrame(columns=['Pallet', 'Damage_Qty'])

def reconcile_inventory(inv_df, sh):
    """
    MAIN TASK:
    - Compare inventory Pallet + Actual Qty vs Master_Pick_Data totals
    - inv qty == picked qty → skip (do NOT pick)
    - inv qty > picked qty → take excess only
    - inv qty < picked qty → skip
    - Fully exclude Damage_Items pallets
    """
    # Normalize inventory columns
    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_lower = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    pallet_col   = inv_col_lower.get('pallet', 'Pallet')
    actual_col   = inv_col_lower.get('actual qty', 'Actual Qty')

    if actual_col not in inv_df.columns:
        # Try to find any column with 'actual' in name
        actual_col = next((c for c in inv_df.columns if 'actual' in c.lower()), actual_col)

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)

    try:
        pick_history = get_safe_dataframe(sh, "Master_Pick_Data")
        if not pick_history.empty and 'Actual Qty' in pick_history.columns and 'Pallet' in pick_history.columns:
            pick_history['Actual Qty'] = pd.to_numeric(pick_history['Actual Qty'], errors='coerce').fillna(0)
            # ✅ FIX: Convert Pallet to str to avoid int64 vs str merge error
            pick_history['Pallet'] = pick_history['Pallet'].astype(str).str.strip()
            pick_summary = pick_history.groupby('Pallet')['Actual Qty'].sum().reset_index()
            pick_summary.columns = ['_pallet_key', 'Total_Picked']

            # ✅ FIX: Convert inv pallet to str for safe merge
            inv_df['_pallet_key'] = inv_df[pallet_col].astype(str).str.strip()
            inv_df = pd.merge(inv_df, pick_summary, on='_pallet_key', how='left')
            inv_df = inv_df.drop(columns=['_pallet_key'], errors='ignore')
            inv_df['Total_Picked'] = inv_df['Total_Picked'].fillna(0)
            inv_df[actual_col] = (inv_df[actual_col] - inv_df['Total_Picked']).clip(lower=0)
            inv_df = inv_df.drop(columns=['Total_Picked'], errors='ignore')
    except Exception as e:
        st.warning(f"Inventory Reconcile Error: {e}")

    # Fully exclude Damage_Items pallets (str comparison - safe for all types)
    try:
        dmg_summary = get_damage_pallets(sh)
        if not dmg_summary.empty:
            damage_pallet_set = set(dmg_summary['Pallet'].astype(str).str.strip().tolist())
            inv_df = inv_df[~inv_df[pallet_col].astype(str).str.strip().isin(damage_pallet_set)].reset_index(drop=True)
    except Exception as e:
        st.warning(f"Damage Exclude Error: {e}")

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)
    inv_df = inv_df[inv_df[actual_col] > 0].reset_index(drop=True)
    return inv_df

def generate_unique_load_id(sh, so_num, so_counts):
    """Generate a unique Load ID that doesn't exist in Load_History."""
    hist_df = get_safe_dataframe(sh, "Load_History")
    existing_ids = set()
    if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
        existing_ids = set(hist_df['Generated Load ID'].astype(str).tolist())

    count = so_counts.get(so_num, 0)
    while True:
        count += 1
        candidate = f"SO-{so_num}-{count:03d}"
        if candidate not in existing_ids:
            return candidate, count

def process_picking(inv_df, req_df, batch_id, sh=None):
    pick_rows, partial_rows, summary = [], [], []

    # Normalize inventory column names: strip whitespace
    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]

    # Build inv_col_map: lowercase_stripped → actual inventory column name
    inv_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    # Find supplier and pick_id columns using loose match
    supplier_col = next((inv_col_map[k] for k in inv_col_map if k == 'supplier'), None)
    pick_id_col  = next((inv_col_map[k] for k in inv_col_map if k in ('pick id', 'pickid')), None)

    # Also find Pallet column
    pallet_col = next((inv_col_map[k] for k in inv_col_map if k == 'pallet'), 'Pallet')

    temp_inv = inv_df.copy()
    # Normalize Actual Qty column name for internal use
    actual_qty_col = next((inv_col_map[k] for k in inv_col_map if k == 'actual qty'), 'Actual Qty')
    temp_inv[actual_qty_col] = pd.to_numeric(temp_inv[actual_qty_col], errors='coerce').fillna(0)
    temp_inv = temp_inv[temp_inv[actual_qty_col] > 0].reset_index(drop=True)

    # ── Normalize Supplier column: float → integer string (remove .0 suffix) ──
    # e.g. 657001362301.0 → "657001362301"  so UPC matching works correctly
    if supplier_col and supplier_col in temp_inv.columns:
        def normalize_supplier(val):
            try:
                f = float(val)
                if f == int(f):
                    return str(int(f))
                return str(f)
            except (ValueError, TypeError):
                return str(val).strip()
        temp_inv[supplier_col] = temp_inv[supplier_col].apply(normalize_supplier)

    # Load existing Gen Pallet IDs from Master_Partial_Data to avoid duplicates
    existing_gen_pallet_ids = set()
    if sh is not None:
        try:
            existing_partial = get_safe_dataframe(sh, "Master_Partial_Data")
            if not existing_partial.empty and 'Gen Pallet ID' in existing_partial.columns:
                existing_gen_pallet_ids = set(existing_partial['Gen Pallet ID'].astype(str).tolist())
        except:
            pass

    gen_pallet_counter = [0]

    def make_unique_gen_pallet_id(pallet):
        while True:
            gen_pallet_counter[0] += 1
            candidate = f"{pallet}-P{gen_pallet_counter[0]:04d}"
            if candidate not in existing_gen_pallet_ids:
                existing_gen_pallet_ids.add(candidate)
                return candidate

    def get_inv_val(item, master_header):
        """
        Map a MASTER_PICK_HEADER name → inventory column value.
        Strategy:
          1. Exact match
          2. Case-insensitive + stripped match via inv_col_map
        """
        key = str(master_header).strip().lower()
        # Try exact first
        if master_header in item.index:
            return item[master_header]
        # Try lowercase map
        orig = inv_col_map.get(key)
        if orig and orig in item.index:
            return item[orig]
        return ''

    for lid in req_df['Generated Load ID'].unique():
        current_reqs = req_df[req_df['Generated Load ID'] == lid]
        so_num = str(current_reqs['SO Number'].iloc[0])
        ship_mode = str(current_reqs['SHIP MODE: (SEA/AIR)'].iloc[0]) if 'SHIP MODE: (SEA/AIR)' in current_reqs.columns else ""

        for _, req in current_reqs.iterrows():
            # Normalize UPC: remove .0 suffix if numeric (matches inventory Supplier format)
            raw_upc = req['Product UPC']
            try:
                f_upc = float(raw_upc)
                upc = str(int(f_upc)) if f_upc == int(f_upc) else str(f_upc)
            except (ValueError, TypeError):
                upc = str(raw_upc).strip()

            needed = float(req['PICK QTY'])
            country = req['Country Name']

            if supplier_col and supplier_col in temp_inv.columns:
                stock = temp_inv[
                    (temp_inv[supplier_col].astype(str).str.strip() == upc) &
                    (temp_inv[actual_qty_col] > 0)
                ].sort_values(by=actual_qty_col, ascending=False)
            else:
                stock = pd.DataFrame()

            picked_qty = 0

            for idx, item in stock.iterrows():
                if needed <= 0:
                    break

                current_avail = float(temp_inv.at[idx, actual_qty_col])
                if current_avail <= 0:
                    continue

                take = min(current_avail, needed)

                if take > 0:
                    # Build pick row strictly aligned to MASTER_PICK_HEADERS
                    p_row = {}
                    for header in MASTER_PICK_HEADERS:
                        p_row[header] = get_inv_val(item, header)

                    # Override WMS-specific fields
                    p_row['Actual Qty']        = take
                    p_row['Pick Quantity']     = take
                    p_row['Pick Id']           = str(item[pick_id_col]) if pick_id_col and pick_id_col in item.index else ''
                    p_row['Supplier']          = str(item[supplier_col]) if supplier_col and supplier_col in item.index else upc
                    p_row['Batch ID']          = batch_id
                    p_row['SO Number']         = so_num
                    p_row['Generated Load ID'] = lid
                    p_row['Country Name']      = country
                    p_row['Remark']            = ''
                    p_row['Order Type']        = 'Sample Orders'
                    p_row['Order Number']      = lid
                    p_row['Store Order Number']= lid
                    p_row['Customer Po Number']= f"{country}-{lid}"
                    p_row['Load Id']           = lid

                    pick_rows.append(p_row)

                    pallet_val = str(item[pallet_col]) if pallet_col in item.index else ''
                    if take < current_avail:
                        # Get extra fields from inventory row (case-insensitive)
                        def _get(col_name):
                            c = inv_col_map.get(col_name.lower())
                            return str(item[c]) if c and c in item.index else ''

                        partial_rows.append({
                            'Batch ID':           batch_id,
                            'SO Number':          so_num,
                            'Pallet':             pallet_val,
                            'Supplier':           p_row['Supplier'],
                            'Load ID':            lid,
                            'Country Name':       country,
                            'Actual Qty':         current_avail,
                            'Partial Qty':        take,
                            'Gen Pallet ID':      make_unique_gen_pallet_id(pallet_val),
                            'Balance Qty':        current_avail - take,   # Actual Qty - Partial Qty
                            'Location Id':        _get('location id'),
                            'Lot Number':         _get('lot number'),
                            'Color':              _get('color'),
                            'Size':               _get('size'),
                            'Style':              _get('style'),
                            'Customer Po Number': _get('customer po number'),
                        })

                    temp_inv.at[idx, actual_qty_col] -= take
                    needed -= take
                    picked_qty += take

            variance = float(req['PICK QTY']) - picked_qty
            summary.append({
                'Batch ID': batch_id, 'SO Number': so_num, 'Load ID': lid,
                'UPC': upc, 'Country': country, 'Ship Mode': ship_mode,
                'Requested': req['PICK QTY'], 'Picked': picked_qty,
                'Variance': variance,
                'Status': 'Fully Picked' if variance == 0 else 'Shortage'
            })

    # Build pick_df strictly from MASTER_PICK_HEADERS — correct column order guaranteed
    if pick_rows:
        pick_df = pd.DataFrame(pick_rows, columns=MASTER_PICK_HEADERS)
        pick_df['Actual Qty'] = pd.to_numeric(pick_df['Actual Qty'], errors='coerce').fillna(0)
        pick_df = pick_df[pick_df['Actual Qty'] > 0].reset_index(drop=True)
    else:
        pick_df = pd.DataFrame(columns=MASTER_PICK_HEADERS)

    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)

def generate_inventory_details_report(inv_df, sh):
    """
    Generate inventory details report showing allocation status.
    - Checks Master_Pick_Data for pick allocations (one line per allocation)
    - Checks Damage_Items for damage status and remark
    - Adds: Batch ID, SO Number, Generated Load ID, Country Name, Pick Quantity, Remark, Allocation Status
    """
    try:
        pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
        hist_df = get_safe_dataframe(sh, "Load_History")

        # Build damage lookup: {pallet: remark}
        damage_lookup = {}
        try:
            dmg_df = get_safe_dataframe(sh, "Damage_Items")
            if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                for _, dr in dmg_df.iterrows():
                    p = str(dr.get('Pallet', '')).strip()
                    r = str(dr.get('Remark', 'Damage')).strip()
                    qty = str(dr.get('Actual Qty', '')).strip()
                    if p:
                        # If same pallet has multiple damage rows, concatenate remarks
                        existing = damage_lookup.get(p, '')
                        new_remark = f"DAMAGE: {r} (Qty:{qty})" if qty else f"DAMAGE: {r}"
                        damage_lookup[p] = (existing + ' | ' + new_remark).lstrip(' | ') if existing else new_remark
        except Exception as e:
            pass

        report_rows = []

        for _, inv_row in inv_df.iterrows():
            pallet = str(inv_row.get('Pallet', '')).strip()

            # --- Check if this pallet is a DAMAGE item ---
            if pallet in damage_lookup:
                row = inv_row.copy()
                row['Batch ID'] = ''
                row['SO Number'] = ''
                row['Generated Load ID'] = ''
                row['Country Name'] = ''
                row['Pick Quantity'] = ''
                row['Remark'] = damage_lookup[pallet]
                row['Allocation Status'] = 'Damage'
                report_rows.append(row)
                continue  # Don't check picks for damage pallets

            # --- Check if this pallet has been picked ---
            if not pick_df.empty and 'Pallet' in pick_df.columns:
                pallet_picks = pick_df[pick_df['Pallet'].astype(str).str.strip() == pallet]

                if not pallet_picks.empty:
                    # One line per pick allocation
                    for _, pick_row in pallet_picks.iterrows():
                        row = inv_row.copy()
                        row['Batch ID'] = pick_row.get('Batch ID', '')
                        row['SO Number'] = pick_row.get('SO Number', '')
                        row['Generated Load ID'] = pick_row.get('Generated Load ID', pick_row.get('Load Id', ''))
                        row['Country Name'] = pick_row.get('Country Name', '')
                        row['Pick Quantity'] = pick_row.get('Pick Quantity', pick_row.get('Actual Qty', ''))
                        row['Remark'] = pick_row.get('Remark', 'Allocated')
                        row['Allocation Status'] = 'Picked'
                        report_rows.append(row)
                else:
                    # Available - not picked, not damaged
                    row = inv_row.copy()
                    row['Batch ID'] = ''
                    row['SO Number'] = ''
                    row['Generated Load ID'] = ''
                    row['Country Name'] = ''
                    row['Pick Quantity'] = ''
                    row['Remark'] = ''
                    row['Allocation Status'] = 'Available'
                    report_rows.append(row)
            else:
                row = inv_row.copy()
                row['Batch ID'] = ''
                row['SO Number'] = ''
                row['Generated Load ID'] = ''
                row['Country Name'] = ''
                row['Pick Quantity'] = ''
                row['Remark'] = ''
                row['Allocation Status'] = 'Available'
                report_rows.append(row)

        return pd.DataFrame(report_rows)

    except Exception as e:
        st.error(f"Report Generation Error: {e}")
        return pd.DataFrame()

# --- 4. App UI & Navigation ---
if login_section():
    current_user = st.session_state.get('username', 'User')
    current_role = st.session_state.get('role', 'user').upper()

    st.sidebar.success(f"👤 Logged in as: {current_user} ({current_role})")

    if 'welcomed' not in st.session_state:
        st.toast(f"Welcome back, {current_user}!", icon="👋")
        st.session_state['welcomed'] = True

    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

    # Reduce header font sizes globally
    st.markdown("""
    <style>
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.05rem !important; }
    </style>
    """, unsafe_allow_html=True)

    menu = []
    if current_role == 'ADMIN':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report", "🔄 Revert/Delete Picks", "🩹 Damage Items", "⚙️ Admin Settings"]
    elif current_role == 'SYSUSER':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report", "🔄 Revert/Delete Picks", "🩹 Damage Items"]
    else:  # user
        menu = ["📊 Dashboard & Tracking", "📋 Inventory Details Report", "🔄 Revert/Delete Picks", "🩹 Damage Items"]

    choice = st.sidebar.radio("Navigation Menu", menu)
    try:
        sh = get_master_workbook()
    except Exception:
        st.stop()

    # ==========================================
    # TAB 1: PICKING OPERATIONS
    # ==========================================
    if choice == "🚀 Picking Operations":
        st.title("📦 Inventory Picking Automation")
        col1, col2 = st.columns(2)
        inv_file = col1.file_uploader("1. Upload Inventory Report", type=['csv', 'xlsx'])
        req_file = col2.file_uploader("2. Upload Customer Requirement", type=['csv', 'xlsx'])

        if inv_file and req_file:
            if st.button("Generate Picks & Process", use_container_width=True, type="primary"):
                with st.spinner("🔄 Processing Data & Saving to Google Sheets..."):

                    inv = pd.read_csv(inv_file) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file)
                    req = pd.read_csv(req_file) if req_file.name.endswith('.csv') else pd.read_excel(req_file)

                    # --- Validate files are not swapped ---
                    inv_cols_lower = [str(c).strip().lower() for c in inv.columns]
                    req_cols_lower = [str(c).strip().lower() for c in req.columns]

                    REQ_REQUIRED = ['so number', 'country name', 'ship mode: (sea/air)', 'product upc', 'pick qty']
                    INV_REQUIRED = ['pallet', 'actual qty']

                    missing_req = [c for c in REQ_REQUIRED if c not in req_cols_lower]
                    missing_inv = [c for c in INV_REQUIRED if c not in inv_cols_lower]

                    # Detect if files are swapped
                    req_has_inv_cols = 'pallet' in req_cols_lower and 'actual qty' in req_cols_lower
                    inv_has_req_cols = 'so number' in inv_cols_lower and 'pick qty' in inv_cols_lower

                    if req_has_inv_cols and inv_has_req_cols:
                        st.error("⚠️ Files swapped! '1. Upload Inventory Report' හි Inventory file සහ '2. Upload Customer Requirement' හි Customer Requirement file upload කරන්න.")
                        st.stop()

                    if missing_req:
                        st.error(f"❌ Customer Requirement file හි required columns නොමැත: **{', '.join(missing_req)}**\n\nCustomer Requirement file '2. Upload Customer Requirement' හිම upload කරන්න.")
                        st.stop()

                    if missing_inv:
                        st.error(f"❌ Inventory file හි required columns නොමැත: **{', '.join(missing_inv)}**\n\nInventory file '1. Upload Inventory Report' හිම upload කරන්න.")
                        st.stop()

                    # Normalize req column names (strip + find correct case)
                    req_col_map = {str(c).strip().lower(): str(c).strip() for c in req.columns}

                    def get_req_col(name):
                        return req_col_map.get(name.lower(), name)

                    # Rename req columns to expected names
                    req = req.rename(columns={
                        req_col_map.get('so number', 'SO Number'): 'SO Number',
                        req_col_map.get('country name', 'Country Name'): 'Country Name',
                        req_col_map.get('ship mode: (sea/air)', 'SHIP MODE: (SEA/AIR)'): 'SHIP MODE: (SEA/AIR)',
                        req_col_map.get('product upc', 'Product UPC'): 'Product UPC',
                        req_col_map.get('pick qty', 'PICK QTY'): 'PICK QTY',
                    })

                    new_inv_cols = []
                    seen_inv = {}
                    for c in inv.columns:
                        c_str = str(c)
                        if c_str in seen_inv:
                            seen_inv[c_str] += 1
                            new_inv_cols.append(f"{c_str}.{seen_inv[c_str]}")
                        else:
                            seen_inv[c_str] = 0
                            new_inv_cols.append(c_str)
                    inv.columns = new_inv_cols

                    batch_id = f"REQ-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

                    ws_hist = get_or_create_sheet(sh, "Load_History", SHEET_HEADERS["Load_History"])
                    hist_df = get_safe_dataframe(sh, "Load_History")

                    req['SO Number'] = req['SO Number'].astype(str).str.strip()
                    req['Country Name'] = req['Country Name'].astype(str).str.strip()
                    req['SHIP MODE: (SEA/AIR)'] = req['SHIP MODE: (SEA/AIR)'].astype(str).str.strip()

                    req['Group'] = req['SO Number'] + "_" + req['Country Name'] + "_" + req['SHIP MODE: (SEA/AIR)']
                    new_hist_entries = []
                    load_id_map = {}
                    so_counts = {}

                    # Track existing Load ID counts per SO to avoid duplicates
                    if not hist_df.empty and 'SO Number' in hist_df.columns and 'Generated Load ID' in hist_df.columns:
                        for so in hist_df['SO Number'].astype(str).unique():
                            so_history = hist_df[hist_df['SO Number'].astype(str) == so]
                            so_counts[so] = len(so_history['Generated Load ID'].dropna().unique())

                    existing_load_ids = set()
                    if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
                        existing_load_ids = set(hist_df['Generated Load ID'].astype(str).tolist())

                    for group, data in req.groupby('Group'):
                        so_num = data['SO Number'].iloc[0]

                        # Generate unique Load ID - no duplicates allowed
                        base_count = so_counts.get(so_num, 0)
                        count = base_count
                        while True:
                            count += 1
                            candidate_lid = f"SO-{so_num}-{count:03d}"
                            if candidate_lid not in existing_load_ids:
                                break

                        so_counts[so_num] = count
                        existing_load_ids.add(candidate_lid)
                        load_id_map[group] = candidate_lid

                        new_hist_entries.append([
                            batch_id, candidate_lid, so_num,
                            data['Country Name'].iloc[0],
                            data['SHIP MODE: (SEA/AIR)'].iloc[0],
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Pending"
                        ])

                    req['Generated Load ID'] = req['Group'].map(load_id_map)
                    if new_hist_entries:
                        ws_hist.append_rows(new_hist_entries)

                    inv = reconcile_inventory(inv, sh)

                    pick_df, part_df, summ_df = process_picking(inv, req, batch_id, sh)

                    # ── Cannot-Pick Diagnostic ──────────────────────────────────────
                    # For each req UPC that has Picked=0 or Shortage, explain why
                    cannot_pick_rows = []
                    try:
                        # Get original inventory (before reconcile) for comparison
                        inv_orig = pd.read_csv(inv_file) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file)
                        inv_orig.columns = [str(c).strip() for c in inv_orig.columns]
                        inv_orig_col = {str(c).strip().lower(): str(c).strip() for c in inv_orig.columns}
                        orig_pallet_col = inv_orig_col.get('pallet', 'Pallet')
                        orig_actual_col = inv_orig_col.get('actual qty', 'Actual Qty')
                        orig_sup_col    = inv_orig_col.get('supplier', 'Supplier')

                        def norm_sup(v):
                            try:
                                f = float(v)
                                return str(int(f)) if f == int(f) else str(f)
                            except:
                                return str(v).strip()

                        inv_orig[orig_actual_col] = pd.to_numeric(inv_orig[orig_actual_col], errors='coerce').fillna(0)
                        inv_orig[orig_sup_col] = inv_orig[orig_sup_col].apply(norm_sup)

                        # Get Master_Pick_Data for already-picked info
                        mpd_df = get_safe_dataframe(sh, "Master_Pick_Data")
                        mpd_picked = {}  # pallet → already picked qty
                        if not mpd_df.empty and 'Pallet' in mpd_df.columns and 'Actual Qty' in mpd_df.columns:
                            mpd_df['Actual Qty'] = pd.to_numeric(mpd_df['Actual Qty'], errors='coerce').fillna(0)
                            for p, grp in mpd_df.groupby('Pallet'):
                                mpd_picked[str(p).strip()] = grp['Actual Qty'].sum()

                        # Damage pallets
                        dmg_df = get_safe_dataframe(sh, "Damage_Items")
                        dmg_pallets = set()
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            dmg_pallets = set(dmg_df['Pallet'].astype(str).str.strip().tolist())

                        for _, summ_row in summ_df.iterrows():
                            upc     = str(summ_row.get('UPC', ''))
                            picked  = float(summ_row.get('Picked', 0))
                            requested = float(summ_row.get('Requested', 0))
                            if picked >= requested:
                                continue  # fully picked — no issue

                            missing = requested - picked

                            # Find pallets in orig inv with this UPC
                            upc_pallets = inv_orig[inv_orig[orig_sup_col] == upc]

                            for _, prow in upc_pallets.iterrows():
                                pallet   = str(prow.get(orig_pallet_col, '')).strip()
                                orig_qty = float(prow.get(orig_actual_col, 0))
                                already  = mpd_picked.get(pallet, 0)
                                avail    = max(0, orig_qty - already)
                                is_dmg   = pallet in dmg_pallets

                                if is_dmg:
                                    reason = "🔴 Damage Item — excluded from picks"
                                elif already >= orig_qty:
                                    reason = f"✅ Fully picked in previous batch (Picked={int(already)}, Inv={int(orig_qty)})"
                                elif already > 0:
                                    reason = f"⚠️ Partially picked — available balance: {int(avail)} (Inv={int(orig_qty)}, Already picked={int(already)})"
                                elif orig_qty == 0:
                                    reason = "❌ Actual Qty = 0 in inventory"
                                else:
                                    reason = f"❓ Available={int(avail)} but not picked (check UPC match)"

                                cannot_pick_rows.append({
                                    'UPC': upc,
                                    'Pallet': pallet,
                                    'Inv Actual Qty': int(orig_qty),
                                    'Already Picked': int(already),
                                    'Available Now': int(avail),
                                    'Requested': int(requested),
                                    'Shortage': int(missing),
                                    'Reason': reason
                                })
                    except Exception as diag_e:
                        pass  # diagnostic errors must not block main flow

                    if not pick_df.empty:
                        # Append new picks (batch, throttled)
                        APIManager.append_rows_to_sheet(
                            sh, "Master_Pick_Data", MASTER_PICK_HEADERS,
                            pick_df.astype(str).replace('nan', '').values.tolist()
                        )

                    if not part_df.empty:
                        # Align part_df columns to SHEET_HEADERS["Master_Partial_Data"] exact order
                        mpd_headers = SHEET_HEADERS["Master_Partial_Data"]
                        for col in mpd_headers:
                            if col not in part_df.columns:
                                part_df[col] = ''
                        part_df_save = part_df[mpd_headers]
                        APIManager.append_rows_to_sheet(
                            sh, "Master_Partial_Data", mpd_headers,
                            part_df_save.astype(str).replace('nan', '').values.tolist()
                        )

                    # Summary_Data: Load ID can only belong to ONE Batch ID — deduplicate
                    existing_summ = APIManager.read_sheet(sh, "Summary_Data")
                    if not existing_summ.empty and 'Load ID' in existing_summ.columns and not summ_df.empty:
                        new_load_ids = set(summ_df['Load ID'].astype(str).tolist())
                        existing_summ_clean = existing_summ[
                            ~existing_summ['Load ID'].astype(str).isin(new_load_ids)
                        ]
                        combined_summ = pd.concat([existing_summ_clean, summ_df], ignore_index=True)
                    else:
                        combined_summ = summ_df
                    # Single overwrite call (clear + update in one batch)
                    APIManager.overwrite_sheet(sh, "Summary_Data", SHEET_HEADERS["Summary_Data"], combined_summ)

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        if not pick_df.empty: pick_df.to_excel(writer, sheet_name='Pick_Report', index=False)
                        if not part_df.empty: part_df.to_excel(writer, sheet_name='Partial_Report', index=False)
                        if not summ_df.empty: summ_df.to_excel(writer, sheet_name='Variance_Summary', index=False)

                    st.session_state['processed_excel'] = output.getvalue()
                    st.session_state['summary_df'] = summ_df
                    st.session_state['batch_id'] = batch_id
                    st.session_state['show_verification'] = True
                    st.session_state['cannot_pick_rows'] = cannot_pick_rows

                    st.success(f"✅ Data Processed! (Batch ID: {batch_id})")

        if st.session_state.get('show_verification', False):
            st.divider()
            st.subheader("📋 Verification: Customer Requirement vs Picked Data")
            st.info("කරුණාකර පහත Summary එක පරීක්ෂා කර Download කිරීමට පෙර Verify කරන්න.")
            st.dataframe(st.session_state['summary_df'].astype(str), use_container_width=True)

            # ── Cannot-Pick Diagnostic Table ──────────────────────────────
            cannot_rows = st.session_state.get('cannot_pick_rows', [])
            if cannot_rows:
                import pandas as pd
                cp_df = pd.DataFrame(cannot_rows)
                st.divider()
                st.markdown("### ⚠️ Pick කරන්න නොහැකි Pallets — හේතු සහිතව")
                st.caption(f"Pick නොවූ හෝ Shortage ඇති UPC {cp_df['UPC'].nunique()} ක, Pallet {len(cp_df)} ක් සඳහා:")

                # Color-code by reason type
                def highlight_reason(row):
                    if '✅' in str(row.get('Reason','')):
                        return ['background-color: #fff3cd'] * len(row)
                    elif '🔴' in str(row.get('Reason','')):
                        return ['background-color: #ffe0e0'] * len(row)
                    elif '⚠️' in str(row.get('Reason','')):
                        return ['background-color: #fff8e1'] * len(row)
                    elif '❌' in str(row.get('Reason','')):
                        return ['background-color: #fce4ec'] * len(row)
                    return [''] * len(row)

                try:
                    styled = cp_df.style.apply(highlight_reason, axis=1)
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                except:
                    st.dataframe(cp_df.astype(str), use_container_width=True, hide_index=True)

                # Summary by reason
                st.markdown("**හේතු Summary:**")
                reason_summary = cp_df['Reason'].apply(lambda r: r.split('—')[0].split('(')[0].strip()).value_counts()
                for reason, count in reason_summary.items():
                    st.markdown(f"- {reason}: **{count} pallets**")

                # Download cannot-pick report
                out_cp = io.BytesIO()
                with pd.ExcelWriter(out_cp, engine='xlsxwriter') as writer:
                    cp_df.to_excel(writer, sheet_name='Cannot_Pick', index=False)
                    wb = writer.book
                    ws_cp = writer.sheets['Cannot_Pick']
                    hdr_fmt = wb.add_format({'bold': True, 'bg_color': '1A1A1A', 'font_color': 'FFFFFF', 'border': 1})
                    for ci, col_name in enumerate(cp_df.columns):
                        ws_cp.write(0, ci, col_name, hdr_fmt)
                        ws_cp.set_column(ci, ci, 20)
                st.download_button(
                    "⬇️ Download Cannot-Pick Report",
                    data=out_cp.getvalue(),
                    file_name=f"Cannot_Pick_{st.session_state['batch_id']}.xlsx",
                    mime="application/vnd.ms-excel"
                )

            verify_check = st.checkbox("✅ මම Customer Requirement එක සහ Picked Data නිවැරදිදැයි පරීක්ෂා කළෙමි.")

            if verify_check:
                st.download_button(
                    "⬇️ Download Verified Processed Report",
                    data=st.session_state['processed_excel'],
                    file_name=f"WMS_{st.session_state['batch_id']}.xlsx",
                    mime="application/vnd.ms-excel",
                    use_container_width=True
                )
                show_confetti()

    # ==========================================
    # TAB 2: DASHBOARD & TRACKING
    # ==========================================
    elif choice == "📊 Dashboard & Tracking":
        col_t1, col_t2 = st.columns([4, 1])
        col_t1.title("📊 Load Tracking & Dashboard")
        if col_t2.button("🔄 Refresh Data", use_container_width=True):
            APIManager.invalidate()  # Clear all cached sheets
            st.rerun()

        # Batch read 3 sheets in one optimized pass (uses cache)
        _batch = APIManager.batch_read(sh, ["Load_History", "Summary_Data", "Master_Pick_Data"])
        hist_df = _batch["Load_History"]
        summ_df = _batch["Summary_Data"]
        pick_df = _batch["Master_Pick_Data"]

        total_loads = hist_df['Generated Load ID'].nunique() if not hist_df.empty and 'Generated Load ID' in hist_df.columns else 0
        total_picks = len(pick_df) if not pick_df.empty else 0
        pending_loads = len(hist_df[hist_df['Pick Status'] == 'Pending']) if not hist_df.empty and 'Pick Status' in hist_df.columns else 0
        processing_loads = len(hist_df[hist_df['Pick Status'] == 'Processing']) if not hist_df.empty and 'Pick Status' in hist_df.columns else 0

        st.subheader("📈 Overall System Summary")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Load IDs", total_loads)
        m2.metric("Total Picks Made", total_picks)
        m3.metric("Pending Loads", pending_loads)
        m4.metric("Processing Loads", processing_loads)
        st.divider()

        if total_loads == 0:
            st.info("දැනට පද්ධතියේ කිසිදු දත්තයක් නොමැත. කරුණාකර 'Picking Operations' මගින් දත්ත ඇතුලත් කරන්න.")
        else:
            # --- Load ID Cards Dashboard ---
            st.subheader("📦 Active Load ID Overview")
            st.caption("Cancelled සහ Completed Load IDs මෙහි නොපෙන්වයි.")

            if not hist_df.empty and 'Generated Load ID' in hist_df.columns and 'Pick Status' in hist_df.columns:
                active_loads = hist_df[
                    ~hist_df['Pick Status'].astype(str).isin(['Cancelled', 'Completed'])
                ].copy()

                if active_loads.empty:
                    st.info("සියලු Loads Completed හෝ Cancelled වී ඇත.")
                else:
                    load_ids = active_loads['Generated Load ID'].dropna().unique().tolist()

                    # --- Status Filter ---
                    filter_col1, filter_col2 = st.columns([2, 4])
                    status_filter = filter_col1.selectbox(
                        "🔽 Filter by Status:",
                        ["All", "Pending", "PL Pending", "Processing"],
                        key="dash_status_filter"
                    )
                    if status_filter != "All":
                        filtered_active = active_loads[active_loads['Pick Status'].astype(str) == status_filter]
                        load_ids = filtered_active['Generated Load ID'].dropna().unique().tolist()

                    # --- Build per-load summary data ---
                    summ_by_load = {}
                    if not summ_df.empty and 'Load ID' in summ_df.columns:
                        summ_df['Variance'] = pd.to_numeric(summ_df.get('Variance', 0), errors='coerce').fillna(0)
                        summ_df['Requested'] = pd.to_numeric(summ_df.get('Requested', 0), errors='coerce').fillna(0)
                        summ_df['Picked'] = pd.to_numeric(summ_df.get('Picked', 0), errors='coerce').fillna(0)
                        for lid_s in summ_df['Load ID'].dropna().unique():
                            rows = summ_df[summ_df['Load ID'].astype(str) == str(lid_s)]
                            summ_by_load[str(lid_s)] = {
                                'requested': rows['Requested'].sum(),
                                'picked': rows['Picked'].sum(),
                                'variance': rows['Variance'].sum()
                            }

                    # Categorise loads
                    zero_pick_ids, shortage_ids, full_pick_ids = [], [], []
                    for lid in load_ids:
                        s = summ_by_load.get(str(lid), {})
                        req_q = s.get('requested', 0)
                        picked_q = s.get('picked', 0)
                        var_q = s.get('variance', 0)
                        if picked_q == 0 and req_q > 0:
                            zero_pick_ids.append(lid)
                        elif var_q > 0:
                            shortage_ids.append(lid)
                        else:
                            full_pick_ids.append(lid)

                    STATUS_OPTIONS = ["Pending", "PL Pending", "Processing", "Completed", "Cancelled"]

                    # Pre-build pick counts per Load ID (fix column name flexibility)
                    pick_counts_by_lid = {}
                    pick_qty_by_lid = {}
                    if not pick_df.empty:
                        # Find Load Id column — try exact, then case-insensitive
                        load_id_col_pick = None
                        for c in pick_df.columns:
                            if str(c).strip().lower() in ('load id', 'loadid', 'load_id'):
                                load_id_col_pick = c
                                break
                        # Find Actual Qty column
                        actual_col_pick = None
                        for c in pick_df.columns:
                            if str(c).strip().lower() == 'actual qty':
                                actual_col_pick = c
                                break

                        if load_id_col_pick:
                            for lid_p in pick_df[load_id_col_pick].dropna().unique():
                                rows_p = pick_df[pick_df[load_id_col_pick].astype(str).str.strip() == str(lid_p).strip()]
                                pick_counts_by_lid[str(lid_p).strip()] = len(rows_p)
                                if actual_col_pick:
                                    pick_qty_by_lid[str(lid_p).strip()] = pd.to_numeric(rows_p[actual_col_pick], errors='coerce').sum()
                                else:
                                    pick_qty_by_lid[str(lid_p).strip()] = 0

                    def render_load_list(id_list, category_color, category_label):
                        """Render loads as a table-style list."""
                        # Header row
                        st.markdown(f"""
                        <div style="display:grid; grid-template-columns:2fr 1fr 1.2fr 1fr 1fr 1fr 1fr 1.5fr; gap:4px;
                             background:{category_color}15; border:1px solid {category_color}40;
                             border-radius:8px 8px 0 0; padding:7px 12px;
                             font-size:11px; font-weight:700; color:#444; margin-top:4px;">
                            <div>Load ID</div>
                            <div>SO</div>
                            <div>Country</div>
                            <div>Ship</div>
                            <div>Date</div>
                            <div>Lines</div>
                            <div>Qty</div>
                            <div>Status</div>
                        </div>
                        """, unsafe_allow_html=True)

                        for lid in id_list:
                            load_row = active_loads[active_loads['Generated Load ID'] == lid].iloc[0]
                            status   = str(load_row.get('Pick Status', 'Pending'))
                            so_num   = str(load_row.get('SO Number', '-'))
                            country  = str(load_row.get('Country Name', '-'))
                            ship     = str(load_row.get('SHIP MODE', '-'))
                            date     = str(load_row.get('Date', '-'))[:10]

                            lid_key      = str(lid).strip()
                            pick_count   = pick_counts_by_lid.get(lid_key, 0)
                            pick_qty_val = pick_qty_by_lid.get(lid_key, 0)

                            s         = summ_by_load.get(str(lid), {})
                            variance  = s.get('variance', 0)
                            requested = s.get('requested', 0)

                            status_bg   = {'Pending':'#fff3cd','Processing':'#cce5ff'}.get(status,'#f0f0f0')
                            status_col  = {'Pending':'#856404','Processing':'#004085'}.get(status,'#333')
                            status_dot  = {'Pending':'🟡','Processing':'🔵'}.get(status,'⚪')

                            shortage_tag = ''
                            if variance > 0:
                                shortage_tag = f'<span style="font-size:9px; background:#ffe0e0; color:#c0392b; padding:1px 5px; border-radius:4px; margin-left:4px;">⚠️ -{int(variance)}</span>'

                            row_html = f"""
                            <div style="display:grid; grid-template-columns:2fr 1fr 1.2fr 1fr 1fr 1fr 1fr 1.5fr; gap:4px;
                                 border-left:3px solid {category_color}; border-bottom:1px solid #eee;
                                 padding:7px 12px; background:#fff; font-size:11px; color:#333; align-items:center;">
                                <div style="font-weight:600; color:#1a1a1a;">{lid}{shortage_tag}</div>
                                <div>{so_num}</div>
                                <div>{country}</div>
                                <div>{ship}</div>
                                <div>{date}</div>
                                <div><b>{pick_count}</b></div>
                                <div><b>{int(pick_qty_val)}</b></div>
                                <div><span style="background:{status_bg}; color:{status_col}; font-size:10px; font-weight:600; padding:2px 8px; border-radius:10px;">{status_dot} {status}</span></div>
                            </div>
                            """
                            st.markdown(row_html, unsafe_allow_html=True)

                            # Inline update controls in a tight row
                            c1, c2 = st.columns([3, 1])
                            safe_idx = STATUS_OPTIONS.index(status) if status in STATUS_OPTIONS else 0
                            new_st = c1.selectbox("", STATUS_OPTIONS, index=safe_idx,
                                                  key=f"st_{lid}", label_visibility="collapsed")
                            if c2.button("💾 Save", key=f"upd_{lid}", use_container_width=True):
                                try:
                                    ws_hist_upd = sh.worksheet("Load_History")
                                    cell = ws_hist_upd.find(str(lid))
                                    if cell:
                                        ws_hist_upd.update_cell(cell.row, 7, new_st)
                                        # If Cancelled → auto-delete from Master_Pick_Data
                                        if new_st == "Cancelled":
                                            mpd = get_safe_dataframe(sh, "Master_Pick_Data")
                                            lid_col = next((c for c in mpd.columns if str(c).strip().lower() == 'load id'), None)
                                            if not mpd.empty and lid_col:
                                                filtered_mpd = mpd[mpd[lid_col].astype(str).str.strip() != str(lid).strip()]
                                                ws_pick_del = sh.worksheet("Master_Pick_Data")
                                                ws_pick_del.clear()
                                                ws_pick_del.append_row(MASTER_PICK_HEADERS)
                                                if not filtered_mpd.empty:
                                                    for col in MASTER_PICK_HEADERS:
                                                        if col not in filtered_mpd.columns:
                                                            filtered_mpd[col] = ''
                                                    ws_pick_del.append_rows(filtered_mpd[MASTER_PICK_HEADERS].astype(str).replace('nan','').values.tolist())
                                            st.success(f"✅ {lid} → Cancelled | Master_Pick_Data records deleted.")
                                        else:
                                            st.success(f"✅ {lid} → {new_st}")
                                        time.sleep(0.5)
                                        st.rerun()
                                except Exception as ex:
                                    st.error(f"Update failed: {ex}")

                    # --- Category 1: Zero Pick ---
                    if zero_pick_ids:
                        st.markdown('<div style="background:#6c757d; color:white; display:inline-block; font-size:11px; font-weight:700; padding:3px 14px; border-radius:12px; margin:16px 0 4px 0;">⬜ NOT PICKED &nbsp;·&nbsp; {}</div>'.format(len(zero_pick_ids)), unsafe_allow_html=True)
                        render_load_list(zero_pick_ids, "#adb5bd", "NOT PICKED")

                    # --- Category 2: Shortage ---
                    if shortage_ids:
                        st.markdown('<div style="background:#e74c3c; color:white; display:inline-block; font-size:11px; font-weight:700; padding:3px 14px; border-radius:12px; margin:16px 0 4px 0;">⚠️ SHORTAGE &nbsp;·&nbsp; {}</div>'.format(len(shortage_ids)), unsafe_allow_html=True)
                        render_load_list(shortage_ids, "#e74c3c", "SHORTAGE")

                    # --- Category 3: Full Pick ---
                    if full_pick_ids:
                        st.markdown('<div style="background:#27ae60; color:white; display:inline-block; font-size:11px; font-weight:700; padding:3px 14px; border-radius:12px; margin:16px 0 4px 0;">✅ FULLY PICKED &nbsp;·&nbsp; {}</div>'.format(len(full_pick_ids)), unsafe_allow_html=True)
                        render_load_list(full_pick_ids, "#27ae60", "FULLY PICKED")

            st.divider()

            # --- Batch Report Download ---
            st.subheader("📑 Download Total Report by Upload Batch")
            if not hist_df.empty and 'Batch ID' in hist_df.columns:
                available_batches = hist_df['Batch ID'].dropna().unique()
                if len(available_batches) > 0:
                    selected_batch = st.selectbox("Select Requirement Batch ID:", available_batches)
                    if st.button("Generate Total Batch Report"):
                        with st.spinner("Generating Total Report..."):
                            batch_picks = pick_df[pick_df['Batch ID'] == selected_batch] if not pick_df.empty and 'Batch ID' in pick_df.columns else pd.DataFrame()
                            batch_summ = summ_df[summ_df['Batch ID'] == selected_batch] if not summ_df.empty and 'Batch ID' in summ_df.columns else pd.DataFrame()

                            out_total = io.BytesIO()
                            with pd.ExcelWriter(out_total, engine='xlsxwriter') as writer:
                                if not batch_picks.empty: batch_picks.to_excel(writer, sheet_name='Pick_Report', index=False)
                                if not batch_summ.empty: batch_summ.to_excel(writer, sheet_name='Variance_Summary', index=False)

                            st.download_button("⬇️ Download Batch Excel", data=out_total.getvalue(), file_name=f"Total_Report_{selected_batch}.xlsx", mime="application/vnd.ms-excel")
            st.divider()

            # --- Advanced Search & Download ---
            st.subheader("🔍 Advanced Search & Pick Report Download")
            col_s1, col_s2 = st.columns([2, 3])

            search_by = col_s1.selectbox("🔎 Search By:", ["Load Id", "Pallet", "Supplier (Product UPC)", "SO Number"])

            search_term = None
            if search_by == "Load Id":
                if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
                    all_load_ids = hist_df['Generated Load ID'].dropna().unique().tolist()
                    search_term = col_s2.selectbox("Select Load ID:", all_load_ids)
                else:
                    search_term = col_s2.text_input("Enter Load ID:")
            elif search_by == "Pallet":
                search_term = col_s2.text_input("Enter Pallet ID:")
            elif search_by == "Supplier (Product UPC)":
                search_term = col_s2.text_input("Enter Supplier / UPC:")
            elif search_by == "SO Number":
                if not hist_df.empty and 'SO Number' in hist_df.columns:
                    so_options = hist_df['SO Number'].dropna().unique().tolist()
                    search_term = col_s2.selectbox("Select SO Number:", so_options)
                else:
                    search_term = col_s2.text_input("Enter SO Number:")

            if search_term:
                st.markdown(f"#### Results for **{search_by}**: `{search_term}`")

                col_map_pick = {
                    "Load Id":              "Load Id",
                    "Pallet":               "Pallet",
                    "Supplier (Product UPC)": "Supplier",
                    "SO Number":            "SO Number"
                }
                col_map_summ = {
                    "Load Id":              "Load ID",
                    "Pallet":               None,
                    "Supplier (Product UPC)": "UPC",
                    "SO Number":            "SO Number"
                }

                # --- Filter pick data ---
                filtered_picks = pd.DataFrame()
                if not pick_df.empty:
                    pick_search_col = col_map_pick[search_by]
                    # Find actual column name case-insensitively
                    actual_col_name = next(
                        (c for c in pick_df.columns if str(c).strip().lower() == pick_search_col.strip().lower()),
                        None
                    )
                    if actual_col_name:
                        if search_by == "Load Id":
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.strip() == str(search_term).strip()]
                        else:
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.contains(str(search_term).strip(), case=False, na=False)]

                # --- Filter summary data ---
                filtered_summ = pd.DataFrame()
                summ_search_key = col_map_summ[search_by]
                if not summ_df.empty and summ_search_key:
                    actual_summ_col = next(
                        (c for c in summ_df.columns if str(c).strip().lower() == summ_search_key.strip().lower()),
                        None
                    )
                    if actual_summ_col:
                        if search_by in ("Load Id", "SO Number"):
                            filtered_summ = summ_df[summ_df[actual_summ_col].astype(str).str.strip() == str(search_term).strip()]
                        else:
                            filtered_summ = summ_df[summ_df[actual_summ_col].astype(str).str.contains(str(search_term).strip(), case=False, na=False)]

                tab_p, tab_v, tab_dl = st.tabs(["📦 Picked Items Detail", "📉 Summary / Variance", "⬇️ Download Pick Report"])

                with tab_p:
                    if not filtered_picks.empty:
                        st.caption(f"{len(filtered_picks)} records found")
                        st.dataframe(filtered_picks.astype(str), use_container_width=True)
                    else:
                        st.info("No pick data found for this search.")

                with tab_v:
                    if not filtered_summ.empty:
                        st.dataframe(filtered_summ.astype(str), use_container_width=True)
                        if 'Variance' in filtered_summ.columns:
                            filtered_summ_num = filtered_summ.copy()
                            filtered_summ_num['Variance'] = pd.to_numeric(filtered_summ_num['Variance'], errors='coerce')
                            shortages = filtered_summ_num[filtered_summ_num['Variance'] > 0]
                            if not shortages.empty:
                                st.warning("⚠️ Shortages detected!")
                                cols_to_show = [c for c in ['UPC', 'Requested', 'Picked', 'Variance'] if c in shortages.columns]
                                st.table(shortages[cols_to_show])
                    elif not summ_search_key:
                        st.info("Summary view is not available for Pallet search.")
                    else:
                        st.info("No summary data found.")

                with tab_dl:
                    st.caption(f"Load ID / Search term: **{search_term}** හට අදාල Pick Report download කරන්න")
                    if not filtered_picks.empty:
                        # Build Excel with Pick + Summary sheets
                        out_pick_dl = io.BytesIO()
                        with pd.ExcelWriter(out_pick_dl, engine='xlsxwriter') as writer:
                            filtered_picks.to_excel(writer, sheet_name='Pick_Report', index=False)
                            if not filtered_summ.empty:
                                filtered_summ.to_excel(writer, sheet_name='Variance_Summary', index=False)
                            # Format
                            wb_dl = writer.book
                            hdr_dl = wb_dl.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#fff', 'border': 1})
                            ws_dl  = writer.sheets['Pick_Report']
                            for ci, col_name in enumerate(filtered_picks.columns):
                                ws_dl.write(0, ci, col_name, hdr_dl)
                                ws_dl.set_column(ci, ci, 16)
                            ws_dl.freeze_panes(1, 0)

                        safe_term = str(search_term).replace('/', '-').replace(' ', '_')
                        st.download_button(
                            f"⬇️ Download Pick Report — {search_term}",
                            data=out_pick_dl.getvalue(),
                            file_name=f"Pick_Report_{safe_term}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel",
                            use_container_width=True,
                            type="primary"
                        )
                        # Quick summary metrics
                        dl_qty = pd.to_numeric(filtered_picks.get('Actual Qty', pd.Series()), errors='coerce').sum()
                        dl_lines = len(filtered_picks)
                        mc1, mc2 = st.columns(2)
                        mc1.metric("Pick Lines", dl_lines)
                        mc2.metric("Total Pick Qty", int(dl_qty))
                    else:
                        st.info("Download කිරීමට data නොමැත.")

    # ==========================================
    # TAB 3: INVENTORY DETAILS REPORT
    # ==========================================
    elif choice == "📋 Inventory Details Report":
        st.title("📋 Inventory Details Report")

        inv_report_file = st.file_uploader("Upload Inventory File", type=['csv', 'xlsx'], key="inv_report_uploader")

        if inv_report_file:
            tab_basic, tab_formatted = st.tabs(["📋 Basic Report", "📊 Formatted Pick Report"])

            with tab_basic:
                st.caption("Inventory file allocation status report (Picked / Available / Damage)")
                if st.button("🔍 Generate Basic Report", type="primary", use_container_width=True, key="gen_basic"):
                    with st.spinner("Generating..."):
                        inv_data = pd.read_csv(inv_report_file) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file)
                        report_df = generate_inventory_details_report(inv_data, sh)
                        if not report_df.empty:
                            st.success(f"✅ Total rows: {len(report_df)}")
                            col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                            if 'Allocation Status' in report_df.columns:
                                col_r1.metric("Total Lines", len(report_df))
                                col_r2.metric("✅ Picked", len(report_df[report_df['Allocation Status']=='Picked']))
                                col_r3.metric("🟢 Available", len(report_df[report_df['Allocation Status']=='Available']))
                                col_r4.metric("🔴 Damage", len(report_df[report_df['Allocation Status']=='Damage']))
                            st.dataframe(report_df.astype(str), use_container_width=True)
                            out_basic = io.BytesIO()
                            with pd.ExcelWriter(out_basic, engine='xlsxwriter') as writer:
                                report_df.to_excel(writer, sheet_name='Inventory_Details', index=False)
                                wb = writer.book; ws_b = writer.sheets['Inventory_Details']
                                for fmt, col_val in [('#FFE0E0','Damage'),('#E8F5E9','Picked'),('#E3F2FD','Available')]:
                                    f = wb.add_format({'bg_color': fmt})
                                    if 'Allocation Status' in report_df.columns:
                                        for ri, sv in enumerate(report_df['Allocation Status'], 1):
                                            if sv == col_val: ws_b.set_row(ri, None, f)
                            st.download_button("⬇️ Download Basic Report", data=out_basic.getvalue(),
                                file_name=f"Inventory_Basic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.ms-excel", use_container_width=True)
                        else:
                            st.warning("Report generate කිරීම අසාර්ථක විය.")

            with tab_formatted:
                st.caption("""
                Notepad headers order → Pick Quantity (Master_Pick_Data) → Damage columns → Destination Country → Order NO → Partial Pallet replace
                """)
                if st.button("📊 Generate Formatted Pick Report", type="primary", use_container_width=True, key="gen_fmt"):
                    with st.spinner("Generating Formatted Report..."):
                        inv_data = pd.read_csv(inv_report_file) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file)
                        # Normalize column names: strip whitespace
                        inv_data.columns = [str(c).strip() for c in inv_data.columns]

                        # Build lowercase map BEFORE rename
                        inv_col_map_r = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

                        # Canonical header names we expect (REPORT_HEADERS exact names)
                        CANONICAL = [
                            'Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                            'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
                            'Lot Number', 'Style', 'Color', 'Size',
                            'Inventory Type', 'Actual Qty'
                        ]
                        # Rename inv_data columns to canonical names (case-insensitive match)
                        rename_map = {}
                        for canon in CANONICAL:
                            matched = inv_col_map_r.get(canon.strip().lower())
                            if matched and matched != canon:
                                rename_map[matched] = canon
                        if rename_map:
                            inv_data = inv_data.rename(columns=rename_map)
                        # Rebuild map after rename
                        inv_col_map_r = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

                        # Notepad headers order
                        REPORT_HEADERS = [
                            'Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                            'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
                            'Lot Number', 'Style', 'Color', 'Size', 'Client So 2',
                            'Inventory Type', 'Actual Qty'
                        ]

                        # Load Master_Pick_Data
                        mpd_df = get_safe_dataframe(sh, "Master_Pick_Data")
                        mpd_col = {str(c).strip().lower(): str(c).strip() for c in (mpd_df.columns if not mpd_df.empty else [])}

                        # Build pick qty per pallet
                        pick_qty_map = {}   # pallet → total pick qty
                        pick_country_map = {}  # pallet → country name
                        pick_loadid_map = {}   # pallet → Generated Load ID

                        if not mpd_df.empty:
                            p_col  = mpd_col.get('pallet','Pallet')
                            aq_col = mpd_col.get('actual qty','Actual Qty')
                            cn_col = mpd_col.get('country name','Country Name')
                            gl_col = mpd_col.get('generated load id','Generated Load ID')
                            for _, pr in mpd_df.iterrows():
                                pkey = str(pr.get(p_col,'')).strip()
                                if not pkey: continue
                                aq = pd.to_numeric(pr.get(aq_col,0), errors='coerce') or 0
                                pick_qty_map[pkey]     = pick_qty_map.get(pkey, 0) + aq
                                pick_country_map[pkey] = str(pr.get(cn_col,''))
                                pick_loadid_map[pkey]  = str(pr.get(gl_col,''))

                        # Load Damage_Items — build remark columns
                        dmg_df = get_safe_dataframe(sh, "Damage_Items")
                        # Distinct remarks → one column each
                        damage_remarks = []
                        dmg_pallet_remark_qty = {}  # (pallet, remark) → actual qty
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns and 'Remark' in dmg_df.columns:
                            for _, dr in dmg_df.iterrows():
                                pkey = str(dr.get('Pallet','')).strip()
                                rmk  = str(dr.get('Remark','Damage')).strip()
                                dqty = pd.to_numeric(dr.get('Actual Qty', 0), errors='coerce') or 0
                                if rmk not in damage_remarks:
                                    damage_remarks.append(rmk)
                                key = (pkey, rmk)
                                dmg_pallet_remark_qty[key] = dmg_pallet_remark_qty.get(key, 0) + dqty

                        # Load Master_Partial_Data for pallet replace
                        partial_df = get_safe_dataframe(sh, "Master_Partial_Data")
                        partial_map = {}  # orig_pallet → (gen_pallet_id, partial_qty) list
                        if not partial_df.empty:
                            pc = {str(c).strip().lower(): str(c).strip() for c in partial_df.columns}
                            pp_col  = pc.get('pallet','Pallet')
                            pq_col  = pc.get('partial qty','Partial Qty')
                            pg_col  = pc.get('gen pallet id','Gen Pallet ID')
                            pl_col  = pc.get('load id','Load ID')
                            for _, par in partial_df.iterrows():
                                opallet  = str(par.get(pp_col,'')).strip()
                                gpallet  = str(par.get(pg_col,'')).strip()
                                pqty     = pd.to_numeric(par.get(pq_col,0), errors='coerce') or 0
                                loadid_p = str(par.get(pl_col,'')).strip()
                                if opallet:
                                    if opallet not in partial_map:
                                        partial_map[opallet] = []
                                    partial_map[opallet].append({'gen_pallet': gpallet, 'partial_qty': pqty, 'load_id': loadid_p})

                        # Build sets of pallets in Damage_Items for ATS check
                        damage_pallets = set()
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            damage_pallets = set(str(p).strip() for p in dmg_df['Pallet'].dropna())

                        def build_row(inv_row, override_pallet=None, override_actual_qty=None):
                            """Build a report row from an inventory row with optional overrides."""
                            row = {}
                            for h in REPORT_HEADERS:
                                if h == 'Pallet':
                                    row[h] = override_pallet if override_pallet is not None else (
                                        inv_row['Pallet'] if 'Pallet' in inv_row.index else '')
                                elif h == 'Actual Qty':
                                    row[h] = override_actual_qty if override_actual_qty is not None else (
                                        inv_row['Actual Qty'] if 'Actual Qty' in inv_row.index else '')
                                elif h == 'Client So 2':
                                    cs_col = inv_col_map_r.get('client so', 'Client So')
                                    row[h] = inv_row[cs_col] if cs_col in inv_row.index else ''
                                elif h in inv_row.index:
                                    row[h] = inv_row[h]
                                else:
                                    fb = inv_col_map_r.get(h.strip().lower())
                                    row[h] = inv_row[fb] if fb and fb in inv_row.index else ''
                            return row

                        # Build formatted report rows
                        fmt_rows = []
                        for _, inv_row in inv_data.iterrows():
                            orig_pallet = str(inv_row.get('Pallet', '')).strip()

                            inv_actual_qty = pd.to_numeric(inv_row.get('Actual Qty', 0), errors='coerce')
                            if pd.isna(inv_actual_qty): inv_actual_qty = 0

                            is_damaged   = orig_pallet in damage_pallets
                            total_picked = pick_qty_map.get(orig_pallet, 0)

                            # Get partial entries for this pallet from Master_Partial_Data
                            partials = partial_map.get(orig_pallet, [])

                            if partials:
                                # --- Line separation for partial pallets ---
                                total_partial_qty = sum(p['partial_qty'] for p in partials)

                                # Line 1..N: one line per partial entry → Gen Pallet ID + Partial Qty
                                for par_entry in partials:
                                    row = build_row(inv_row,
                                                    override_pallet=par_entry['gen_pallet'],
                                                    override_actual_qty=par_entry['partial_qty'])
                                    row['Pick Quantity']       = par_entry['partial_qty']
                                    row['Destination Country'] = pick_country_map.get(orig_pallet, '')
                                    row['Order NO']            = par_entry['load_id']
                                    for rmk in damage_remarks:
                                        row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                    row['ATS'] = ''
                                    fmt_rows.append(row)

                                # Balance line: Original Pallet + (Actual Qty - total partial qty) → ATS
                                balance_qty = inv_actual_qty - total_partial_qty
                                if balance_qty > 0 and not is_damaged:
                                    bal_row = build_row(inv_row,
                                                        override_pallet=orig_pallet,
                                                        override_actual_qty=balance_qty)
                                    bal_row['Pick Quantity']       = ''
                                    bal_row['Destination Country'] = ''
                                    bal_row['Order NO']            = ''
                                    for rmk in damage_remarks:
                                        bal_row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                    bal_row['ATS'] = int(balance_qty)
                                    fmt_rows.append(bal_row)

                            else:
                                # --- No partial: single line ---
                                row = build_row(inv_row)
                                row['Pick Quantity']       = pick_qty_map.get(orig_pallet, '')
                                row['Destination Country'] = pick_country_map.get(orig_pallet, '')
                                row['Order NO']            = pick_loadid_map.get(orig_pallet, '')
                                for rmk in damage_remarks:
                                    row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')

                                # ATS = Actual Qty - Total Picked (only if not damage and > 0)
                                ats_qty   = inv_actual_qty - total_picked
                                row['ATS'] = int(ats_qty) if (not is_damaged and ats_qty > 0) else ''
                                fmt_rows.append(row)

                        # Build final column order
                        final_cols = REPORT_HEADERS.copy()
                        final_cols += ['Pick Quantity', 'Destination Country', 'Order NO']
                        final_cols += damage_remarks
                        final_cols += ['ATS']

                        fmt_df = pd.DataFrame(fmt_rows, columns=final_cols)

                        # ── Actual Qty Validation ──────────────────────────────────────
                        inv_total_qty = pd.to_numeric(inv_data['Actual Qty'], errors='coerce').fillna(0).sum()
                        rpt_total_qty = pd.to_numeric(fmt_df['Actual Qty'],   errors='coerce').fillna(0).sum()
                        qty_match     = abs(inv_total_qty - rpt_total_qty) < 0.01

                        # inv_pallet_qty: original inventory qty per pallet
                        inv_pallet_qty = (
                            inv_data.groupby('Pallet')['Actual Qty']
                            .apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum())
                            .to_dict()
                        )

                        # rpt_pallet_qty: report qty per pallet
                        # Gen Pallet IDs (e.g. PAL001-P0001) mapped back to original pallet
                        rpt_pallet_qty = {}
                        for _, rr in fmt_df.iterrows():
                            p      = str(rr.get('Pallet', '')).strip()
                            parts  = p.split('-P')
                            base_p = parts[0] if len(parts) >= 2 and parts[-1].isdigit() else p
                            # BUG FIX: explicit fillna before add — avoid NaN propagation via 'or 0'
                            val    = pd.to_numeric(rr.get('Actual Qty', 0), errors='coerce')
                            val    = 0.0 if pd.isna(val) else float(val)
                            rpt_pallet_qty[base_p] = rpt_pallet_qty.get(base_p, 0.0) + val

                        # ── Qty Mismatch Detection ─────────────────────────────────────
                        # Strategy: group inventory rows by base pallet (Gen Pallet IDs mapped
                        # back to original), sum total inv qty, compare to report total.
                        # This way T0503260046(3) + T0503260046-P0001(2) = 5 == report(5) → OK
                        # No dependency on Master_Partial_Data for this check.
                        import re as _re
                        _gen_pat = _re.compile(r'^(.+)-P(\d+)$')

                        def _base_pallet(p):
                            m = _gen_pat.match(str(p).strip())
                            return m.group(1) if m else str(p).strip()

                        mismatch_pallets = []
                        try:
                            # orig_total_inv: total inventory qty per base pallet
                            # (orig row + all Gen Pallet ID variant rows summed together)
                            orig_total_inv = {}
                            for _, inv_r in inv_data.iterrows():
                                p = str(inv_r.get('Pallet', '')).strip()
                                q = pd.to_numeric(inv_r.get('Actual Qty', 0), errors='coerce')
                                q = 0.0 if pd.isna(q) else float(q)
                                base = _base_pallet(p)
                                orig_total_inv[base] = orig_total_inv.get(base, 0.0) + q

                            # Compare: orig_total_inv vs rpt_pallet_qty per base pallet
                            all_pallets = set(orig_total_inv) | set(rpt_pallet_qty)
                            for pal in all_pallets:
                                inv_q = orig_total_inv.get(pal, 0.0)
                                rpt_q = rpt_pallet_qty.get(pal, 0.0)
                                if abs(inv_q - rpt_q) > 0.01:
                                    mismatch_pallets.append({
                                        'Pallet':               pal,
                                        'Inventory Actual Qty': inv_q,
                                        'Report Actual Qty':    rpt_q,
                                        'Difference':           inv_q - rpt_q,
                                    })

                        except Exception as _mm_err:
                            st.warning(f"⚠️ Mismatch check error: {_mm_err}")

                        # ── Summary — always renders regardless of mismatch errors ──────
                        total_pick_qty = pd.to_numeric(fmt_df['Pick Quantity'], errors='coerce').fillna(0).sum()
                        total_ats_qty  = pd.to_numeric(fmt_df['ATS'],           errors='coerce').fillna(0).sum()
                        total_dmg_qty  = sum(
                            pd.to_numeric(fmt_df[r], errors='coerce').fillna(0).sum()
                            for r in damage_remarks
                        )
                        total_lines   = len(fmt_df)
                        partial_lines = sum(1 for r in fmt_rows if r.get('Order NO', '') != '')

                        # Qty match banner
                        if qty_match:
                            st.success(f"✅ Actual Qty Match! Inventory: **{int(inv_total_qty)}** = Report: **{int(rpt_total_qty)}**")
                        else:
                            st.error(f"⚠️ Actual Qty Mismatch! Inventory: **{int(inv_total_qty)}** ≠ Report: **{int(rpt_total_qty)}** (diff: {int(inv_total_qty - rpt_total_qty)})")

                        # Summary metric cards
                        st.markdown("#### 📊 Report Summary")
                        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
                        sc1.metric("Total Lines",   total_lines)
                        sc2.metric("Pick Qty",      int(total_pick_qty))
                        sc3.metric("ATS Qty",       int(total_ats_qty))
                        sc4.metric("Damage Qty",    int(total_dmg_qty))
                        sc5.metric("Partial Lines", partial_lines)

                        # Reconciliation cross-check
                        accounted = total_pick_qty + total_ats_qty + total_dmg_qty
                        if abs(inv_total_qty - accounted) < 0.01:
                            st.success(f"✅ Qty Reconciled: Pick({int(total_pick_qty)}) + ATS({int(total_ats_qty)}) + Damage({int(total_dmg_qty)}) = {int(accounted)}")
                        else:
                            st.warning(f"⚠️ Unaccounted Qty: {int(inv_total_qty - accounted)} | Pick+ATS+Damage={int(accounted)} vs Inventory={int(inv_total_qty)}")

                        # Mismatch detail table
                        if mismatch_pallets:
                            st.markdown("#### 🔍 Pallet Qty Mismatch Details")
                            mm_df = pd.DataFrame(mismatch_pallets)
                            st.dataframe(mm_df, use_container_width=True)

                        st.dataframe(fmt_df.astype(str), use_container_width=True)

                        out_fmt = io.BytesIO()
                        with pd.ExcelWriter(out_fmt, engine='xlsxwriter') as writer:
                            fmt_df.to_excel(writer, sheet_name='Pick_Report', index=False)
                            wb = writer.book
                            ws_fmt = writer.sheets['Pick_Report']

                            # Summary sheet
                            ws_summ_sheet = wb.add_worksheet('Summary')
                            bold = wb.add_format({'bold': True, 'font_size': 11})
                            val_fmt = wb.add_format({'font_size': 11, 'num_format': '#,##0'})
                            ok_fmt  = wb.add_format({'bold': True, 'font_color': '#27ae60', 'font_size': 11})
                            err_fmt = wb.add_format({'bold': True, 'font_color': '#e74c3c', 'font_size': 11})

                            summary_rows = [
                                ('Inventory Total Actual Qty', int(inv_total_qty)),
                                ('Report Total Actual Qty',    int(rpt_total_qty)),
                                ('Qty Match',                  'YES ✅' if qty_match else 'NO ⚠️'),
                                ('', ''),
                                ('Pick Quantity',              int(total_pick_qty)),
                                ('ATS Quantity',               int(total_ats_qty)),
                                ('Damage Quantity',            int(total_dmg_qty)),
                                ('Total Accounted',            int(accounted)),
                                ('Unaccounted',                int(inv_total_qty - accounted)),
                                ('', ''),
                                ('Total Report Lines',         total_lines),
                                ('Partial Lines',              partial_lines),
                            ]
                            ws_summ_sheet.set_column(0, 0, 28)
                            ws_summ_sheet.set_column(1, 1, 18)
                            for ri, (label, value) in enumerate(summary_rows):
                                ws_summ_sheet.write(ri, 0, label, bold)
                                if isinstance(value, int):
                                    ws_summ_sheet.write(ri, 1, value, val_fmt)
                                elif 'YES' in str(value):
                                    ws_summ_sheet.write(ri, 1, value, ok_fmt)
                                elif 'NO' in str(value):
                                    ws_summ_sheet.write(ri, 1, value, err_fmt)
                                else:
                                    ws_summ_sheet.write(ri, 1, value)

                            # Mismatch sheet
                            if mismatch_pallets:
                                mm_sheet = wb.add_worksheet('Qty_Mismatch')
                                mm_df2 = pd.DataFrame(mismatch_pallets)
                                mm_hdr = wb.add_format({'bold': True, 'bg_color': '#e74c3c', 'font_color': '#fff'})
                                for ci, col in enumerate(mm_df2.columns):
                                    mm_sheet.write(0, ci, col, mm_hdr)
                                    mm_sheet.set_column(ci, ci, 22)
                                for ri2, row2 in mm_df2.iterrows():
                                    for ci2, val2 in enumerate(row2):
                                        mm_sheet.write(ri2+1, ci2, val2)

                            hdr_fmt = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#ffffff', 'border': 1, 'font_size': 10})
                            pick_col_fmt = wb.add_format({'bg_color': '#E8F5E9', 'border': 1, 'font_size': 10})
                            dmg_col_fmt  = wb.add_format({'bg_color': '#FFE0E0', 'border': 1, 'font_size': 10})
                            normal_fmt   = wb.add_format({'border': 1, 'font_size': 10})
                            for ci, col_name in enumerate(final_cols):
                                ws_fmt.write(0, ci, col_name, hdr_fmt)
                                ws_fmt.set_column(ci, ci, 15)
                                if col_name in ['Pick Quantity', 'Destination Country', 'Order NO']:
                                    for ri in range(1, len(fmt_df)+1):
                                        ws_fmt.write(ri, ci, str(fmt_df.iloc[ri-1][col_name]), pick_col_fmt)
                                elif col_name in damage_remarks:
                                    for ri in range(1, len(fmt_df)+1):
                                        ws_fmt.write(ri, ci, str(fmt_df.iloc[ri-1][col_name]), dmg_col_fmt)
                                elif col_name == 'ATS':
                                    ats_fmt = wb.add_format({'bg_color': '#E3F2FD', 'border': 1, 'font_size': 10, 'bold': True})
                                    for ri in range(1, len(fmt_df)+1):
                                        ws_fmt.write(ri, ci, str(fmt_df.iloc[ri-1][col_name]), ats_fmt)
                                else:
                                    for ri in range(1, len(fmt_df)+1):
                                        ws_fmt.write(ri, ci, str(fmt_df.iloc[ri-1][col_name]), normal_fmt)
                            ws_fmt.freeze_panes(1, 0)

                        st.download_button("⬇️ Download Formatted Pick Report",
                            data=out_fmt.getvalue(),
                            file_name=f"Pick_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel", use_container_width=True)
                        show_confetti()

    # ==========================================
    # TAB 4: REVERT / DELETE PICKS
    # ==========================================
    elif choice == "🔄 Revert/Delete Picks":
        st.title("🔄 Revert / Delete Picked Data")

        del_tab1, del_tab2, del_tab3, del_tab4 = st.tabs([
            "📁 Upload File to Delete",
            "🆔 Delete by Load ID Only",
            "🗂️ Delete by Batch ID",
            "📦 Delete by Pallet"
        ])

        # --- Option 1: Upload file ---
        with del_tab1:
            st.info("Load ID, Pallet සහ Actual Qty අඩංගු Excel හෝ CSV ගොනුවක් Upload කිරීමෙන් ගැලපෙන දත්ත **Master_Pick_Data** එකෙන් පමණක් මකා දැමිය හැක. Load_History record නොවෙනස්ව පවතී.")

            del_file = st.file_uploader("Upload Data to Delete", type=['csv', 'xlsx'], key="del_file_uploader")
            if del_file:
                if st.button("🗑️ Delete Matching Records", type="primary"):
                    with st.spinner("Deleting Data..."):
                        del_df = pd.read_csv(del_file) if del_file.name.endswith('.csv') else pd.read_excel(del_file)
                        del_df.columns = del_df.columns.str.strip().str.upper()

                        if not all(col in del_df.columns for col in ['LOAD ID', 'PALLET', 'ACTUAL QTY']):
                            st.error("Uploaded file must contain 'Load ID', 'Pallet', and 'Actual Qty' columns.")
                            st.stop()

                        master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")

                        if not master_pick_df.empty:
                            initial_len = len(master_pick_df)

                            temp_master = master_pick_df.copy()
                            temp_master.columns = temp_master.columns.str.strip().str.upper()

                            temp_master['MATCH_KEY'] = (
                                temp_master['LOAD ID'].astype(str).str.strip() + "_" +
                                temp_master['PALLET'].astype(str).str.strip() + "_" +
                                temp_master['ACTUAL QTY'].astype(float).astype(str)
                            )
                            del_df['MATCH_KEY'] = (
                                del_df['LOAD ID'].astype(str).str.strip() + "_" +
                                del_df['PALLET'].astype(str).str.strip() + "_" +
                                del_df['ACTUAL QTY'].astype(float).astype(str)
                            )

                            keys_to_delete = del_df['MATCH_KEY'].tolist()
                            filtered_master = master_pick_df[~temp_master['MATCH_KEY'].isin(keys_to_delete)]
                            deleted_count = initial_len - len(filtered_master)

                            if deleted_count > 0:
                                ws_pick = sh.worksheet("Master_Pick_Data")
                                ws_pick.clear()
                                ws_pick.append_row(MASTER_PICK_HEADERS)
                                if not filtered_master.empty:
                                    for col in MASTER_PICK_HEADERS:
                                        if col not in filtered_master.columns:
                                            filtered_master[col] = ''
                                    save_df = filtered_master[MASTER_PICK_HEADERS]
                                    ws_pick.append_rows(save_df.astype(str).replace('nan', '').values.tolist())

                                # ✅ Load_History DELETE කරන්නේ නැහැ — record history රකිනවා
                                st.success(f"✅ Master_Pick_Data එකෙන් records {deleted_count} ක් සාර්ථකව මකා දමන ලදී! (Load_History නොවෙනස්ව ඇත)")
                                show_confetti()
                            else:
                                st.warning("⚠️ Upload කල දත්ත හා ගැලපෙන වාර්තා Master_Pick_Data හි හමු නොවීය.")
                        else:
                            st.error("දැනට Master_Pick_Data හි දත්ත නොමැත.")

        # --- Option 2: Delete by Load ID only ---
        with del_tab2:
            st.info("Load ID එකක් ටයිප් කිරීමෙන් ඒ Load ID එකට අදාල සියලු data **Master_Pick_Data** එකෙන් පමණක් delete කළ හැක. Load_History record නොවෙනස්ව පවතී.")

            del_load_id = st.text_input("🆔 Enter Load ID to Delete:")

            if del_load_id:
                master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
                if not master_pick_df.empty and 'Load Id' in master_pick_df.columns:
                    preview = master_pick_df[master_pick_df['Load Id'].astype(str).str.strip() == del_load_id.strip()]
                    if not preview.empty:
                        st.warning(f"⚠️ Load ID **{del_load_id}** සඳහා Master_Pick_Data හි {len(preview)} records මකා දැමෙනු ඇත.")
                        st.dataframe(preview.astype(str), use_container_width=True)
                    else:
                        st.info(f"Load ID **{del_load_id}** සඳහා Master_Pick_Data හි records හමු නොවීය.")

                if st.button("🗑️ Delete by Load ID", type="primary"):
                    with st.spinner("Deleting..."):
                        master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
                        deleted_pick = 0

                        if not master_pick_df.empty and 'Load Id' in master_pick_df.columns:
                            filtered = master_pick_df[master_pick_df['Load Id'].astype(str).str.strip() != del_load_id.strip()]
                            deleted_pick = len(master_pick_df) - len(filtered)

                            ws_pick = sh.worksheet("Master_Pick_Data")
                            ws_pick.clear()
                            ws_pick.append_row(MASTER_PICK_HEADERS)
                            if not filtered.empty:
                                for col in MASTER_PICK_HEADERS:
                                    if col not in filtered.columns:
                                        filtered[col] = ''
                                save_df = filtered[MASTER_PICK_HEADERS]
                                ws_pick.append_rows(save_df.astype(str).replace('nan', '').values.tolist())

                        # ✅ Load_History DELETE කරන්නේ නැහැ — Load ID history රකිනවා
                        st.success(f"✅ Load ID **{del_load_id}** — Master_Pick_Data: {deleted_pick} records මකා දමන ලදී! (Load_History නොවෙනස්ව ඇත)")
                        show_confetti()

        # --- Option 3: Delete by Batch ID ---
        with del_tab3:
            st.info("Batch ID එකක් ගෙනහිර ඒ Batch ID එකට අදාල **සියලු** records **Master_Pick_Data** එකෙන් delete කළ හැක. Load_History නොවෙනස්ව පවතී.")

            # Load Batch IDs from Master_Pick_Data
            mpd_for_batch = get_safe_dataframe(sh, "Master_Pick_Data")
            batch_col_mpd = next((c for c in (mpd_for_batch.columns if not mpd_for_batch.empty else []) 
                                  if str(c).strip().lower() == 'batch id'), None)

            if not mpd_for_batch.empty and batch_col_mpd:
                available_batches_mpd = mpd_for_batch[batch_col_mpd].dropna().unique().tolist()
                available_batches_mpd = [b for b in available_batches_mpd if str(b).strip()]

                if available_batches_mpd:
                    del_batch_id = st.selectbox("🗂️ Select Batch ID to Delete:", available_batches_mpd, key="del_batch_sel")

                    if del_batch_id:
                        preview_batch = mpd_for_batch[mpd_for_batch[batch_col_mpd].astype(str).str.strip() == str(del_batch_id).strip()]
                        if not preview_batch.empty:
                            st.warning(f"⚠️ Batch ID **{del_batch_id}** හි **{len(preview_batch)}** records මකා දැමෙනු ඇත.")

                            # Show summary of what will be deleted
                            bc1, bc2, bc3 = st.columns(3)
                            bc1.metric("Records", len(preview_batch))
                            load_id_col_b = next((c for c in preview_batch.columns if str(c).strip().lower() == 'generated load id'), None)
                            if load_id_col_b:
                                bc2.metric("Load IDs", preview_batch[load_id_col_b].nunique())
                            aq_col_b = next((c for c in preview_batch.columns if str(c).strip().lower() == 'actual qty'), None)
                            if aq_col_b:
                                bc3.metric("Total Qty", int(pd.to_numeric(preview_batch[aq_col_b], errors='coerce').sum()))

                            st.dataframe(preview_batch[[c for c in ['Generated Load ID','Pallet','Actual Qty','SO Number','Country Name'] 
                                                         if c in preview_batch.columns]].astype(str).head(20), 
                                         use_container_width=True)

                        if st.button("🗑️ Delete by Batch ID", type="primary", key="del_batch_btn"):
                            with st.spinner("Deleting..."):
                                mpd_latest = get_safe_dataframe(sh, "Master_Pick_Data")
                                batch_col_latest = next((c for c in (mpd_latest.columns if not mpd_latest.empty else [])
                                                         if str(c).strip().lower() == 'batch id'), None)
                                deleted_batch = 0

                                if not mpd_latest.empty and batch_col_latest:
                                    filtered_batch = mpd_latest[
                                        mpd_latest[batch_col_latest].astype(str).str.strip() != str(del_batch_id).strip()
                                    ]
                                    deleted_batch = len(mpd_latest) - len(filtered_batch)

                                    ws_pick_b = sh.worksheet("Master_Pick_Data")
                                    ws_pick_b.clear()
                                    ws_pick_b.append_row(MASTER_PICK_HEADERS)
                                    if not filtered_batch.empty:
                                        for col in MASTER_PICK_HEADERS:
                                            if col not in filtered_batch.columns:
                                                filtered_batch[col] = ''
                                        ws_pick_b.append_rows(
                                            filtered_batch[MASTER_PICK_HEADERS].astype(str).replace('nan', '').values.tolist()
                                        )

                                # Load_History නොවෙනස්ව
                                st.success(f"✅ Batch ID **{del_batch_id}** — Master_Pick_Data: {deleted_batch} records මකා දමන ලදී! (Load_History නොවෙනස්ව ඇත)")
                                show_confetti()
                else:
                    st.info("Master_Pick_Data හි Batch IDs නොමැත.")
            else:
                st.info("Master_Pick_Data හි data නොමැත.")

        # --- Option 4: Delete by Pallet ---
        with del_tab4:
            st.info("Pallet ID එකක් දීමෙන් ඒ Pallet හා සම්බන්ධ **සියලු data** — Master_Pick_Data, Master_Partial_Data, Damage_Items — වලින් delete කළ හැක. Load_History නොවෙනස්ව පවතී.")

            # Load all pallets from Master_Pick_Data + Master_Partial_Data + Damage_Items
            _mpd_pal   = get_safe_dataframe(sh, "Master_Pick_Data")
            _mpart_pal = get_safe_dataframe(sh, "Master_Partial_Data")
            _dmg_pal   = get_safe_dataframe(sh, "Damage_Items")

            all_pallets_set = set()
            for _df, _col in [(_mpd_pal, 'Pallet'), (_mpart_pal, 'Pallet'), (_dmg_pal, 'Pallet')]:
                if not _df.empty and _col in _df.columns:
                    all_pallets_set.update(_df[_col].dropna().astype(str).str.strip().tolist())
            # Also include Gen Pallet IDs from Master_Partial_Data
            if not _mpart_pal.empty and 'Gen Pallet ID' in _mpart_pal.columns:
                all_pallets_set.update(_mpart_pal['Gen Pallet ID'].dropna().astype(str).str.strip().tolist())

            all_pallets_list = sorted([p for p in all_pallets_set if p])

            if all_pallets_list:
                del_pallet = st.selectbox("📦 Select Pallet to Delete:", all_pallets_list, key="del_pallet_sel")

                if del_pallet:
                    # Preview counts across all sheets
                    def _count_rows(df, col, val):
                        if df.empty or col not in df.columns:
                            return 0
                        return len(df[df[col].astype(str).str.strip() == str(val).strip()])

                    mpd_count   = _count_rows(_mpd_pal, 'Pallet', del_pallet)
                    mpart_count = _count_rows(_mpart_pal, 'Pallet', del_pallet)
                    # Also count Gen Pallet ID matches in partial data
                    mpart_gen_count = _count_rows(_mpart_pal, 'Gen Pallet ID', del_pallet) if not _mpart_pal.empty and 'Gen Pallet ID' in _mpart_pal.columns else 0
                    dmg_count   = _count_rows(_dmg_pal, 'Pallet', del_pallet)

                    st.warning(f"⚠️ Pallet **{del_pallet}** හා සම්බන්ධ records:")
                    pc1, pc2, pc3 = st.columns(3)
                    pc1.metric("Master_Pick_Data",    mpd_count)
                    pc2.metric("Master_Partial_Data", mpart_count + mpart_gen_count)
                    pc3.metric("Damage_Items",        dmg_count)

                    if st.button("🗑️ Delete Pallet from All Sheets", type="primary", key="del_pallet_btn"):
                        with st.spinner("Deleting..."):
                            results = []

                            # 1. Master_Pick_Data
                            mpd_fresh = get_safe_dataframe(sh, "Master_Pick_Data")
                            if not mpd_fresh.empty and 'Pallet' in mpd_fresh.columns:
                                filtered_mpd = mpd_fresh[
                                    mpd_fresh['Pallet'].astype(str).str.strip() != str(del_pallet).strip()
                                ]
                                deleted = len(mpd_fresh) - len(filtered_mpd)
                                APIManager.overwrite_sheet(sh, "Master_Pick_Data", MASTER_PICK_HEADERS, filtered_mpd)
                                results.append(f"Master_Pick_Data: {deleted} records")

                            # 2. Master_Partial_Data — Pallet column AND Gen Pallet ID column
                            mpart_fresh = get_safe_dataframe(sh, "Master_Partial_Data")
                            mpd_hdr = SHEET_HEADERS["Master_Partial_Data"]
                            if not mpart_fresh.empty:
                                mask = pd.Series([True] * len(mpart_fresh))
                                if 'Pallet' in mpart_fresh.columns:
                                    mask &= mpart_fresh['Pallet'].astype(str).str.strip() != str(del_pallet).strip()
                                if 'Gen Pallet ID' in mpart_fresh.columns:
                                    mask &= mpart_fresh['Gen Pallet ID'].astype(str).str.strip() != str(del_pallet).strip()
                                filtered_mpart = mpart_fresh[mask]
                                deleted = len(mpart_fresh) - len(filtered_mpart)
                                APIManager.overwrite_sheet(sh, "Master_Partial_Data", mpd_hdr, filtered_mpart)
                                results.append(f"Master_Partial_Data: {deleted} records")

                            # 3. Damage_Items
                            dmg_fresh = get_safe_dataframe(sh, "Damage_Items")
                            if not dmg_fresh.empty and 'Pallet' in dmg_fresh.columns:
                                filtered_dmg = dmg_fresh[
                                    dmg_fresh['Pallet'].astype(str).str.strip() != str(del_pallet).strip()
                                ]
                                deleted = len(dmg_fresh) - len(filtered_dmg)
                                APIManager.overwrite_sheet(sh, "Damage_Items", SHEET_HEADERS["Damage_Items"], filtered_dmg)
                                results.append(f"Damage_Items: {deleted} records")

                            st.success(f"✅ Pallet **{del_pallet}** deleted — {' | '.join(results)}")
                            show_confetti()
            else:
                st.info("Delete කළ හැකි Pallets නොමැත.")

    # ==========================================
    # TAB 5: DAMAGE ITEMS
    # ==========================================
    elif choice == "🩹 Damage Items":
        st.title("🩹 Damage Items Management")
        st.info("Damage, defective හෝ unavailable items මෙහි Pallet/Actual Qty/Remark සහිතව upload කරන්න. මෙම Pallets pick operations වලින් automatically exclude වේ.")

        dmg_tab1, dmg_tab2 = st.tabs(["📤 Upload Damage Items", "📋 View Damage Records"])

        with dmg_tab1:
            st.subheader("Upload Damage Items File")
            st.caption("File එකේ Pallet, Actual Qty, Remark columns තිබිය යුතුය.")

            dmg_file = st.file_uploader("Upload Damage Items (CSV/Excel)", type=['csv', 'xlsx'], key="dmg_uploader")

            if dmg_file:
                dmg_preview = pd.read_csv(dmg_file) if dmg_file.name.endswith('.csv') else pd.read_excel(dmg_file)
                st.dataframe(dmg_preview.astype(str), use_container_width=True)

                if st.button("💾 Save Damage Items", type="primary"):
                    with st.spinner("Saving Damage Items..."):
                        dmg_cols = dmg_preview.columns.str.strip().str.lower()

                        pallet_col = next((c for c in dmg_preview.columns if 'pallet' in c.lower()), None)
                        qty_col = next((c for c in dmg_preview.columns if 'actual qty' in c.lower() or 'qty' in c.lower()), None)
                        remark_col = next((c for c in dmg_preview.columns if 'remark' in c.lower()), None)

                        if not pallet_col or not qty_col:
                            st.error("File එකේ අවම වශයෙන් 'Pallet' සහ 'Actual Qty' columns තිබිය යුතුය.")
                        else:
                            ws_dmg = get_or_create_sheet(sh, "Damage_Items", SHEET_HEADERS["Damage_Items"])

                            rows_to_add = []
                            for _, row in dmg_preview.iterrows():
                                rows_to_add.append([
                                    str(row.get(pallet_col, '')),
                                    str(row.get(qty_col, '')),
                                    str(row.get(remark_col, '')) if remark_col else 'Damage',
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    current_user
                                ])

                            ws_dmg.append_rows(rows_to_add)
                            st.success(f"✅ Damage Items {len(rows_to_add)} ක් සාර්ථකව save කරන ලදී! මෙම Pallets pick operations වලින් exclude වේ.")
                            show_confetti()

        with dmg_tab2:
            st.subheader("Damage Items Records")
            dmg_df = get_safe_dataframe(sh, "Damage_Items")
            if dmg_df.empty:
                st.info("Damage Items records නොමැත.")
            else:
                st.metric("Total Damage Records", len(dmg_df))
                st.dataframe(dmg_df.astype(str), use_container_width=True)

                # Download damage records
                out_dmg = io.BytesIO()
                with pd.ExcelWriter(out_dmg, engine='xlsxwriter') as writer:
                    dmg_df.to_excel(writer, sheet_name='Damage_Items', index=False)
                st.download_button(
                    "⬇️ Download Damage Records",
                    data=out_dmg.getvalue(),
                    file_name=f"Damage_Items_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.ms-excel"
                )

                # Option to clear a specific damage record by pallet
                st.divider()
                st.subheader("🗑️ Remove Damage Record")
                if 'Pallet' in dmg_df.columns:
                    remove_pallet = st.selectbox("Select Pallet to Remove from Damage List:", dmg_df['Pallet'].dropna().unique())
                    if st.button("Remove Damage Record"):
                        filtered_dmg = dmg_df[dmg_df['Pallet'].astype(str) != str(remove_pallet)]
                        ws_dmg = sh.worksheet("Damage_Items")
                        ws_dmg.clear()
                        ws_dmg.append_row(SHEET_HEADERS["Damage_Items"])
                        if not filtered_dmg.empty:
                            ws_dmg.append_rows(filtered_dmg.astype(str).replace('nan', '').values.tolist())
                        st.success(f"✅ Pallet **{remove_pallet}** Damage list එකෙන් ඉවත් කරන ලදී.")
                        st.rerun()

    # ==========================================
    # TAB 6: ADMIN SETTINGS
    # ==========================================
    elif choice == "⚙️ Admin Settings":
        st.title("⚙️ System Administration")

        col_adm1, col_adm2 = st.columns(2)

        with col_adm1:
            st.subheader("👥 Add New User")
            with st.form("add_user_form"):
                n_user = st.text_input("New Username")
                n_pass = st.text_input("New Password", type="password")
                n_role = st.selectbox("Role", ["user", "SysUser", "admin"])
                submitted = st.form_submit_button("Add User")
                if submitted and n_user and n_pass:
                    ws_users = sh.worksheet("Users")
                    users_data = get_safe_dataframe(sh, "Users")
                    if not users_data.empty and n_user in users_data['Username'].values:
                        st.error("මෙම Username එක දැනටමත් ඇත.")
                    else:
                        ws_users.append_row([n_user, n_pass, n_role])
                        st.success("User සාර්ථකව ඇතුලත් කරන ලදී!")

        with col_adm2:
            st.subheader("⚠️ Database Management")
            st.warning("මෙමඟින් පද්ධතියේ පරණ දත්ත සහ Headers සම්පූර්ණයෙන්ම මකා දමයි. (Clear Database)")

            sheet_to_clear = st.selectbox("Select Data to Clear:", [
                "Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History", "Damage_Items", "ALL_DATA"
            ])

            confirm = st.text_input("Type 'CONFIRM' to proceed:")
            if st.button("🗑️ Clear Selected Data", type="primary"):
                if confirm == 'CONFIRM':
                    try:
                        sheets_to_process = [
                            "Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History", "Damage_Items"
                        ] if sheet_to_clear == "ALL_DATA" else [sheet_to_clear]

                        for s_name in sheets_to_process:
                            try:
                                ws = sh.worksheet(s_name)
                                ws.clear()
                                if s_name in SHEET_HEADERS:
                                    ws.append_row(SHEET_HEADERS[s_name])
                            except:
                                pass
                        st.success(f"✅ {sheet_to_clear} සාර්ථකව Reset කර Headers අලුතින් ඇතුලත් කරන ලදී.")
                    except Exception as e:
                        st.error(f"Error clearing data: {e}")
                else:
                    st.error("කරුණාකර CONFIRM ලෙස Type කරන්න.")

footer_branding()
