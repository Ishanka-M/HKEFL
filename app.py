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

WMS_FIELDS = ['Batch ID', 'SO Number', 'Generated Load ID', 'Country Name', 'Pick Quantity', 'Remark']
MASTER_PICK_HEADERS = INVENTORY_HEADERS + WMS_FIELDS
HEADER_LOWER_MAP = {h.strip().lower(): h for h in MASTER_PICK_HEADERS}

SHEET_HEADERS = {
    "Load_History": ['Batch ID', 'Generated Load ID', 'SO Number', 'Country Name', 'SHIP MODE', 'Date', 'Pick Status'],
    "Summary_Data": ['Batch ID', 'SO Number', 'Load ID', 'UPC', 'Country', 'Ship Mode', 'Requested', 'Picked', 'Variance', 'Status'],
    "Master_Partial_Data": ['Batch ID', 'SO Number', 'Pallet', 'Supplier', 'Load ID', 'Country Name',
                             'Actual Qty', 'Partial Qty', 'Gen Pallet ID', 'Balance Qty',
                             'Location Id', 'Lot Number', 'Color', 'Size', 'Style', 'Customer Po Number',
                             'Vendor Name', 'Invoice Number', 'Grn Number'],
    "Master_Pick_Data": MASTER_PICK_HEADERS,
    "Damage_Items": ['Pallet', 'Actual Qty', 'Remark', 'Date Added', 'Added By'],
    "Vendor_Maintain": ['Vendor Name', 'Country'],
}

# Formatted Pick Report column headers (Notepad order)
REPORT_HEADERS = [
    'Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
    'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
    'Lot Number', 'Style', 'Color', 'Size', 'Client So 2',
    'Inventory Type', 'Actual Qty'
]


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
    return APIManager.get_workbook(retries=retries, delay=delay)


class APIManager:
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
        now = time.time()
        if cls._wb_cache is not None and (now - cls._wb_ts) < cls._wb_ttl:
            return cls._wb_cache
        for attempt in range(retries):
            try:
                cls._throttle()
                client = get_gsheet_client()
                wb = client.open(st.secrets["general"]["spreadsheet_name"])
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
                        st.error("❌ Spreadsheet not found (404). spreadsheet_name correct දැයි confirm කරන්න.")
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
        return {name: cls.read_sheet(sh, name, force=force) for name in sheet_names}

    @classmethod
    def get_or_create_ws(cls, sh, name, headers, retries=3):
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

    st.sidebar.markdown("""
    <div style="text-align:center; padding:12px 0 14px 0; border-bottom:1px solid #eee; margin-bottom:8px;">
        <div style="font-family:'Georgia',serif; font-size:14px; font-weight:800; letter-spacing:3px; color:#1a1a1a;">HELEN KAMINSKI</div>
        <div style="font-size:9px; letter-spacing:2px; color:#999; margin-top:2px;">PICK MANAGEMENT</div>
    </div>
    """, unsafe_allow_html=True)
    return True


# --- 3. Inventory Logic ---
def get_damage_pallets(sh):
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
    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_lower = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    pallet_col = inv_col_lower.get('pallet', 'Pallet')
    actual_col = inv_col_lower.get('actual qty', 'Actual Qty')

    if actual_col not in inv_df.columns:
        actual_col = next((c for c in inv_df.columns if 'actual' in c.lower()), actual_col)

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)

    try:
        pick_history = get_safe_dataframe(sh, "Master_Pick_Data")
        if not pick_history.empty and 'Actual Qty' in pick_history.columns and 'Pallet' in pick_history.columns:
            pick_history['Actual Qty'] = pd.to_numeric(pick_history['Actual Qty'], errors='coerce').fillna(0)
            pick_history['Pallet'] = pick_history['Pallet'].astype(str).str.strip()
            pick_summary = pick_history.groupby('Pallet')['Actual Qty'].sum().reset_index()
            pick_summary.columns = ['_pallet_key', 'Total_Picked']

            inv_df['_pallet_key'] = inv_df[pallet_col].astype(str).str.strip()
            inv_df = pd.merge(inv_df, pick_summary, on='_pallet_key', how='left')
            inv_df = inv_df.drop(columns=['_pallet_key'], errors='ignore')
            inv_df['Total_Picked'] = inv_df['Total_Picked'].fillna(0)
            inv_df[actual_col] = (inv_df[actual_col] - inv_df['Total_Picked']).clip(lower=0)
            inv_df = inv_df.drop(columns=['Total_Picked'], errors='ignore')
    except Exception as e:
        st.warning(f"Inventory Reconcile Error: {e}")

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


def process_picking(inv_df, req_df, batch_id, sh=None, inv_original=None):
    pick_rows, partial_rows, summary = [], [], []

    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    supplier_col = next((inv_col_map[k] for k in inv_col_map if k == 'supplier'), None)
    pick_id_col = next((inv_col_map[k] for k in inv_col_map if k in ('pick id', 'pickid')), None)
    pallet_col = next((inv_col_map[k] for k in inv_col_map if k == 'pallet'), 'Pallet')

    temp_inv = inv_df.copy()
    actual_qty_col = next((inv_col_map[k] for k in inv_col_map if k == 'actual qty'), 'Actual Qty')
    temp_inv[actual_qty_col] = pd.to_numeric(temp_inv[actual_qty_col], errors='coerce').fillna(0)
    temp_inv = temp_inv[temp_inv[actual_qty_col] > 0].reset_index(drop=True)

    orig_qty_map = {}
    if inv_original is not None:
        inv_orig_norm = inv_original.copy()
        inv_orig_norm.columns = [str(c).strip() for c in inv_orig_norm.columns]
        orig_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_orig_norm.columns}
        orig_pallet_col = orig_col_map.get('pallet', 'Pallet')
        orig_actual_col = orig_col_map.get('actual qty', 'Actual Qty')
        if orig_pallet_col in inv_orig_norm.columns and orig_actual_col in inv_orig_norm.columns:
            inv_orig_norm[orig_actual_col] = pd.to_numeric(inv_orig_norm[orig_actual_col], errors='coerce').fillna(0)
            for _, orig_row in inv_orig_norm.iterrows():
                p = str(orig_row[orig_pallet_col]).strip()
                q = float(orig_row[orig_actual_col])
                orig_qty_map[p] = q

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
        key = str(master_header).strip().lower()
        if master_header in item.index:
            return item[master_header]
        orig = inv_col_map.get(key)
        if orig and orig in item.index:
            return item[orig]
        return ''

    for lid in req_df['Generated Load ID'].unique():
        current_reqs = req_df[req_df['Generated Load ID'] == lid]
        so_num = str(current_reqs['SO Number'].iloc[0])
        ship_mode = str(current_reqs['SHIP MODE: (SEA/AIR)'].iloc[0]) if 'SHIP MODE: (SEA/AIR)' in current_reqs.columns else ""

        for _, req in current_reqs.iterrows():
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
                    p_row = {}
                    for header in MASTER_PICK_HEADERS:
                        p_row[header] = get_inv_val(item, header)

                    p_row['Actual Qty'] = take
                    p_row['Pick Quantity'] = take
                    p_row['Pick Id'] = str(item[pick_id_col]) if pick_id_col and pick_id_col in item.index else ''
                    p_row['Supplier'] = str(item[supplier_col]) if supplier_col and supplier_col in item.index else upc
                    p_row['Batch ID'] = batch_id
                    p_row['SO Number'] = so_num
                    p_row['Generated Load ID'] = lid
                    p_row['Country Name'] = country
                    p_row['Remark'] = ''
                    p_row['Order Type'] = 'Pick Orders'
                    p_row['Order Number'] = lid
                    p_row['Store Order Number'] = lid
                    p_row['Customer Po Number'] = f"{country}-{lid}"
                    p_row['Load Id'] = lid

                    pick_rows.append(p_row)

                    pallet_val = str(item[pallet_col]) if pallet_col in item.index else ''
                    orig_qty = orig_qty_map.get(pallet_val, current_avail)
                    is_partial = (take < current_avail) or (orig_qty > take)

                    if is_partial:
                        def _get(col_name):
                            c = inv_col_map.get(col_name.lower())
                            return str(item[c]) if c and c in item.index else ''

                        partial_rows.append({
                            'Batch ID': batch_id,
                            'SO Number': so_num,
                            'Pallet': pallet_val,
                            'Supplier': p_row['Supplier'],
                            'Load ID': lid,
                            'Country Name': country,
                            'Actual Qty': orig_qty,
                            'Partial Qty': take,
                            'Gen Pallet ID': make_unique_gen_pallet_id(pallet_val),
                            'Balance Qty': orig_qty - take,
                            'Location Id': _get('location id'),
                            'Lot Number': _get('lot number'),
                            'Color': _get('color'),
                            'Size': _get('size'),
                            'Style': _get('style'),
                            'Customer Po Number': _get('customer po number'),
                            'Vendor Name': _get('vendor name'),
                            'Invoice Number': _get('invoice number'),
                            'Grn Number': _get('grn number'),
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

    if pick_rows:
        pick_df = pd.DataFrame(pick_rows, columns=MASTER_PICK_HEADERS)
        pick_df['Actual Qty'] = pd.to_numeric(pick_df['Actual Qty'], errors='coerce').fillna(0)
        pick_df = pick_df[pick_df['Actual Qty'] > 0].reset_index(drop=True)
    else:
        pick_df = pd.DataFrame(columns=MASTER_PICK_HEADERS)

    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)


# ==========================================
# FORMATTED PICK REPORT GENERATION (NEW LOGIC)
# ==========================================

def generate_formatted_pick_report(inv_data, sh):
    """
    New Formatted Pick Report logic:

    For each inventory Pallet:
    1. Check master_pick_data by Pallet + Actual Qty
    2. If Pallet NOT in DB → add to report directly, set ATS = Actual Qty
    3. If Pallet in DB AND Actual Qty SAME → Pick Quantity = Actual Qty, Destination Country + Order NO from DB
    4. If Pallet in DB AND Actual Qty DIFFERENT → check master_partial_data:
       - For each matching gen_pallet_id line → add a report row with gen_pallet_id as Pallet,
         partial_qty as Actual Qty + Pick Quantity, country_name/load_id as Destination/Order NO,
         vendor_name/invoice_number/grn_number from master_partial_data
       - Balance (inv Actual Qty - sum partial_qty) → add original pallet row with ATS = balance
       - Validate: inv Actual Qty >= sum of partial_qtys
    5. COO column: lookup vendor_name in vendor_maintain → country
    6. Damage columns remain as-is from Damage_Items
    """

    inv_data = inv_data.copy()
    inv_data.columns = [str(c).strip() for c in inv_data.columns]
    inv_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

    # Rename to canonical
    CANONICAL = [
        'Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
        'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
        'Lot Number', 'Style', 'Color', 'Size', 'Inventory Type', 'Actual Qty'
    ]
    rename_map = {}
    for canon in CANONICAL:
        matched = inv_col_map.get(canon.strip().lower())
        if matched and matched != canon:
            rename_map[matched] = canon
    if rename_map:
        inv_data = inv_data.rename(columns=rename_map)
    inv_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

    # Load DB tables
    mpd_df = get_safe_dataframe(sh, "Master_Pick_Data")         # master_pick_data
    mpart_df = get_safe_dataframe(sh, "Master_Partial_Data")    # master_partial_data
    dmg_df = get_safe_dataframe(sh, "Damage_Items")             # damage items
    vendor_df = get_safe_dataframe(sh, "Vendor_Maintain")       # vendor_maintain (COO)

    # --- Build Master_Pick_Data lookup: pallet → {actual_qty, country_name, generated_load_id} ---
    mpd_pallet_map = {}  # pallet_str → {'actual_qty': float, 'country': str, 'order_no': str}
    if not mpd_df.empty:
        mpd_c = {str(c).strip().lower(): str(c).strip() for c in mpd_df.columns}
        p_col = mpd_c.get('pallet', 'Pallet')
        aq_col = mpd_c.get('actual qty', 'Actual Qty')
        cn_col = mpd_c.get('country name', 'Country Name')
        gl_col = mpd_c.get('generated load id', 'Generated Load ID')

        for _, pr in mpd_df.iterrows():
            pkey = str(pr.get(p_col, '')).strip()
            if not pkey:
                continue
            aq = pd.to_numeric(pr.get(aq_col, 0), errors='coerce') or 0
            existing = mpd_pallet_map.get(pkey)
            if existing:
                existing['actual_qty'] += aq
                # Keep first country/order_no
            else:
                mpd_pallet_map[pkey] = {
                    'actual_qty': aq,
                    'country': str(pr.get(cn_col, '')),
                    'order_no': str(pr.get(gl_col, '')),
                }

    # --- Build Master_Partial_Data lookup: pallet → list of partial entries ---
    mpart_map = {}  # orig_pallet → list of {gen_pallet_id, partial_qty, country, order_no, vendor_name, invoice_number, grn_number}
    if not mpart_df.empty:
        mpc = {str(c).strip().lower(): str(c).strip() for c in mpart_df.columns}
        pp_col = mpc.get('pallet', 'Pallet')
        pg_col = mpc.get('gen pallet id', 'Gen Pallet ID')
        pq_col = mpc.get('partial qty', 'Partial Qty')
        pl_col = mpc.get('load id', 'Load ID')
        pcn_col = mpc.get('country name', 'Country Name')
        pvn_col = mpc.get('vendor name', 'Vendor Name')
        pin_col = mpc.get('invoice number', 'Invoice Number')
        pgn_col = mpc.get('grn number', 'Grn Number')

        for _, par in mpart_df.iterrows():
            opallet = str(par.get(pp_col, '')).strip()
            gpallet = str(par.get(pg_col, '')).strip()
            pqty = pd.to_numeric(par.get(pq_col, 0), errors='coerce') or 0
            if opallet:
                if opallet not in mpart_map:
                    mpart_map[opallet] = []
                mpart_map[opallet].append({
                    'gen_pallet_id': gpallet,
                    'partial_qty': pqty,
                    'order_no': str(par.get(pl_col, '')),
                    'country': str(par.get(pcn_col, '')),
                    'vendor_name': str(par.get(pvn_col, '')),
                    'invoice_number': str(par.get(pin_col, '')),
                    'grn_number': str(par.get(pgn_col, '')),
                })

    # --- Build Damage lookup: pallet → {remark_col: qty} ---
    damage_remarks = []
    dmg_pallet_remark_qty = {}
    damage_pallets_set = set()
    if not dmg_df.empty and 'Pallet' in dmg_df.columns:
        damage_pallets_set = set(str(p).strip() for p in dmg_df['Pallet'].dropna())
        if 'Remark' in dmg_df.columns:
            for _, dr in dmg_df.iterrows():
                pkey = str(dr.get('Pallet', '')).strip()
                rmk = str(dr.get('Remark', 'Damage')).strip()
                dqty = pd.to_numeric(dr.get('Actual Qty', 0), errors='coerce') or 0
                if rmk not in damage_remarks:
                    damage_remarks.append(rmk)
                key = (pkey, rmk)
                dmg_pallet_remark_qty[key] = dmg_pallet_remark_qty.get(key, 0) + dqty

    # --- Build Vendor_Maintain COO lookup: vendor_name → country ---
    vendor_coo_map = {}  # vendor_name (lower) → country
    if not vendor_df.empty:
        vc = {str(c).strip().lower(): str(c).strip() for c in vendor_df.columns}
        vn_col = vc.get('vendor name', 'Vendor Name')
        vc_col = vc.get('country', 'Country')
        if vn_col in vendor_df.columns and vc_col in vendor_df.columns:
            for _, vr in vendor_df.iterrows():
                vname = str(vr.get(vn_col, '')).strip().lower()
                vcountry = str(vr.get(vc_col, '')).strip()
                if vname:
                    vendor_coo_map[vname] = vcountry

    def get_coo(vendor_name):
        """Lookup COO from vendor_maintain by vendor_name."""
        return vendor_coo_map.get(str(vendor_name).strip().lower(), '')

    def build_base_row(inv_row, override_pallet=None, override_actual_qty=None,
                       override_vendor=None, override_invoice=None, override_grn=None):
        """Build a report base row from inventory row."""
        row = {}
        for h in REPORT_HEADERS:
            if h == 'Pallet':
                row[h] = override_pallet if override_pallet is not None else (
                    inv_row.get('Pallet', ''))
            elif h == 'Actual Qty':
                row[h] = override_actual_qty if override_actual_qty is not None else (
                    inv_row.get('Actual Qty', ''))
            elif h == 'Vendor Name':
                row[h] = override_vendor if override_vendor is not None else inv_row.get('Vendor Name', '')
            elif h == 'Invoice Number':
                row[h] = override_invoice if override_invoice is not None else inv_row.get('Invoice Number', '')
            elif h == 'Grn Number':
                row[h] = override_grn if override_grn is not None else inv_row.get('Grn Number', '')
            elif h == 'Client So 2':
                cs_col = inv_col_map.get('client so', 'Client So')
                row[h] = inv_row.get(cs_col, '') if cs_col in (inv_data.columns if hasattr(inv_data, 'columns') else []) else ''
            elif h in inv_row.index:
                row[h] = inv_row[h]
            else:
                fb = inv_col_map.get(h.strip().lower())
                row[h] = inv_row[fb] if fb and fb in inv_row.index else ''
        return row

    fmt_rows = []
    validation_issues = []

    for _, inv_row in inv_data.iterrows():
        orig_pallet = str(inv_row.get('Pallet', '')).strip()
        inv_actual_qty = pd.to_numeric(inv_row.get('Actual Qty', 0), errors='coerce')
        if pd.isna(inv_actual_qty):
            inv_actual_qty = 0.0
        inv_actual_qty = float(inv_actual_qty)

        vendor_name_inv = str(inv_row.get('Vendor Name', '')).strip()
        is_damaged = orig_pallet in damage_pallets_set

        # --- Damage columns for this pallet ---
        dmg_cols_data = {rmk: dmg_pallet_remark_qty.get((orig_pallet, rmk), '') for rmk in damage_remarks}

        # =====================================================
        # CASE 1: Pallet NOT in master_pick_data DB
        # =====================================================
        if orig_pallet not in mpd_pallet_map:
            row = build_base_row(inv_row)
            row['Pick Quantity'] = ''
            row['Destination Country'] = ''
            row['Order NO'] = ''
            row['ATS'] = int(inv_actual_qty) if (not is_damaged and inv_actual_qty > 0) else ''
            row['COO'] = get_coo(vendor_name_inv)
            for rmk, val in dmg_cols_data.items():
                row[rmk] = val
            fmt_rows.append(row)

        else:
            db_entry = mpd_pallet_map[orig_pallet]
            db_actual_qty = db_entry['actual_qty']

            # =====================================================
            # CASE 2: Pallet in DB AND Actual Qty SAME → full pick
            # =====================================================
            if abs(db_actual_qty - inv_actual_qty) < 0.01:
                row = build_base_row(inv_row)
                row['Pick Quantity'] = inv_actual_qty
                row['Destination Country'] = db_entry['country']
                row['Order NO'] = db_entry['order_no']
                row['ATS'] = ''
                row['COO'] = get_coo(vendor_name_inv)
                for rmk, val in dmg_cols_data.items():
                    row[rmk] = val
                fmt_rows.append(row)

            # =====================================================
            # CASE 3: Pallet in DB BUT Actual Qty DIFFERENT → partial
            # =====================================================
            else:
                partials = mpart_map.get(orig_pallet, [])
                if partials:
                    total_partial_qty = sum(p['partial_qty'] for p in partials)

                    # Validation: total partial_qty must NOT exceed inv_actual_qty
                    if total_partial_qty > inv_actual_qty + 0.01:
                        validation_issues.append({
                            'Pallet': orig_pallet,
                            'Inv Actual Qty': inv_actual_qty,
                            'Total Partial Qty': total_partial_qty,
                            'Issue': f'Partial Qty ({total_partial_qty}) exceeds Inventory Actual Qty ({inv_actual_qty})'
                        })

                    # Add one row per gen_pallet_id line
                    for par_entry in partials:
                        row = build_base_row(
                            inv_row,
                            override_pallet=par_entry['gen_pallet_id'],
                            override_actual_qty=par_entry['partial_qty'],
                            override_vendor=par_entry['vendor_name'] if par_entry['vendor_name'] else vendor_name_inv,
                            override_invoice=par_entry['invoice_number'] if par_entry['invoice_number'] else inv_row.get('Invoice Number', ''),
                            override_grn=par_entry['grn_number'] if par_entry['grn_number'] else inv_row.get('Grn Number', ''),
                        )
                        row['Pick Quantity'] = par_entry['partial_qty']
                        row['Destination Country'] = par_entry['country']
                        row['Order NO'] = par_entry['order_no']
                        row['ATS'] = ''
                        row['COO'] = get_coo(par_entry['vendor_name'] if par_entry['vendor_name'] else vendor_name_inv)
                        for rmk, val in dmg_cols_data.items():
                            row[rmk] = val
                        fmt_rows.append(row)

                    # Balance row: original pallet + (inv_actual_qty - sum partial_qty) → ATS
                    balance_qty = inv_actual_qty - total_partial_qty
                    if balance_qty > 0.01 and not is_damaged:
                        bal_row = build_base_row(inv_row)
                        bal_row['Pick Quantity'] = ''
                        bal_row['Destination Country'] = ''
                        bal_row['Order NO'] = ''
                        bal_row['ATS'] = int(balance_qty)
                        bal_row['COO'] = get_coo(vendor_name_inv)
                        for rmk, val in dmg_cols_data.items():
                            bal_row[rmk] = val
                        fmt_rows.append(bal_row)

                else:
                    # No partial entries found, just show the pallet as-is
                    row = build_base_row(inv_row)
                    row['Pick Quantity'] = db_actual_qty if db_actual_qty > 0 else ''
                    row['Destination Country'] = db_entry['country']
                    row['Order NO'] = db_entry['order_no']
                    ats_qty = inv_actual_qty - db_actual_qty
                    row['ATS'] = int(ats_qty) if (not is_damaged and ats_qty > 0) else ''
                    row['COO'] = get_coo(vendor_name_inv)
                    for rmk, val in dmg_cols_data.items():
                        row[rmk] = val
                    fmt_rows.append(row)

    # Build final column order
    final_cols = REPORT_HEADERS.copy()
    final_cols += ['Pick Quantity', 'Destination Country', 'Order NO', 'COO']
    final_cols += damage_remarks
    final_cols += ['ATS']

    fmt_df = pd.DataFrame(fmt_rows, columns=final_cols)

    return fmt_df, validation_issues, damage_remarks


def generate_inventory_details_report(inv_df, sh):
    try:
        pick_df = get_safe_dataframe(sh, "Master_Pick_Data")

        damage_lookup = {}
        try:
            dmg_df = get_safe_dataframe(sh, "Damage_Items")
            if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                for _, dr in dmg_df.iterrows():
                    p = str(dr.get('Pallet', '')).strip()
                    r = str(dr.get('Remark', 'Damage')).strip()
                    qty = str(dr.get('Actual Qty', '')).strip()
                    if p:
                        existing = damage_lookup.get(p, '')
                        new_remark = f"DAMAGE: {r} (Qty:{qty})" if qty else f"DAMAGE: {r}"
                        damage_lookup[p] = (existing + ' | ' + new_remark).lstrip(' | ') if existing else new_remark
        except Exception:
            pass

        report_rows = []

        for _, inv_row in inv_df.iterrows():
            pallet = str(inv_row.get('Pallet', '')).strip()

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
                continue

            if not pick_df.empty and 'Pallet' in pick_df.columns:
                pallet_picks = pick_df[pick_df['Pallet'].astype(str).str.strip() == pallet]

                if not pallet_picks.empty:
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
    else:
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

                    inv_cols_lower = [str(c).strip().lower() for c in inv.columns]
                    req_cols_lower = [str(c).strip().lower() for c in req.columns]

                    REQ_REQUIRED = ['so number', 'country name', 'ship mode: (sea/air)', 'product upc', 'pick qty']
                    INV_REQUIRED = ['pallet', 'actual qty']

                    missing_req = [c for c in REQ_REQUIRED if c not in req_cols_lower]
                    missing_inv = [c for c in INV_REQUIRED if c not in inv_cols_lower]

                    req_has_inv_cols = 'pallet' in req_cols_lower and 'actual qty' in req_cols_lower
                    inv_has_req_cols = 'so number' in inv_cols_lower and 'pick qty' in inv_cols_lower

                    if req_has_inv_cols and inv_has_req_cols:
                        st.error("⚠️ Files swapped! Inventory file සහ Customer Requirement file නිවැරදිව upload කරන්න.")
                        st.stop()

                    if missing_req:
                        st.error(f"❌ Customer Requirement file හි required columns නොමැත: **{', '.join(missing_req)}**")
                        st.stop()

                    if missing_inv:
                        st.error(f"❌ Inventory file හි required columns නොමැත: **{', '.join(missing_inv)}**")
                        st.stop()

                    req_col_map = {str(c).strip().lower(): str(c).strip() for c in req.columns}

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

                    if not hist_df.empty and 'SO Number' in hist_df.columns and 'Generated Load ID' in hist_df.columns:
                        for so in hist_df['SO Number'].astype(str).unique():
                            so_history = hist_df[hist_df['SO Number'].astype(str) == so]
                            so_counts[so] = len(so_history['Generated Load ID'].dropna().unique())

                    existing_load_ids = set()
                    if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
                        existing_load_ids = set(hist_df['Generated Load ID'].astype(str).tolist())

                    for group, data in req.groupby('Group'):
                        so_num = data['SO Number'].iloc[0]

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

                    inv_original = inv.copy()
                    inv = reconcile_inventory(inv, sh)

                    pick_df, part_df, summ_df = process_picking(inv, req, batch_id, sh, inv_original=inv_original)

                    if not pick_df.empty:
                        APIManager.append_rows_to_sheet(
                            sh, "Master_Pick_Data", MASTER_PICK_HEADERS,
                            pick_df.astype(str).replace('nan', '').values.tolist()
                        )

                    if not part_df.empty:
                        mpd_headers = SHEET_HEADERS["Master_Partial_Data"]
                        for col in mpd_headers:
                            if col not in part_df.columns:
                                part_df[col] = ''
                        part_df_save = part_df[mpd_headers]
                        APIManager.append_rows_to_sheet(
                            sh, "Master_Partial_Data", mpd_headers,
                            part_df_save.astype(str).replace('nan', '').values.tolist()
                        )

                    existing_summ = APIManager.read_sheet(sh, "Summary_Data")
                    if not existing_summ.empty and 'Load ID' in existing_summ.columns and not summ_df.empty:
                        new_load_ids = set(summ_df['Load ID'].astype(str).tolist())
                        existing_summ_clean = existing_summ[
                            ~existing_summ['Load ID'].astype(str).isin(new_load_ids)
                        ]
                        combined_summ = pd.concat([existing_summ_clean, summ_df], ignore_index=True)
                    else:
                        combined_summ = summ_df
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

                    st.success(f"✅ Data Processed! (Batch ID: {batch_id})")

        if st.session_state.get('show_verification', False):
            st.divider()
            st.subheader("📋 Verification: Customer Requirement vs Picked Data")
            st.info("කරුණාකර පහත Summary එක පරීක්ෂා කර Download කිරීමට පෙර Verify කරන්න.")
            st.dataframe(st.session_state['summary_df'].astype(str), use_container_width=True)

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
            APIManager.invalidate()
            st.rerun()

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
            st.info("දැනට පද්ධතියේ කිසිදු දත්තයක් නොමැත. 'Picking Operations' මගින් දත්ත ඇතුලත් කරන්න.")
        else:
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

                    filter_col1, _ = st.columns([2, 4])
                    status_filter = filter_col1.selectbox(
                        "🔽 Filter by Status:",
                        ["All", "Pending", "PL Pending", "Processing"],
                        key="dash_status_filter"
                    )
                    if status_filter != "All":
                        filtered_active = active_loads[active_loads['Pick Status'].astype(str) == status_filter]
                        load_ids = filtered_active['Generated Load ID'].dropna().unique().tolist()

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

                    pick_counts_by_lid = {}
                    pick_qty_by_lid = {}
                    if not pick_df.empty:
                        load_id_col_pick = None
                        for c in pick_df.columns:
                            if str(c).strip().lower() in ('load id', 'loadid', 'load_id'):
                                load_id_col_pick = c
                                break
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
                            status = str(load_row.get('Pick Status', 'Pending'))
                            so_num = str(load_row.get('SO Number', '-'))
                            country = str(load_row.get('Country Name', '-'))
                            ship = str(load_row.get('SHIP MODE', '-'))
                            date = str(load_row.get('Date', '-'))[:10]

                            lid_key = str(lid).strip()
                            pick_count = pick_counts_by_lid.get(lid_key, 0)
                            pick_qty_val = pick_qty_by_lid.get(lid_key, 0)

                            s = summ_by_load.get(str(lid), {})
                            variance = s.get('variance', 0)

                            status_bg = {'Pending': '#fff3cd', 'Processing': '#cce5ff'}.get(status, '#f0f0f0')
                            status_col = {'Pending': '#856404', 'Processing': '#004085'}.get(status, '#333')
                            status_dot = {'Pending': '🟡', 'Processing': '🔵'}.get(status, '⚪')

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
                                                    ws_pick_del.append_rows(filtered_mpd[MASTER_PICK_HEADERS].astype(str).replace('nan', '').values.tolist())
                                            st.success(f"✅ {lid} → Cancelled | Master_Pick_Data records deleted.")
                                        else:
                                            st.success(f"✅ {lid} → {new_st}")
                                        time.sleep(0.5)
                                        st.rerun()
                                except Exception as ex:
                                    st.error(f"Update failed: {ex}")

                    if zero_pick_ids:
                        st.markdown('<div style="background:#6c757d; color:white; display:inline-block; font-size:11px; font-weight:700; padding:3px 14px; border-radius:12px; margin:16px 0 4px 0;">⬜ NOT PICKED &nbsp;·&nbsp; {}</div>'.format(len(zero_pick_ids)), unsafe_allow_html=True)
                        render_load_list(zero_pick_ids, "#adb5bd", "NOT PICKED")

                    if shortage_ids:
                        st.markdown('<div style="background:#e74c3c; color:white; display:inline-block; font-size:11px; font-weight:700; padding:3px 14px; border-radius:12px; margin:16px 0 4px 0;">⚠️ SHORTAGE &nbsp;·&nbsp; {}</div>'.format(len(shortage_ids)), unsafe_allow_html=True)
                        render_load_list(shortage_ids, "#e74c3c", "SHORTAGE")

                    if full_pick_ids:
                        st.markdown('<div style="background:#27ae60; color:white; display:inline-block; font-size:11px; font-weight:700; padding:3px 14px; border-radius:12px; margin:16px 0 4px 0;">✅ FULLY PICKED &nbsp;·&nbsp; {}</div>'.format(len(full_pick_ids)), unsafe_allow_html=True)
                        render_load_list(full_pick_ids, "#27ae60", "FULLY PICKED")

            st.divider()

            # Batch Report Download
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

            # Advanced Search
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
                col_map_pick = {
                    "Load Id": "Load Id",
                    "Pallet": "Pallet",
                    "Supplier (Product UPC)": "Supplier",
                    "SO Number": "SO Number"
                }
                col_map_summ = {
                    "Load Id": "Load ID",
                    "Pallet": None,
                    "Supplier (Product UPC)": "UPC",
                    "SO Number": "SO Number"
                }

                filtered_picks = pd.DataFrame()
                if not pick_df.empty:
                    pick_search_col = col_map_pick[search_by]
                    actual_col_name = next(
                        (c for c in pick_df.columns if str(c).strip().lower() == pick_search_col.strip().lower()),
                        None
                    )
                    if actual_col_name:
                        if search_by == "Load Id":
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.strip() == str(search_term).strip()]
                        else:
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.contains(str(search_term).strip(), case=False, na=False)]

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
                    elif not summ_search_key:
                        st.info("Summary view is not available for Pallet search.")
                    else:
                        st.info("No summary data found.")

                with tab_dl:
                    if not filtered_picks.empty:
                        out_pick_dl = io.BytesIO()
                        with pd.ExcelWriter(out_pick_dl, engine='xlsxwriter') as writer:
                            filtered_picks.to_excel(writer, sheet_name='Pick_Report', index=False)
                            if not filtered_summ.empty:
                                filtered_summ.to_excel(writer, sheet_name='Variance_Summary', index=False)
                        safe_term = str(search_term).replace('/', '-').replace(' ', '_')
                        st.download_button(
                            f"⬇️ Download Pick Report — {search_term}",
                            data=out_pick_dl.getvalue(),
                            file_name=f"Pick_Report_{safe_term}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel",
                            use_container_width=True,
                            type="primary"
                        )
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
                                col_r2.metric("✅ Picked", len(report_df[report_df['Allocation Status'] == 'Picked']))
                                col_r3.metric("🟢 Available", len(report_df[report_df['Allocation Status'] == 'Available']))
                                col_r4.metric("🔴 Damage", len(report_df[report_df['Allocation Status'] == 'Damage']))
                            st.dataframe(report_df.astype(str), use_container_width=True)
                            out_basic = io.BytesIO()
                            with pd.ExcelWriter(out_basic, engine='xlsxwriter') as writer:
                                report_df.to_excel(writer, sheet_name='Inventory_Details', index=False)
                            st.download_button("⬇️ Download Basic Report", data=out_basic.getvalue(),
                                file_name=f"Inventory_Basic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.ms-excel", use_container_width=True)
                        else:
                            st.warning("Report generate කිරීම අසාර්ථක විය.")

            with tab_formatted:
                st.caption("""
                **Formatted Pick Report Logic:**
                - **Pallet DB නැත** → ATS = Actual Qty (direct to report)
                - **Pallet DB ඇත + Actual Qty සමාන** → Pick Quantity = Actual Qty
                - **Pallet DB ඇත + Actual Qty වෙනස්** → Partial Pallet check (Master_Partial_Data)
                - **COO** column → Vendor Name → Vendor_Maintain table → Country
                """)

                if st.button("📊 Generate Formatted Pick Report", type="primary", use_container_width=True, key="gen_fmt"):
                    with st.spinner("Generating Formatted Report..."):
                        inv_data = pd.read_csv(inv_report_file) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file)

                        fmt_df, validation_issues, damage_remarks = generate_formatted_pick_report(inv_data, sh)

                        # --- Validation Issues ---
                        if validation_issues:
                            st.error("⚠️ Validation Issues Detected!")
                            vi_df = pd.DataFrame(validation_issues)
                            st.dataframe(vi_df, use_container_width=True)
                            st.warning("ඉහත Pallets වල Partial Qty එකතුව Inventory Actual Qty ට වඩා වැඩිය. Report generate කළ ද validation errors ඇත.")

                        # --- Summary Metrics ---
                        if not fmt_df.empty:
                            inv_data_norm = inv_data.copy()
                            inv_data_norm.columns = [str(c).strip() for c in inv_data_norm.columns]
                            actual_col_inv = next((c for c in inv_data_norm.columns if c.strip().lower() == 'actual qty'), 'Actual Qty')
                            inv_total_qty = pd.to_numeric(inv_data_norm[actual_col_inv], errors='coerce').fillna(0).sum()

                            rpt_total_qty = pd.to_numeric(fmt_df['Actual Qty'], errors='coerce').fillna(0).sum()
                            total_pick_qty = pd.to_numeric(fmt_df['Pick Quantity'], errors='coerce').fillna(0).sum()
                            total_ats_qty = pd.to_numeric(fmt_df['ATS'], errors='coerce').fillna(0).sum()
                            total_dmg_qty = sum(
                                pd.to_numeric(fmt_df[r], errors='coerce').fillna(0).sum()
                                for r in damage_remarks if r in fmt_df.columns
                            )

                            qty_match = abs(inv_total_qty - rpt_total_qty) < 0.01
                            if qty_match:
                                st.success(f"✅ Actual Qty Match! Inventory: **{int(inv_total_qty)}** = Report: **{int(rpt_total_qty)}**")
                            else:
                                st.error(f"⚠️ Actual Qty Mismatch! Inventory: **{int(inv_total_qty)}** ≠ Report: **{int(rpt_total_qty)}** (diff: {int(inv_total_qty - rpt_total_qty)})")

                            st.markdown("#### 📊 Report Summary")
                            sc1, sc2, sc3, sc4 = st.columns(4)
                            sc1.metric("Total Lines", len(fmt_df))
                            sc2.metric("Pick Qty", int(total_pick_qty))
                            sc3.metric("ATS Qty", int(total_ats_qty))
                            sc4.metric("Damage Qty", int(total_dmg_qty))

                            accounted = total_pick_qty + total_ats_qty + total_dmg_qty
                            if abs(inv_total_qty - accounted) < 0.01:
                                st.success(f"✅ Qty Reconciled: Pick({int(total_pick_qty)}) + ATS({int(total_ats_qty)}) + Damage({int(total_dmg_qty)}) = {int(accounted)}")
                            else:
                                st.warning(f"⚠️ Unaccounted Qty: {int(inv_total_qty - accounted)} | Pick+ATS+Damage={int(accounted)} vs Inventory={int(inv_total_qty)}")

                            st.dataframe(fmt_df.astype(str), use_container_width=True)

                            # --- Excel Export ---
                            final_cols = list(fmt_df.columns)
                            out_fmt = io.BytesIO()
                            with pd.ExcelWriter(out_fmt, engine='xlsxwriter') as writer:
                                fmt_df.to_excel(writer, sheet_name='Pick_Report', index=False)
                                wb = writer.book
                                ws_fmt = writer.sheets['Pick_Report']

                                # Summary sheet
                                ws_summ_sheet = wb.add_worksheet('Summary')
                                bold = wb.add_format({'bold': True, 'font_size': 11})
                                val_fmt_xl = wb.add_format({'font_size': 11, 'num_format': '#,##0'})
                                ok_fmt_xl = wb.add_format({'bold': True, 'font_color': '#27ae60', 'font_size': 11})
                                err_fmt_xl = wb.add_format({'bold': True, 'font_color': '#e74c3c', 'font_size': 11})

                                summary_rows_xl = [
                                    ('Inventory Total Actual Qty', int(inv_total_qty)),
                                    ('Report Total Actual Qty', int(rpt_total_qty)),
                                    ('Qty Match', 'YES ✅' if qty_match else 'NO ⚠️'),
                                    ('', ''),
                                    ('Pick Quantity', int(total_pick_qty)),
                                    ('ATS Quantity', int(total_ats_qty)),
                                    ('Damage Quantity', int(total_dmg_qty)),
                                    ('Total Accounted', int(accounted)),
                                    ('Unaccounted', int(inv_total_qty - accounted)),
                                    ('', ''),
                                    ('Total Report Lines', len(fmt_df)),
                                    ('Validation Issues', len(validation_issues)),
                                ]
                                ws_summ_sheet.set_column(0, 0, 28)
                                ws_summ_sheet.set_column(1, 1, 18)
                                for ri, (label, value) in enumerate(summary_rows_xl):
                                    ws_summ_sheet.write(ri, 0, label, bold)
                                    if isinstance(value, int):
                                        ws_summ_sheet.write(ri, 1, value, val_fmt_xl)
                                    elif 'YES' in str(value):
                                        ws_summ_sheet.write(ri, 1, value, ok_fmt_xl)
                                    elif 'NO' in str(value):
                                        ws_summ_sheet.write(ri, 1, value, err_fmt_xl)
                                    else:
                                        ws_summ_sheet.write(ri, 1, value)

                                # Validation Issues sheet
                                if validation_issues:
                                    vi_sheet = wb.add_worksheet('Validation_Issues')
                                    vi_df2 = pd.DataFrame(validation_issues)
                                    vi_hdr = wb.add_format({'bold': True, 'bg_color': '#e74c3c', 'font_color': '#fff'})
                                    for ci, col in enumerate(vi_df2.columns):
                                        vi_sheet.write(0, ci, col, vi_hdr)
                                        vi_sheet.set_column(ci, ci, 22)
                                    for ri2, row2 in vi_df2.iterrows():
                                        for ci2, val2 in enumerate(row2):
                                            vi_sheet.write(ri2 + 1, ci2, val2)

                                # Format Pick_Report sheet
                                hdr_fmt = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#ffffff', 'border': 1, 'font_size': 10})
                                pick_col_fmt = wb.add_format({'bg_color': '#E8F5E9', 'border': 1, 'font_size': 10})
                                dmg_col_fmt = wb.add_format({'bg_color': '#FFE0E0', 'border': 1, 'font_size': 10})
                                coo_col_fmt = wb.add_format({'bg_color': '#FFF9C4', 'border': 1, 'font_size': 10})
                                ats_col_fmt = wb.add_format({'bg_color': '#E3F2FD', 'border': 1, 'font_size': 10, 'bold': True})
                                normal_fmt = wb.add_format({'border': 1, 'font_size': 10})

                                for ci, col_name in enumerate(final_cols):
                                    ws_fmt.write(0, ci, col_name, hdr_fmt)
                                    ws_fmt.set_column(ci, ci, 15)
                                    for ri in range(1, len(fmt_df) + 1):
                                        val = str(fmt_df.iloc[ri - 1][col_name])
                                        if col_name in ['Pick Quantity', 'Destination Country', 'Order NO']:
                                            ws_fmt.write(ri, ci, val, pick_col_fmt)
                                        elif col_name in damage_remarks:
                                            ws_fmt.write(ri, ci, val, dmg_col_fmt)
                                        elif col_name == 'COO':
                                            ws_fmt.write(ri, ci, val, coo_col_fmt)
                                        elif col_name == 'ATS':
                                            ws_fmt.write(ri, ci, val, ats_col_fmt)
                                        else:
                                            ws_fmt.write(ri, ci, val, normal_fmt)
                                ws_fmt.freeze_panes(1, 0)

                            st.download_button("⬇️ Download Formatted Pick Report",
                                data=out_fmt.getvalue(),
                                file_name=f"Pick_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.ms-excel", use_container_width=True)
                            show_confetti()
                        else:
                            st.warning("Report generate කිරීම අසාර්ථක විය. Data නොමැත.")

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

        with del_tab1:
            st.info("Load ID, Pallet සහ Actual Qty අඩංගු file upload කිරීමෙන් Master_Pick_Data එකෙන් මකා දැමිය හැක.")
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
                                st.success(f"✅ {deleted_count} records මකා දමන ලදී!")
                                show_confetti()
                            else:
                                st.warning("⚠️ ගැලපෙන records නොමැත.")
                        else:
                            st.error("Master_Pick_Data හි data නොමැත.")

        with del_tab2:
            st.info("Load ID එකක් ටයිප් කිරීමෙන් ඒ Load ID හා සම්බන්ධ data Master_Pick_Data එකෙන් delete කළ හැක.")
            del_load_id = st.text_input("🆔 Enter Load ID to Delete:")

            if del_load_id:
                master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
                if not master_pick_df.empty and 'Load Id' in master_pick_df.columns:
                    preview = master_pick_df[master_pick_df['Load Id'].astype(str).str.strip() == del_load_id.strip()]
                    if not preview.empty:
                        st.warning(f"⚠️ Load ID **{del_load_id}** සඳහා {len(preview)} records මකා දැමෙනු ඇත.")
                        st.dataframe(preview.astype(str), use_container_width=True)
                    else:
                        st.info(f"Load ID **{del_load_id}** සඳහා records නොමැත.")

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

                        st.success(f"✅ Load ID **{del_load_id}** — {deleted_pick} records මකා දමන ලදී!")
                        show_confetti()

        with del_tab3:
            st.info("Batch ID එකකට අදාල සියලු records Master_Pick_Data එකෙන් delete කළ හැක.")
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

                                st.success(f"✅ Batch ID **{del_batch_id}** — {deleted_batch} records මකා දමන ලදී!")
                                show_confetti()
                else:
                    st.info("Master_Pick_Data හි Batch IDs නොමැත.")
            else:
                st.info("Master_Pick_Data හි data නොමැත.")

        with del_tab4:
            st.info("Pallet ID ගෙනහිර Master_Pick_Data, Master_Partial_Data, Damage_Items වලින් delete කළ හැක.")

            _mpd_pal = get_safe_dataframe(sh, "Master_Pick_Data")
            _mpart_pal = get_safe_dataframe(sh, "Master_Partial_Data")
            _dmg_pal = get_safe_dataframe(sh, "Damage_Items")

            all_pallets_set = set()
            for _df, _col in [(_mpd_pal, 'Pallet'), (_mpart_pal, 'Pallet'), (_dmg_pal, 'Pallet')]:
                if not _df.empty and _col in _df.columns:
                    all_pallets_set.update(_df[_col].dropna().astype(str).str.strip().tolist())
            if not _mpart_pal.empty and 'Gen Pallet ID' in _mpart_pal.columns:
                all_pallets_set.update(_mpart_pal['Gen Pallet ID'].dropna().astype(str).str.strip().tolist())

            all_pallets_list = sorted([p for p in all_pallets_set if p])

            if all_pallets_list:
                del_pallet = st.selectbox("📦 Select Pallet to Delete:", all_pallets_list, key="del_pallet_sel")

                if del_pallet:
                    def _count_rows(df, col, val):
                        if df.empty or col not in df.columns:
                            return 0
                        return len(df[df[col].astype(str).str.strip() == str(val).strip()])

                    mpd_count = _count_rows(_mpd_pal, 'Pallet', del_pallet)
                    mpart_count = _count_rows(_mpart_pal, 'Pallet', del_pallet)
                    mpart_gen_count = _count_rows(_mpart_pal, 'Gen Pallet ID', del_pallet) if not _mpart_pal.empty and 'Gen Pallet ID' in _mpart_pal.columns else 0
                    dmg_count = _count_rows(_dmg_pal, 'Pallet', del_pallet)

                    st.warning(f"⚠️ Pallet **{del_pallet}** හා සම්බන්ධ records:")
                    pc1, pc2, pc3 = st.columns(3)
                    pc1.metric("Master_Pick_Data", mpd_count)
                    pc2.metric("Master_Partial_Data", mpart_count + mpart_gen_count)
                    pc3.metric("Damage_Items", dmg_count)

                    if st.button("🗑️ Delete Pallet from All Sheets", type="primary", key="del_pallet_btn"):
                        with st.spinner("Deleting..."):
                            results = []

                            mpd_fresh = get_safe_dataframe(sh, "Master_Pick_Data")
                            if not mpd_fresh.empty and 'Pallet' in mpd_fresh.columns:
                                filtered_mpd = mpd_fresh[
                                    mpd_fresh['Pallet'].astype(str).str.strip() != str(del_pallet).strip()
                                ]
                                deleted = len(mpd_fresh) - len(filtered_mpd)
                                APIManager.overwrite_sheet(sh, "Master_Pick_Data", MASTER_PICK_HEADERS, filtered_mpd)
                                results.append(f"Master_Pick_Data: {deleted} records")

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
        st.info("Damage, defective හෝ unavailable items මෙහි Pallet/Actual Qty/Remark සහිතව upload කරන්න. Remark column ඇති ලෙසම save වේ.")

        dmg_tab1, dmg_tab2 = st.tabs(["📤 Upload Damage Items", "📋 View Damage Records"])

        with dmg_tab1:
            st.subheader("Upload Damage Items File")
            st.caption("File එකේ Pallet, Actual Qty, Remark columns තිබිය යුතුය. Remark column values report columns ලෙස use වේ.")

            dmg_file = st.file_uploader("Upload Damage Items (CSV/Excel)", type=['csv', 'xlsx'], key="dmg_uploader")

            if dmg_file:
                dmg_preview = pd.read_csv(dmg_file) if dmg_file.name.endswith('.csv') else pd.read_excel(dmg_file)
                st.dataframe(dmg_preview.astype(str), use_container_width=True)

                if st.button("💾 Save Damage Items", type="primary"):
                    with st.spinner("Saving Damage Items..."):
                        pallet_col = next((c for c in dmg_preview.columns if 'pallet' in c.lower()), None)
                        qty_col = next((c for c in dmg_preview.columns if 'actual qty' in c.lower() or 'qty' in c.lower()), None)
                        remark_col = next((c for c in dmg_preview.columns if 'remark' in c.lower()), None)

                        if not pallet_col or not qty_col:
                            st.error("File එකේ 'Pallet' සහ 'Actual Qty' columns අවශ්‍යයි.")
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
                            st.success(f"✅ Damage Items {len(rows_to_add)} ක් save කරන ලදී!")
                            show_confetti()

        with dmg_tab2:
            st.subheader("Damage Items Records")
            dmg_df = get_safe_dataframe(sh, "Damage_Items")
            if dmg_df.empty:
                st.info("Damage Items records නොමැත.")
            else:
                st.metric("Total Damage Records", len(dmg_df))
                st.dataframe(dmg_df.astype(str), use_container_width=True)

                out_dmg = io.BytesIO()
                with pd.ExcelWriter(out_dmg, engine='xlsxwriter') as writer:
                    dmg_df.to_excel(writer, sheet_name='Damage_Items', index=False)
                st.download_button(
                    "⬇️ Download Damage Records",
                    data=out_dmg.getvalue(),
                    file_name=f"Damage_Items_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.ms-excel"
                )

                st.divider()
                st.subheader("🗑️ Remove Damage Record")
                if 'Pallet' in dmg_df.columns:
                    remove_pallet = st.selectbox("Select Pallet to Remove:", dmg_df['Pallet'].dropna().unique())
                    if st.button("Remove Damage Record"):
                        filtered_dmg = dmg_df[dmg_df['Pallet'].astype(str) != str(remove_pallet)]
                        ws_dmg = sh.worksheet("Damage_Items")
                        ws_dmg.clear()
                        ws_dmg.append_row(SHEET_HEADERS["Damage_Items"])
                        if not filtered_dmg.empty:
                            ws_dmg.append_rows(filtered_dmg.astype(str).replace('nan', '').values.tolist())
                        st.success(f"✅ Pallet **{remove_pallet}** Damage list ගෙන් ඉවත් කරන ලදී.")
                        st.rerun()

    # ==========================================
    # TAB 6: ADMIN SETTINGS
    # ==========================================
    elif choice == "⚙️ Admin Settings":
        st.title("⚙️ System Administration")

        adm_tab1, adm_tab2, adm_tab3 = st.tabs(["👥 User Management", "⚠️ Database Management", "🏭 Vendor Maintain"])

        with adm_tab1:
            st.subheader("👥 Add New User")
            n_user = st.text_input("New Username")
            n_pass = st.text_input("New Password", type="password")
            n_role = st.selectbox("Role", ["user", "SysUser", "admin"])
            if st.button("Add User", type="primary"):
                if n_user and n_pass:
                    ws_users = sh.worksheet("Users")
                    users_data = get_safe_dataframe(sh, "Users")
                    if not users_data.empty and n_user in users_data['Username'].values:
                        st.error("මෙම Username දැනටමත් ඇත.")
                    else:
                        ws_users.append_row([n_user, n_pass, n_role])
                        st.success("User සාර්ථකව ඇතුලත් කරන ලදී!")

        with adm_tab2:
            st.subheader("⚠️ Database Management")
            st.warning("මෙමඟින් selected sheet data සම්පූර්ණයෙන්ම reset වේ.")

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
                        st.success(f"✅ {sheet_to_clear} Reset කරන ලදී.")
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.error("'CONFIRM' ලෙස type කරන්න.")

        with adm_tab3:
            st.subheader("🏭 Vendor Maintain (COO Lookup)")
            st.caption("Vendor Name → Country (COO) mapping. Formatted Pick Report COO column update කිරීමට use වේ.")

            vendor_df_view = get_safe_dataframe(sh, "Vendor_Maintain")
            if not vendor_df_view.empty:
                st.dataframe(vendor_df_view.astype(str), use_container_width=True)
            else:
                st.info("Vendor_Maintain data නොමැත.")

            st.divider()
            st.subheader("Add/Update Vendor")
            v_name = st.text_input("Vendor Name")
            v_country = st.text_input("Country (COO)")
            if st.button("💾 Save Vendor", type="primary"):
                if v_name and v_country:
                    ws_vendor = get_or_create_sheet(sh, "Vendor_Maintain", SHEET_HEADERS["Vendor_Maintain"])
                    ws_vendor.append_row([v_name, v_country])
                    APIManager.invalidate("Vendor_Maintain")
                    st.success(f"✅ Vendor **{v_name}** → **{v_country}** save කරන ලදී!")
                    st.rerun()

            st.divider()
            st.subheader("📤 Bulk Upload Vendor Data")
            vendor_file = st.file_uploader("Upload Vendor List (Vendor Name, Country columns)", type=['csv', 'xlsx'], key="vendor_upload")
            if vendor_file:
                vendor_upload_df = pd.read_csv(vendor_file) if vendor_file.name.endswith('.csv') else pd.read_excel(vendor_file)
                st.dataframe(vendor_upload_df.astype(str), use_container_width=True)
                if st.button("📤 Upload Vendor Data", type="primary"):
                    ws_vendor = get_or_create_sheet(sh, "Vendor_Maintain", SHEET_HEADERS["Vendor_Maintain"])
                    rows = vendor_upload_df.astype(str).replace('nan', '').values.tolist()
                    ws_vendor.append_rows(rows)
                    APIManager.invalidate("Vendor_Maintain")
                    st.success(f"✅ {len(rows)} vendor records upload කරන ලදී!")


footer_branding()
