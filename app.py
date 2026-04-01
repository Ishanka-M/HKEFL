import streamlit as st
import pandas as pd
from supabase import create_client, Client
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
                             'Location Id', 'Lot Number', 'Color', 'Size', 'Style', 'Customer Po Number'],
    "Master_Pick_Data": MASTER_PICK_HEADERS,
    "Damage_Items": ['Pallet', 'Actual Qty', 'Remark', 'Date Added', 'Added By']
}

# ── Supabase column maps: Excel header → DB column name ──────
MPD_COL_MAP = {
    'Wh Id': 'wh_id', 'Client Code': 'client_code', 'Pallet': 'pallet',
    'Invoice Number': 'invoice_number', 'Location Id': 'location_id',
    'Item Number': 'item_number', 'Description': 'description',
    'Lot Number': 'lot_number', 'Actual Qty': 'actual_qty',
    'Unavailable Qty': 'unavailable_qty', 'Uom': 'uom', 'Status': 'status',
    'Mlp': 'mlp', 'Stored Attribute Id': 'stored_attribute_id',
    'Fifo Date': 'fifo_date', 'Expiration Date': 'expiration_date',
    'Grn Number': 'grn_number', 'Gate Pass Id': 'gate_pass_id',
    'Cust Dec No': 'cust_dec_no', 'Color': 'color', 'Size': 'size',
    'Style': 'style', 'Supplier': 'supplier', 'Plant': 'plant',
    'Client So': 'client_so', 'Client So Line': 'client_so_line',
    'Po Cust Dec': 'po_cust_dec', 'Customer Ref Number': 'customer_ref_number',
    'Item Id': 'item_id', 'Invoice Number1': 'invoice_number1',
    'Transaction': 'transaction', 'Order Type': 'order_type',
    'Order Number': 'order_number', 'Store Order Number': 'store_order_number',
    'Customer Po Number': 'customer_po_number',
    'Partial Order Flag': 'partial_order_flag', 'Order Date': 'order_date',
    'Load Id': 'load_id', 'Asn Number': 'asn_number', 'Po Number': 'po_number',
    'Supplier Hu': 'supplier_hu', 'New Item Number': 'new_item_number',
    'Asn Line Number': 'asn_line_number',
    'Received Gross Weight': 'received_gross_weight',
    'Current Gross Weight': 'current_gross_weight',
    'Received Net Weight': 'received_net_weight',
    'Current Net Weight': 'current_net_weight',
    'Supplier Desc': 'supplier_desc', 'Cbm': 'cbm',
    'Container Type': 'container_type',
    'Display Item Number': 'display_item_number',
    'Old Item Number': 'old_item_number', 'Inventory Type': 'inventory_type',
    'Type Qc': 'type_qc', 'Vendor Name': 'vendor_name',
    'Manufacture Date': 'manufacture_date', 'Suom': 'suom',
    'S Qty': 's_qty', 'Pick Id': 'pick_id', 'Downloaded Date': 'downloaded_date',
    'Batch ID': 'batch_id', 'SO Number': 'so_number',
    'Generated Load ID': 'generated_load_id', 'Country Name': 'country_name',
    'Pick Quantity': 'pick_quantity', 'Remark': 'remark',
}

# Reverse map: db_col → Excel header
MPD_COL_REVERSE = {v: k for k, v in MPD_COL_MAP.items()}

MPD_NUMERIC_DB = {
    'actual_qty', 'unavailable_qty', 'received_gross_weight', 'current_gross_weight',
    'received_net_weight', 'current_net_weight', 'cbm', 's_qty', 'pick_quantity',
    'asn_line_number', 'client_so_line',
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


# ============================================================
# 2. Supabase Client (replaces gspread)
# ============================================================
@st.cache_resource
def get_supabase_client() -> Client:
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["service_role_key"]
    return create_client(url, key)


# ============================================================
# 3. SupabaseManager — replaces APIManager
#    Same public interface: read_sheet / overwrite_sheet /
#    append_rows_to_sheet / batch_read / invalidate
# ============================================================
class SupabaseManager:
    _cache: dict = {}
    _cache_ttl: int = 60  # seconds

    # ── table routing ──────────────────────────────────────
    TABLE_MAP = {
        "Load_History":        "load_history",
        "Master_Pick_Data":    "master_pick_data",
        "Master_Partial_Data": "master_partial_data",
        "Summary_Data":        "summary_data",
        "Damage_Items":        "damage_items",
        "Users":               "users",
    }

    @classmethod
    def _tbl(cls, sheet_name: str) -> str:
        return cls.TABLE_MAP.get(sheet_name, sheet_name.lower())

    @classmethod
    def invalidate(cls, sheet_name=None):
        if sheet_name:
            cls._cache.pop(sheet_name, None)
        else:
            cls._cache.clear()

    # ── row → DataFrame conversions ───────────────────────
    @classmethod
    def _rows_to_df_load_history(cls, rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=SHEET_HEADERS["Load_History"])
        df = pd.DataFrame(rows)
        rename = {
            'batch_id': 'Batch ID', 'generated_load_id': 'Generated Load ID',
            'so_number': 'SO Number', 'country_name': 'Country Name',
            'ship_mode': 'SHIP MODE', 'date': 'Date', 'pick_status': 'Pick Status',
        }
        df = df.rename(columns=rename)
        for col in SHEET_HEADERS["Load_History"]:
            if col not in df.columns:
                df[col] = ''
        return df[SHEET_HEADERS["Load_History"]]

    @classmethod
    def _rows_to_df_mpd(cls, rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=MASTER_PICK_HEADERS)
        df = pd.DataFrame(rows)
        df = df.rename(columns=MPD_COL_REVERSE)
        for col in MASTER_PICK_HEADERS:
            if col not in df.columns:
                df[col] = ''
        return df[MASTER_PICK_HEADERS]

    @classmethod
    def _rows_to_df_partial(cls, rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=SHEET_HEADERS["Master_Partial_Data"])
        df = pd.DataFrame(rows)
        rename = {
            'batch_id': 'Batch ID', 'so_number': 'SO Number', 'pallet': 'Pallet',
            'supplier': 'Supplier', 'load_id': 'Load ID', 'country_name': 'Country Name',
            'actual_qty': 'Actual Qty', 'partial_qty': 'Partial Qty',
            'gen_pallet_id': 'Gen Pallet ID', 'balance_qty': 'Balance Qty',
            'location_id': 'Location Id', 'lot_number': 'Lot Number',
            'color': 'Color', 'size': 'Size', 'style': 'Style',
            'customer_po_number': 'Customer Po Number',
        }
        df = df.rename(columns=rename)
        for col in SHEET_HEADERS["Master_Partial_Data"]:
            if col not in df.columns:
                df[col] = ''
        return df[SHEET_HEADERS["Master_Partial_Data"]]

    @classmethod
    def _rows_to_df_summary(cls, rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=SHEET_HEADERS["Summary_Data"])
        df = pd.DataFrame(rows)
        rename = {
            'batch_id': 'Batch ID', 'so_number': 'SO Number', 'load_id': 'Load ID',
            'upc': 'UPC', 'country': 'Country', 'ship_mode': 'Ship Mode',
            'requested': 'Requested', 'picked': 'Picked',
            'variance': 'Variance', 'status': 'Status',
        }
        df = df.rename(columns=rename)
        for col in SHEET_HEADERS["Summary_Data"]:
            if col not in df.columns:
                df[col] = ''
        return df[SHEET_HEADERS["Summary_Data"]]

    @classmethod
    def _rows_to_df_damage(cls, rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=SHEET_HEADERS["Damage_Items"])
        df = pd.DataFrame(rows)
        rename = {
            'pallet': 'Pallet', 'actual_qty': 'Actual Qty', 'remark': 'Remark',
            'date_added': 'Date Added', 'added_by': 'Added By',
        }
        df = df.rename(columns=rename)
        for col in SHEET_HEADERS["Damage_Items"]:
            if col not in df.columns:
                df[col] = ''
        return df[SHEET_HEADERS["Damage_Items"]]

    @classmethod
    def _rows_to_df_users(cls, rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=['Username', 'Password', 'Role'])
        df = pd.DataFrame(rows)
        rename = {'username': 'Username', 'password': 'Password', 'role': 'Role'}
        return df.rename(columns=rename)[['Username', 'Password', 'Role']]

    CONVERTERS = {
        "Load_History":        _rows_to_df_load_history.__func__,
        "Master_Pick_Data":    _rows_to_df_mpd.__func__,
        "Master_Partial_Data": _rows_to_df_partial.__func__,
        "Summary_Data":        _rows_to_df_summary.__func__,
        "Damage_Items":        _rows_to_df_damage.__func__,
        "Users":               _rows_to_df_users.__func__,
    }

    # ── read_sheet (with TTL cache) ───────────────────────
    @classmethod
    def read_sheet(cls, _sh, sheet_name: str, force=False) -> pd.DataFrame:
        now = time.time()
        if not force and sheet_name in cls._cache:
            ts, df = cls._cache[sheet_name]
            if (now - ts) < cls._cache_ttl:
                return df.copy()
        try:
            sb = get_supabase_client()
            tbl = cls._tbl(sheet_name)
            res = sb.table(tbl).select("*").execute()
            rows = res.data or []
            conv = cls.CONVERTERS.get(sheet_name)
            df = conv(cls, rows) if conv else pd.DataFrame(rows)
            cls._cache[sheet_name] = (time.time(), df)
            return df.copy()
        except Exception as e:
            if sheet_name in cls._cache:
                return cls._cache[sheet_name][1].copy()
            return pd.DataFrame()

    @classmethod
    def batch_read(cls, _sh, sheet_names: list, force=False) -> dict:
        return {name: cls.read_sheet(None, name, force=force) for name in sheet_names}

    # ── append rows ───────────────────────────────────────
    @classmethod
    def append_rows_to_sheet(cls, _sh, sheet_name: str, headers, rows, retries=3) -> bool:
        if not rows:
            return True
        sb = get_supabase_client()
        tbl = cls._tbl(sheet_name)
        db_rows = cls._excel_rows_to_db(sheet_name, headers, rows)
        for attempt in range(retries):
            try:
                BATCH = 500
                for i in range(0, len(db_rows), BATCH):
                    sb.table(tbl).insert(db_rows[i:i+BATCH]).execute()
                cls.invalidate(sheet_name)
                return True
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    st.error(f"❌ Supabase insert error ({sheet_name}): {e}")
                    raise
        return False

    @classmethod
    def overwrite_sheet(cls, _sh, sheet_name: str, headers, df: pd.DataFrame, retries=3) -> bool:
        """Delete all rows then re-insert."""
        sb = get_supabase_client()
        tbl = cls._tbl(sheet_name)
        for attempt in range(retries):
            try:
                # Delete all existing rows
                sb.table(tbl).delete().neq("id", 0).execute()
                cls.invalidate(sheet_name)
                if not df.empty:
                    rows_list = df.astype(str).replace('nan', '').values.tolist()
                    db_rows = cls._excel_rows_to_db(sheet_name, headers, rows_list)
                    BATCH = 500
                    for i in range(0, len(db_rows), BATCH):
                        sb.table(tbl).insert(db_rows[i:i+BATCH]).execute()
                cls.invalidate(sheet_name)
                return True
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    st.error(f"❌ Supabase overwrite error ({sheet_name}): {e}")
                    raise
        return False

    # ── Excel-style rows → Supabase dicts ────────────────
    @classmethod
    def _excel_rows_to_db(cls, sheet_name: str, headers: list, rows: list) -> list:
        """Convert list-of-lists (Excel row format) to list-of-dicts for Supabase."""
        result = []
        for row in rows:
            d = dict(zip(headers, row))
            result.append(cls._dict_excel_to_db(sheet_name, d))
        return result

    @classmethod
    def _dict_excel_to_db(cls, sheet_name: str, d: dict) -> dict:
        def _clean(v):
            if v is None or str(v).strip() in ('', 'nan', 'None'):
                return None
            return str(v).strip()

        def _num(v):
            try:
                f = float(str(v).strip())
                return f
            except (ValueError, TypeError):
                return None

        if sheet_name == "Load_History":
            return {
                'batch_id':          _clean(d.get('Batch ID')),
                'generated_load_id': _clean(d.get('Generated Load ID')),
                'so_number':         _clean(d.get('SO Number')),
                'country_name':      _clean(d.get('Country Name')),
                'ship_mode':         _clean(d.get('SHIP MODE')),
                'date':              _clean(d.get('Date')),
                'pick_status':       _clean(d.get('Pick Status', 'Pending')),
            }
        elif sheet_name == "Master_Pick_Data":
            row = {}
            for excel_col, db_col in MPD_COL_MAP.items():
                val = d.get(excel_col)
                if db_col in MPD_NUMERIC_DB:
                    row[db_col] = _num(val)
                else:
                    row[db_col] = _clean(val)
            return row
        elif sheet_name == "Master_Partial_Data":
            return {
                'batch_id':           _clean(d.get('Batch ID')),
                'so_number':          _clean(d.get('SO Number')),
                'pallet':             _clean(d.get('Pallet')),
                'supplier':           _clean(d.get('Supplier')),
                'load_id':            _clean(d.get('Load ID')),
                'country_name':       _clean(d.get('Country Name')),
                'actual_qty':         _num(d.get('Actual Qty')),
                'partial_qty':        _num(d.get('Partial Qty')),
                'gen_pallet_id':      _clean(d.get('Gen Pallet ID')),
                'balance_qty':        _num(d.get('Balance Qty')),
                'location_id':        _clean(d.get('Location Id')),
                'lot_number':         _clean(d.get('Lot Number')),
                'color':              _clean(d.get('Color')),
                'size':               _clean(d.get('Size')),
                'style':              _clean(d.get('Style')),
                'customer_po_number': _clean(d.get('Customer Po Number')),
            }
        elif sheet_name == "Summary_Data":
            return {
                'batch_id':  _clean(d.get('Batch ID')),
                'so_number': _clean(d.get('SO Number')),
                'load_id':   _clean(d.get('Load ID')),
                'upc':       _clean(d.get('UPC')),
                'country':   _clean(d.get('Country')),
                'ship_mode': _clean(d.get('Ship Mode')),
                'requested': _num(d.get('Requested')),
                'picked':    _num(d.get('Picked')),
                'variance':  _num(d.get('Variance')),
                'status':    _clean(d.get('Status')),
            }
        elif sheet_name == "Damage_Items":
            return {
                'pallet':     _clean(d.get('Pallet')),
                'actual_qty': _num(d.get('Actual Qty')),
                'remark':     _clean(d.get('Remark')),
                'date_added': _clean(d.get('Date Added')),
                'added_by':   _clean(d.get('Added By')),
            }
        elif sheet_name == "Users":
            return {
                'username': _clean(d.get('Username')),
                'password': _clean(d.get('Password')),
                'role':     _clean(d.get('Role', 'user')),
            }
        return d

    # ── Compatibility shims (match old APIManager API) ────
    @classmethod
    def get_workbook(cls, retries=3, delay=5):
        """Returns None — Supabase doesn't need a workbook object."""
        try:
            get_supabase_client()
            return True   # truthy sentinel so callers know connection is OK
        except Exception as e:
            st.error(f"❌ Supabase connection error: {e}")
            raise

    @classmethod
    def get_or_create_ws(cls, _sh, name, headers, retries=3):
        """No-op for Supabase — tables are created via SQL schema."""
        return name  # return sheet name as identifier


# ── Drop-in aliases (keep same names as original code) ───────
APIManager = SupabaseManager


def get_master_workbook(retries=3, delay=5):
    return APIManager.get_workbook(retries=retries, delay=delay)


def get_safe_dataframe(_sh, sheet_name, retries=3):
    return APIManager.read_sheet(None, sheet_name)


def get_or_create_sheet(_sh, name, headers, retries=3):
    return APIManager.get_or_create_ws(None, name, headers, retries)


# ── Supabase-specific helpers (replace direct ws.* calls) ────
def _sb_update_load_status(load_id: str, new_status: str):
    """Update pick_status for a Load ID in load_history."""
    sb = get_supabase_client()
    sb.table("load_history").update({"pick_status": new_status}) \
        .eq("generated_load_id", str(load_id).strip()).execute()
    APIManager.invalidate("Load_History")


def _sb_delete_mpd_by_load_id(load_ids: list):
    """Delete master_pick_data rows matching any of the given generated_load_ids."""
    sb = get_supabase_client()
    for lid in load_ids:
        sb.table("master_pick_data").delete().eq("generated_load_id", str(lid).strip()).execute()
    APIManager.invalidate("Master_Pick_Data")


def _sb_delete_mpd_by_match_keys(keys: list):
    """Delete rows matching (generated_load_id, pallet, actual_qty) tuples."""
    sb = get_supabase_client()
    for (lid, pallet, qty) in keys:
        sb.table("master_pick_data").delete() \
            .eq("generated_load_id", str(lid).strip()) \
            .eq("pallet", str(pallet).strip()) \
            .eq("actual_qty", float(qty) if qty is not None else 0) \
            .execute()
    APIManager.invalidate("Master_Pick_Data")


def _sb_delete_mpd_by_batch(batch_id: str):
    sb = get_supabase_client()
    sb.table("master_pick_data").delete().eq("batch_id", str(batch_id).strip()).execute()
    APIManager.invalidate("Master_Pick_Data")


def _sb_delete_by_pallet(pallet: str):
    """Delete given pallet from master_pick_data, master_partial_data, damage_items."""
    sb = get_supabase_client()
    sb.table("master_pick_data").delete().eq("pallet", str(pallet).strip()).execute()
    sb.table("master_partial_data").delete().eq("pallet", str(pallet).strip()).execute()
    sb.table("master_partial_data").delete().eq("gen_pallet_id", str(pallet).strip()).execute()
    sb.table("damage_items").delete().eq("pallet", str(pallet).strip()).execute()
    APIManager.invalidate("Master_Pick_Data")
    APIManager.invalidate("Master_Partial_Data")
    APIManager.invalidate("Damage_Items")


def _sb_upsert_summary(summ_df: pd.DataFrame):
    """Upsert summary rows (load_id is unique key)."""
    sb = get_supabase_client()
    rows = []
    for _, r in summ_df.iterrows():
        rows.append({
            'batch_id':  str(r.get('Batch ID', '') or '').strip() or None,
            'so_number': str(r.get('SO Number', '') or '').strip() or None,
            'load_id':   str(r.get('Load ID', '') or '').strip() or None,
            'upc':       str(r.get('UPC', '') or '').strip() or None,
            'country':   str(r.get('Country', '') or '').strip() or None,
            'ship_mode': str(r.get('Ship Mode', '') or '').strip() or None,
            'requested': float(r.get('Requested', 0) or 0),
            'picked':    float(r.get('Picked', 0) or 0),
            'variance':  float(r.get('Variance', 0) or 0),
            'status':    str(r.get('Status', '') or '').strip() or None,
        })
    if rows:
        sb.table("summary_data").upsert(rows, on_conflict="load_id").execute()
    APIManager.invalidate("Summary_Data")


def _sb_clear_table(sheet_name: str):
    """Clear all data from a table."""
    sb = get_supabase_client()
    tbl = APIManager._tbl(sheet_name)
    sb.table(tbl).delete().neq("id", 0).execute()
    APIManager.invalidate(sheet_name)


# ============================================================
# 4. User Management & Login
# ============================================================
def init_users():
    """Ensure default users exist (idempotent via UPSERT)."""
    sb = get_supabase_client()
    defaults = [
        {"username": "admin", "password": "admin@123", "role": "admin"},
        {"username": "sys",   "password": "sys@123",   "role": "SysUser"},
        {"username": "user",  "password": "user@123",  "role": "user"},
    ]
    sb.table("users").upsert(defaults, on_conflict="username").execute()
    return APIManager.read_sheet(None, "Users")


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
                users_df = init_users()
            except Exception as e:
                st.error("Supabase සම්බන්ධ වීමේ දෝෂයක්. Secrets පරීක්ෂා කරන්න.")
                return False

            user = st.text_input("Username", key="login_user")
            pw = st.text_input("Password", type="password", key="login_pw")

            if st.button("Login", type="primary", use_container_width=True):
                user_match = users_df[
                    (users_df['Username'] == user) & (users_df['Password'] == str(pw))
                ]
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


# ============================================================
# 5. Inventory Logic  (unchanged — works on DataFrames)
# ============================================================
def get_damage_pallets(_sh):
    try:
        dmg_df = get_safe_dataframe(None, "Damage_Items")
        if not dmg_df.empty and 'Pallet' in dmg_df.columns and 'Actual Qty' in dmg_df.columns:
            dmg_df['Actual Qty'] = pd.to_numeric(dmg_df['Actual Qty'], errors='coerce').fillna(0)
            dmg_summary = dmg_df.groupby('Pallet')['Actual Qty'].sum().reset_index()
            dmg_summary.columns = ['Pallet', 'Damage_Qty']
            return dmg_summary
    except:
        pass
    return pd.DataFrame(columns=['Pallet', 'Damage_Qty'])


def reconcile_inventory(inv_df, _sh):
    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_lower = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    pallet_col = inv_col_lower.get('pallet', 'Pallet')
    actual_col = inv_col_lower.get('actual qty', 'Actual Qty')
    if actual_col not in inv_df.columns:
        actual_col = next((c for c in inv_df.columns if 'actual' in c.lower()), actual_col)

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)

    try:
        pick_history = APIManager.read_sheet(None, "Master_Pick_Data")
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
        dmg_summary = get_damage_pallets(None)
        if not dmg_summary.empty:
            damage_pallet_set = set(dmg_summary['Pallet'].astype(str).str.strip().tolist())
            inv_df = inv_df[~inv_df[pallet_col].astype(str).str.strip().isin(damage_pallet_set)].reset_index(drop=True)
    except Exception as e:
        st.warning(f"Damage Exclude Error: {e}")

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)
    inv_df = inv_df[inv_df[actual_col] > 0].reset_index(drop=True)
    return inv_df


def generate_unique_load_id(_sh, so_num, so_counts):
    hist_df = get_safe_dataframe(None, "Load_History")
    existing_ids = set()
    if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
        existing_ids = set(hist_df['Generated Load ID'].astype(str).tolist())
    count = so_counts.get(so_num, 0)
    while True:
        count += 1
        candidate = f"SO-{so_num}-{count:03d}"
        if candidate not in existing_ids:
            return candidate, count


def process_picking(inv_df, req_df, batch_id, sh=None, inv_original=None):
    pick_rows, partial_rows, summary = [], [], []

    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    supplier_col = next((inv_col_map[k] for k in inv_col_map if k == 'supplier'), None)
    pick_id_col  = next((inv_col_map[k] for k in inv_col_map if k in ('pick id', 'pickid')), None)
    pallet_col   = next((inv_col_map[k] for k in inv_col_map if k == 'pallet'), 'Pallet')

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
            orig_qty_map = dict(zip(
                inv_orig_norm[orig_pallet_col].astype(str).str.strip(),
                inv_orig_norm[orig_actual_col].astype(float)
            ))

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
    try:
        existing_partial = APIManager.read_sheet(None, "Master_Partial_Data")
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
                    p_row['Actual Qty']         = take
                    p_row['Pick Quantity']      = take
                    p_row['Pick Id']            = str(item[pick_id_col]) if pick_id_col and pick_id_col in item.index else ''
                    p_row['Supplier']           = str(item[supplier_col]) if supplier_col and supplier_col in item.index else upc
                    p_row['Batch ID']           = batch_id
                    p_row['SO Number']          = so_num
                    p_row['Generated Load ID']  = lid
                    p_row['Country Name']       = country
                    p_row['Remark']             = ''
                    p_row['Order Type']         = 'Sample Orders'
                    p_row['Order Number']       = lid
                    p_row['Store Order Number'] = lid
                    p_row['Customer Po Number'] = f"{country}-{lid}"
                    p_row['Load Id']            = lid
                    pick_rows.append(p_row)

                    pallet_val = str(item[pallet_col]) if pallet_col in item.index else ''
                    orig_qty = orig_qty_map.get(pallet_val, current_avail)
                    is_partial = (take < current_avail) or (orig_qty > take)

                    if is_partial:
                        def _get(col_name):
                            c = inv_col_map.get(col_name.lower())
                            return str(item[c]) if c and c in item.index else ''
                        gen_pallet_id = make_unique_gen_pallet_id(pallet_val)
                        pick_rows[-1]['Remark'] = 'Partial'
                        pick_rows[-1]['Gen Pallet ID'] = gen_pallet_id
                        partial_rows.append({
                            'Batch ID':           batch_id,
                            'SO Number':          so_num,
                            'Pallet':             pallet_val,
                            'Supplier':           p_row['Supplier'],
                            'Load ID':            lid,
                            'Country Name':       country,
                            'Actual Qty':         orig_qty,
                            'Partial Qty':        take,
                            'Gen Pallet ID':      gen_pallet_id,
                            'Balance Qty':        orig_qty - take,
                            'Location Id':        _get('location id'),
                            'Lot Number':         _get('lot number'),
                            'Color':              _get('color'),
                            'Size':               _get('size'),
                            'Style':              _get('style'),
                            'Customer Po Number': _get('customer po number'),
                        })
                    else:
                        pick_rows[-1]['Gen Pallet ID'] = ''

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

    PICK_REPORT_COLS = MASTER_PICK_HEADERS + ['Gen Pallet ID']
    if pick_rows:
        pick_df = pd.DataFrame(pick_rows, columns=PICK_REPORT_COLS)
        pick_df['Actual Qty'] = pd.to_numeric(pick_df['Actual Qty'], errors='coerce').fillna(0)
        pick_df = pick_df[pick_df['Actual Qty'] > 0].reset_index(drop=True)
    else:
        pick_df = pd.DataFrame(columns=PICK_REPORT_COLS)

    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)


def generate_inventory_details_report(inv_df, _sh):
    try:
        pick_df = get_safe_dataframe(None, "Master_Pick_Data")
        damage_lookup = {}
        try:
            dmg_df = get_safe_dataframe(None, "Damage_Items")
            if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                dmg_df['_pallet'] = dmg_df['Pallet'].astype(str).str.strip()
                dmg_df['_remark'] = dmg_df.get('Remark', 'Damage').astype(str).str.strip()
                dmg_df['_qty']    = dmg_df.get('Actual Qty', '').astype(str).str.strip()
                dmg_df['_entry']  = dmg_df.apply(
                    lambda r: f"DAMAGE: {r['_remark']} (Qty:{r['_qty']})" if r['_qty'] else f"DAMAGE: {r['_remark']}", axis=1
                )
                for p, grp in dmg_df.groupby('_pallet'):
                    if p:
                        damage_lookup[p] = ' | '.join(grp['_entry'].tolist())
        except Exception:
            pass

        pick_by_pallet = {}
        if not pick_df.empty and 'Pallet' in pick_df.columns:
            pick_df['_pkey'] = pick_df['Pallet'].astype(str).str.strip()
            for pkey, grp in pick_df.groupby('_pkey'):
                pick_by_pallet[pkey] = grp.to_dict('records')

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
            pallet_pick_rows = pick_by_pallet.get(pallet)
            if pallet_pick_rows:
                for pick_rec in pallet_pick_rows:
                    row = inv_row.copy()
                    row['Batch ID'] = pick_rec.get('Batch ID', '')
                    row['SO Number'] = pick_rec.get('SO Number', '')
                    row['Generated Load ID'] = pick_rec.get('Generated Load ID', pick_rec.get('Load Id', ''))
                    row['Country Name'] = pick_rec.get('Country Name', '')
                    row['Pick Quantity'] = pick_rec.get('Pick Quantity', pick_rec.get('Actual Qty', ''))
                    row['Remark'] = pick_rec.get('Remark', 'Allocated')
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

        return pd.DataFrame(report_rows)
    except Exception as e:
        st.error(f"Report Generation Error: {e}")
        return pd.DataFrame()


# ============================================================
# 6. App UI & Navigation  (same as original, DB calls updated)
# ============================================================
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
        sh = get_master_workbook()  # returns True (connection check)
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
                with st.spinner("🔄 Processing Data & Saving to Supabase..."):

                    inv = pd.read_csv(inv_file, keep_default_na=False, na_values=['']) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file, keep_default_na=False, na_values=[''])
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
                        st.error("⚠️ Files swapped! '1. Upload Inventory Report' හි Inventory file සහ '2. Upload Customer Requirement' හි Customer Requirement file upload කරන්න.")
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
                    hist_df = get_safe_dataframe(None, "Load_History")

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
                        new_hist_entries.append({
                            'Batch ID': batch_id,
                            'Generated Load ID': candidate_lid,
                            'SO Number': so_num,
                            'Country Name': data['Country Name'].iloc[0],
                            'SHIP MODE': data['SHIP MODE: (SEA/AIR)'].iloc[0],
                            'Date': datetime.now().isoformat(),
                            'Pick Status': 'Pending',
                        })

                    req['Generated Load ID'] = req['Group'].map(load_id_map)
                    if new_hist_entries:
                        APIManager.append_rows_to_sheet(
                            None, "Load_History", SHEET_HEADERS["Load_History"],
                            [[e['Batch ID'], e['Generated Load ID'], e['SO Number'],
                              e['Country Name'], e['SHIP MODE'], e['Date'], e['Pick Status']]
                             for e in new_hist_entries]
                        )

                    inv_original = inv.copy()
                    inv = reconcile_inventory(inv, None)

                    pick_df, part_df, summ_df = process_picking(inv, req, batch_id, None, inv_original=inv_original)

                    # Cannot-Pick Diagnostic
                    cannot_pick_rows = []
                    try:
                        inv_orig = pd.read_csv(inv_file, keep_default_na=False, na_values=['']) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file, keep_default_na=False, na_values=[''])
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

                        mpd_df = get_safe_dataframe(None, "Master_Pick_Data")
                        mpd_picked = {}
                        if not mpd_df.empty and 'Pallet' in mpd_df.columns and 'Actual Qty' in mpd_df.columns:
                            mpd_df['Actual Qty'] = pd.to_numeric(mpd_df['Actual Qty'], errors='coerce').fillna(0)
                            for p, grp in mpd_df.groupby('Pallet'):
                                mpd_picked[str(p).strip()] = grp['Actual Qty'].sum()

                        dmg_df = get_safe_dataframe(None, "Damage_Items")
                        dmg_pallets = set()
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            dmg_pallets = set(dmg_df['Pallet'].astype(str).str.strip().tolist())

                        inv_orig_by_upc = {}
                        if orig_sup_col in inv_orig.columns:
                            for upc_key, grp in inv_orig.groupby(orig_sup_col):
                                inv_orig_by_upc[str(upc_key).strip()] = grp

                        for _, summ_row in summ_df.iterrows():
                            upc      = str(summ_row.get('UPC', ''))
                            picked   = float(summ_row.get('Picked', 0))
                            requested = float(summ_row.get('Requested', 0))
                            if picked >= requested:
                                continue
                            missing = requested - picked
                            upc_pallets = inv_orig_by_upc.get(upc)
                            if upc_pallets is None:
                                continue
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
                                    'UPC': upc, 'Pallet': pallet,
                                    'Inv Actual Qty': int(orig_qty), 'Already Picked': int(already),
                                    'Available Now': int(avail), 'Requested': int(requested),
                                    'Shortage': int(missing), 'Reason': reason
                                })
                    except Exception:
                        pass

                    if not pick_df.empty:
                        pick_save_df = pick_df[MASTER_PICK_HEADERS] if all(c in pick_df.columns for c in MASTER_PICK_HEADERS) else pick_df[[c for c in MASTER_PICK_HEADERS if c in pick_df.columns]]
                        APIManager.append_rows_to_sheet(
                            None, "Master_Pick_Data", MASTER_PICK_HEADERS,
                            pick_save_df.astype(str).replace('nan', '').values.tolist()
                        )

                    if not part_df.empty:
                        mpd_headers = SHEET_HEADERS["Master_Partial_Data"]
                        for col in mpd_headers:
                            if col not in part_df.columns:
                                part_df[col] = ''
                        APIManager.append_rows_to_sheet(
                            None, "Master_Partial_Data", mpd_headers,
                            part_df[mpd_headers].astype(str).replace('nan', '').values.tolist()
                        )

                    # Summary: upsert per load_id
                    _sb_upsert_summary(summ_df)

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        wb = writer.book
                        if not pick_df.empty:
                            WMS_ONLY_COLS = ['Batch ID', 'SO Number', 'Generated Load ID',
                                             'Country Name', 'Pick Quantity', 'Remark',
                                             'Actual Qty', 'Order Type', 'Order Number',
                                             'Store Order Number', 'Customer Po Number', 'Load Id']
                            inv_orig_norm = inv_original.copy()
                            inv_orig_norm.columns = [str(c).strip() for c in inv_orig_norm.columns]
                            inv_orig_norm_map = {str(c).strip().lower(): str(c).strip() for c in inv_orig_norm.columns}
                            inv_orig_pallet_col = inv_orig_norm_map.get('pallet', 'Pallet')
                            inv_orig_norm['_pallet_key'] = inv_orig_norm[inv_orig_pallet_col].astype(str).str.strip()
                            inv_orig_dedup = inv_orig_norm.drop_duplicates(subset='_pallet_key').set_index('_pallet_key')

                            pick_df_wms = pick_df[MASTER_PICK_HEADERS + ['Gen Pallet ID']].copy()
                            pick_df_wms['_pallet_key'] = pick_df_wms['Pallet'].astype(str).str.strip()

                            NON_WMS_COLS = [c for c in MASTER_PICK_HEADERS if c not in WMS_ONLY_COLS]
                            inv_non_wms = inv_orig_dedup[[
                                col for col in [inv_orig_norm_map.get(c.strip().lower()) for c in NON_WMS_COLS]
                                if col is not None and col in inv_orig_dedup.columns
                            ]].copy()
                            inv_col_rename = {}
                            for c in NON_WMS_COLS:
                                matched = inv_orig_norm_map.get(c.strip().lower())
                                if matched and matched in inv_non_wms.columns and matched != c:
                                    inv_col_rename[matched] = c
                            inv_non_wms = inv_non_wms.rename(columns=inv_col_rename)
                            inv_non_wms.index.name = '_pallet_key'
                            inv_non_wms = inv_non_wms.reset_index()

                            pick_df_excel = pick_df_wms.merge(inv_non_wms, on='_pallet_key', how='left', suffixes=('', '_inv'))
                            for c in NON_WMS_COLS:
                                inv_c = c + '_inv'
                                if inv_c in pick_df_excel.columns:
                                    pick_df_excel[c] = pick_df_excel[inv_c].where(
                                        pick_df_excel[inv_c].notna(), pick_df_excel.get(c, '')
                                    )
                                    pick_df_excel.drop(columns=[inv_c], inplace=True)
                            EXCEL_PICK_COLS = MASTER_PICK_HEADERS + ['Gen Pallet ID']
                            for col in EXCEL_PICK_COLS:
                                if col not in pick_df_excel.columns:
                                    pick_df_excel[col] = ''
                            pick_df_excel = pick_df_excel[EXCEL_PICK_COLS].drop(
                                columns=[c for c in ['_pallet_key'] if c in pick_df_excel.columns], errors='ignore'
                            )
                            pick_df_excel.to_excel(writer, sheet_name='Pick_Report', index=False)

                            hdr_fmt     = wb.add_format({'bold': True, 'bg_color': '#1A1A1A', 'font_color': '#FFFFFF', 'border': 1})
                            partial_hdr = wb.add_format({'bold': True, 'bg_color': '#1A6B3C', 'font_color': '#FFFFFF', 'border': 1})
                            int_fmt     = wb.add_format({'num_format': '0'})
                            float_fmt   = wb.add_format({'num_format': '0.######'})

                            BIG_INT_COLS = {'Supplier', 'Invoice Number1', 'Stored Attribute Id', 'Gate Pass Id', 'Client So Line', 'Asn Line Number', 'S Qty'}
                            FLOAT_COLS   = {'Received Gross Weight', 'Current Gross Weight', 'Received Net Weight', 'Current Net Weight', 'Cbm', 'Container Type'}

                            ws_pick_xl = writer.sheets['Pick_Report']
                            for ci, col_name in enumerate(EXCEL_PICK_COLS):
                                ws_pick_xl.write(0, ci, col_name, partial_hdr if col_name == 'Gen Pallet ID' else hdr_fmt)
                                ws_pick_xl.set_column(ci, ci, 20 if col_name == 'Gen Pallet ID' else 18)
                                if col_name in BIG_INT_COLS:
                                    for ri in range(1, len(pick_df_excel) + 1):
                                        val = pick_df_excel.iloc[ri-1][col_name]
                                        if pd.notna(val) and str(val) not in ('', 'nan', 'None'):
                                            try:
                                                ws_pick_xl.write_number(ri, ci, int(float(str(val))), int_fmt)
                                            except:
                                                ws_pick_xl.write(ri, ci, str(val))
                                elif col_name in FLOAT_COLS:
                                    for ri in range(1, len(pick_df_excel) + 1):
                                        val = pick_df_excel.iloc[ri-1][col_name]
                                        if pd.notna(val) and str(val) not in ('', 'nan', 'None'):
                                            try:
                                                ws_pick_xl.write_number(ri, ci, float(str(val)), float_fmt)
                                            except:
                                                ws_pick_xl.write(ri, ci, str(val))
                            ws_pick_xl.freeze_panes(1, 0)

                        if not part_df.empty:
                            part_df.to_excel(writer, sheet_name='Partial_Report', index=False)
                            hdr_fmt2 = wb.add_format({'bold': True, 'bg_color': '#1A1A1A', 'font_color': '#FFFFFF', 'border': 1})
                            ws_part_xl = writer.sheets['Partial_Report']
                            for ci, col_name in enumerate(part_df.columns):
                                ws_part_xl.write(0, ci, col_name, hdr_fmt2)
                                ws_part_xl.set_column(ci, ci, 18)
                            ws_part_xl.freeze_panes(1, 0)

                        if not summ_df.empty:
                            summ_df.to_excel(writer, sheet_name='Variance_Summary', index=False)

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

            cannot_rows = st.session_state.get('cannot_pick_rows', [])
            if cannot_rows:
                cp_df = pd.DataFrame(cannot_rows)
                st.divider()
                st.markdown("### ⚠️ Pick කරන්න නොහැකි Pallets — හේතු සහිතව")
                st.caption(f"Pick නොවූ හෝ Shortage ඇති UPC {cp_df['UPC'].nunique()} ක, Pallet {len(cp_df)} ක් සඳහා:")

                def highlight_reason(row):
                    if '✅' in str(row.get('Reason', '')):
                        return ['background-color: #fff3cd'] * len(row)
                    elif '🔴' in str(row.get('Reason', '')):
                        return ['background-color: #ffe0e0'] * len(row)
                    elif '⚠️' in str(row.get('Reason', '')):
                        return ['background-color: #fff8e1'] * len(row)
                    elif '❌' in str(row.get('Reason', '')):
                        return ['background-color: #fce4ec'] * len(row)
                    return [''] * len(row)

                try:
                    styled = cp_df.style.apply(highlight_reason, axis=1)
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                except:
                    st.dataframe(cp_df.astype(str), use_container_width=True, hide_index=True)

                st.markdown("**හේතු Summary:**")
                reason_summary = cp_df['Reason'].apply(lambda r: r.split('—')[0].split('(')[0].strip()).value_counts()
                for reason, count in reason_summary.items():
                    st.markdown(f"- {reason}: **{count} pallets**")

                out_cp = io.BytesIO()
                with pd.ExcelWriter(out_cp, engine='xlsxwriter') as writer:
                    cp_df.to_excel(writer, sheet_name='Cannot_Pick', index=False)
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
            APIManager.invalidate()
            st.rerun()

        _batch = APIManager.batch_read(None, ["Load_History", "Summary_Data", "Master_Pick_Data"])
        hist_df = _batch["Load_History"]
        summ_df = _batch["Summary_Data"]
        pick_df = _batch["Master_Pick_Data"]

        total_loads     = hist_df['Generated Load ID'].nunique() if not hist_df.empty and 'Generated Load ID' in hist_df.columns else 0
        total_picks     = len(pick_df) if not pick_df.empty else 0
        pending_loads   = len(hist_df[hist_df['Pick Status'] == 'Pending']) if not hist_df.empty and 'Pick Status' in hist_df.columns else 0
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
                    filter_col1, filter_col2 = st.columns([2, 4])
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
                        summ_df['Variance']  = pd.to_numeric(summ_df.get('Variance', 0), errors='coerce').fillna(0)
                        summ_df['Requested'] = pd.to_numeric(summ_df.get('Requested', 0), errors='coerce').fillna(0)
                        summ_df['Picked']    = pd.to_numeric(summ_df.get('Picked', 0), errors='coerce').fillna(0)
                        for lid_s in summ_df['Load ID'].dropna().unique():
                            rows = summ_df[summ_df['Load ID'].astype(str) == str(lid_s)]
                            summ_by_load[str(lid_s)] = {
                                'requested': rows['Requested'].sum(),
                                'picked':    rows['Picked'].sum(),
                                'variance':  rows['Variance'].sum()
                            }

                    zero_pick_ids, shortage_ids, full_pick_ids = [], [], []
                    for lid in load_ids:
                        s = summ_by_load.get(str(lid), {})
                        req_q    = s.get('requested', 0)
                        picked_q = s.get('picked', 0)
                        var_q    = s.get('variance', 0)
                        if picked_q == 0 and req_q > 0:
                            zero_pick_ids.append(lid)
                        elif var_q > 0:
                            shortage_ids.append(lid)
                        else:
                            full_pick_ids.append(lid)

                    STATUS_OPTIONS = ["Pending", "PL Pending", "Processing", "Completed", "Cancelled"]

                    pick_counts_by_lid = {}
                    pick_qty_by_lid    = {}
                    if not pick_df.empty:
                        load_id_col_pick = next((c for c in pick_df.columns if str(c).strip().lower() in ('load id', 'loadid', 'load_id')), None)
                        actual_col_pick  = next((c for c in pick_df.columns if str(c).strip().lower() == 'actual qty'), None)
                        if load_id_col_pick:
                            _lid_series = pick_df[load_id_col_pick].astype(str).str.strip()
                            pick_counts_by_lid = _lid_series.value_counts().to_dict()
                            if actual_col_pick:
                                _qty_grp = pick_df.groupby(_lid_series)[actual_col_pick].apply(
                                    lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum()
                                )
                                pick_qty_by_lid = _qty_grp.to_dict()

                    def render_load_list(id_list, category_color, category_label):
                        st.markdown(f"""
                        <div style="display:grid; grid-template-columns:2fr 1fr 1.2fr 1fr 1fr 1fr 1fr 1.5fr; gap:4px;
                             background:{category_color}15; border:1px solid {category_color}40;
                             border-radius:8px 8px 0 0; padding:7px 12px;
                             font-size:11px; font-weight:700; color:#444; margin-top:4px;">
                            <div>Load ID</div><div>SO</div><div>Country</div><div>Ship</div>
                            <div>Date</div><div>Lines</div><div>Qty</div><div>Status</div>
                        </div>
                        """, unsafe_allow_html=True)

                        for lid in id_list:
                            load_row   = active_loads[active_loads['Generated Load ID'] == lid].iloc[0]
                            status     = str(load_row.get('Pick Status', 'Pending'))
                            so_num     = str(load_row.get('SO Number', '-'))
                            country    = str(load_row.get('Country Name', '-'))
                            ship       = str(load_row.get('SHIP MODE', '-'))
                            date       = str(load_row.get('Date', '-'))[:10]
                            lid_key    = str(lid).strip()
                            pick_count = pick_counts_by_lid.get(lid_key, 0)
                            pick_qty_v = pick_qty_by_lid.get(lid_key, 0)
                            s          = summ_by_load.get(str(lid), {})
                            variance   = s.get('variance', 0)

                            status_bg  = {'Pending': '#fff3cd', 'Processing': '#cce5ff'}.get(status, '#f0f0f0')
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
                                <div>{so_num}</div><div>{country}</div><div>{ship}</div>
                                <div>{date}</div><div><b>{pick_count}</b></div>
                                <div><b>{int(pick_qty_v)}</b></div>
                                <div><span style="background:{status_bg}; color:{status_col}; font-size:10px; font-weight:600; padding:2px 8px; border-radius:10px;">{status_dot} {status}</span></div>
                            </div>
                            """
                            st.markdown(row_html, unsafe_allow_html=True)

                            c1, c2 = st.columns([3, 1])
                            safe_idx = STATUS_OPTIONS.index(status) if status in STATUS_OPTIONS else 0
                            new_st = c1.selectbox("", STATUS_OPTIONS, index=safe_idx, key=f"st_{lid}", label_visibility="collapsed")
                            if c2.button("💾 Save", key=f"upd_{lid}", use_container_width=True):
                                try:
                                    _sb_update_load_status(str(lid), new_st)
                                    if new_st == "Cancelled":
                                        _sb_delete_mpd_by_load_id([str(lid)])
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
            st.subheader("📑 Download Total Report by Upload Batch")
            if not hist_df.empty and 'Batch ID' in hist_df.columns:
                available_batches = hist_df['Batch ID'].dropna().unique()
                if len(available_batches) > 0:
                    selected_batch = st.selectbox("Select Requirement Batch ID:", available_batches)
                    if st.button("Generate Total Batch Report"):
                        with st.spinner("Generating Total Report..."):
                            batch_picks = pick_df[pick_df['Batch ID'] == selected_batch] if not pick_df.empty and 'Batch ID' in pick_df.columns else pd.DataFrame()
                            batch_summ  = summ_df[summ_df['Batch ID'] == selected_batch] if not summ_df.empty and 'Batch ID' in summ_df.columns else pd.DataFrame()
                            out_total   = io.BytesIO()
                            with pd.ExcelWriter(out_total, engine='xlsxwriter') as writer:
                                if not batch_picks.empty: batch_picks.to_excel(writer, sheet_name='Pick_Report', index=False)
                                if not batch_summ.empty:  batch_summ.to_excel(writer, sheet_name='Variance_Summary', index=False)
                            st.download_button("⬇️ Download Batch Excel", data=out_total.getvalue(), file_name=f"Total_Report_{selected_batch}.xlsx", mime="application/vnd.ms-excel")
            st.divider()

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
                    "Load Id": "Load Id", "Pallet": "Pallet",
                    "Supplier (Product UPC)": "Supplier", "SO Number": "SO Number"
                }
                col_map_summ = {
                    "Load Id": "Load ID", "Pallet": None,
                    "Supplier (Product UPC)": "UPC", "SO Number": "SO Number"
                }
                filtered_picks = pd.DataFrame()
                if not pick_df.empty:
                    pick_search_col = col_map_pick[search_by]
                    actual_col_name = next((c for c in pick_df.columns if str(c).strip().lower() == pick_search_col.strip().lower()), None)
                    if actual_col_name:
                        if search_by == "Load Id":
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.strip() == str(search_term).strip()]
                        else:
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.contains(str(search_term).strip(), case=False, na=False)]

                filtered_summ = pd.DataFrame()
                summ_search_key = col_map_summ[search_by]
                if not summ_df.empty and summ_search_key:
                    actual_summ_col = next((c for c in summ_df.columns if str(c).strip().lower() == summ_search_key.strip().lower()), None)
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
                        out_pick_dl = io.BytesIO()
                        with pd.ExcelWriter(out_pick_dl, engine='xlsxwriter') as writer:
                            filtered_picks.to_excel(writer, sheet_name='Pick_Report', index=False)
                            if not filtered_summ.empty:
                                filtered_summ.to_excel(writer, sheet_name='Variance_Summary', index=False)
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
                            use_container_width=True, type="primary"
                        )
                        dl_qty   = pd.to_numeric(filtered_picks.get('Actual Qty', pd.Series()), errors='coerce').sum()
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
                        inv_data = pd.read_csv(inv_report_file, keep_default_na=False, na_values=['']) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file, keep_default_na=False, na_values=[''])
                        report_df = generate_inventory_details_report(inv_data, None)
                        if not report_df.empty:
                            st.success(f"✅ Total rows: {len(report_df)}")
                            col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                            if 'Allocation Status' in report_df.columns:
                                col_r1.metric("Total Lines", len(report_df))
                                col_r2.metric("✅ Picked",    len(report_df[report_df['Allocation Status'] == 'Picked']))
                                col_r3.metric("🟢 Available", len(report_df[report_df['Allocation Status'] == 'Available']))
                                col_r4.metric("🔴 Damage",    len(report_df[report_df['Allocation Status'] == 'Damage']))
                            st.dataframe(report_df.astype(str), use_container_width=True)
                            out_basic = io.BytesIO()
                            with pd.ExcelWriter(out_basic, engine='xlsxwriter') as writer:
                                report_df.to_excel(writer, sheet_name='Inventory_Details', index=False)
                                wb = writer.book
                                ws_b = writer.sheets['Inventory_Details']
                                for fmt, col_val in [('#FFE0E0', 'Damage'), ('#E8F5E9', 'Picked'), ('#E3F2FD', 'Available')]:
                                    f = wb.add_format({'bg_color': fmt})
                                    if 'Allocation Status' in report_df.columns:
                                        for ri, sv in enumerate(report_df['Allocation Status'], 1):
                                            if sv == col_val: ws_b.set_row(ri, None, f)
                            st.download_button(
                                "⬇️ Download Basic Report", data=out_basic.getvalue(),
                                file_name=f"Inventory_Basic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.ms-excel", use_container_width=True
                            )
                        else:
                            st.warning("Report generate කිරීම අසාර්ථක විය.")

            with tab_formatted:
                st.caption("Notepad headers order → Pick Quantity → Damage columns → Destination Country → Order NO → Partial Pallet replace")
                if st.button("📊 Generate Formatted Pick Report", type="primary", use_container_width=True, key="gen_fmt"):
                    with st.spinner("Generating Formatted Report..."):
                        inv_data = pd.read_csv(inv_report_file, keep_default_na=False, na_values=['']) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file, keep_default_na=False, na_values=[''])
                        inv_data.columns = [str(c).strip() for c in inv_data.columns]
                        inv_col_map_r = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

                        CANONICAL = [
                            'Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                            'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
                            'Lot Number', 'Style', 'Color', 'Size', 'Inventory Type', 'Actual Qty'
                        ]
                        rename_map = {}
                        for canon in CANONICAL:
                            matched = inv_col_map_r.get(canon.strip().lower())
                            if matched and matched != canon:
                                rename_map[matched] = canon
                        if rename_map:
                            inv_data = inv_data.rename(columns=rename_map)
                        inv_col_map_r = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

                        _inv_aq_col  = inv_col_map_r.get('actual qty', 'Actual Qty')
                        _inv_pal_col = inv_col_map_r.get('pallet', 'Pallet')

                        REPORT_HEADERS = [
                            'Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                            'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
                            'Lot Number', 'Style', 'Color', 'Size', 'Client So 2',
                            'Inventory Type', 'Actual Qty'
                        ]

                        _rpt_sheets = APIManager.batch_read(None, ["Master_Pick_Data", "Damage_Items", "Master_Partial_Data"])
                        mpd_df  = _rpt_sheets["Master_Pick_Data"]
                        mpd_col = {str(c).strip().lower(): str(c).strip() for c in (mpd_df.columns if not mpd_df.empty else [])}

                        pick_qty_map     = {}
                        pick_country_map = {}
                        pick_loadid_map  = {}

                        if not mpd_df.empty:
                            p_col  = mpd_col.get('pallet', 'Pallet')
                            aq_col = mpd_col.get('actual qty', 'Actual Qty')
                            cn_col = mpd_col.get('country name', 'Country Name')
                            gl_col = mpd_col.get('generated load id', 'Generated Load ID')
                            _mpd   = mpd_df[[p_col, aq_col, cn_col, gl_col]].copy()
                            _mpd['_pkey'] = _mpd[p_col].astype(str).str.strip()
                            _mpd[aq_col]  = pd.to_numeric(_mpd[aq_col], errors='coerce').fillna(0)
                            pick_qty_map  = _mpd.groupby('_pkey')[aq_col].sum().to_dict()
                            _last = _mpd.drop_duplicates('_pkey', keep='last').set_index('_pkey')
                            pick_country_map = _last[cn_col].astype(str).to_dict()
                            pick_loadid_map  = _last[gl_col].astype(str).to_dict()

                        dmg_df = _rpt_sheets["Damage_Items"]
                        damage_remarks = []
                        dmg_pallet_remark_qty = {}
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns and 'Remark' in dmg_df.columns:
                            _dmg = dmg_df.copy()
                            _dmg['_pkey'] = _dmg['Pallet'].astype(str).str.strip()
                            _dmg['_rmk']  = _dmg['Remark'].astype(str).str.strip()
                            _dmg['_dqty'] = pd.to_numeric(_dmg.get('Actual Qty', 0), errors='coerce').fillna(0)
                            damage_remarks = list(_dmg['_rmk'].unique())
                            _dmg_grp = _dmg.groupby(['_pkey', '_rmk'])['_dqty'].sum()
                            dmg_pallet_remark_qty = {(p, r): v for (p, r), v in _dmg_grp.items()}

                        partial_df = _rpt_sheets["Master_Partial_Data"]
                        partial_map = {}
                        gen_to_orig = {}
                        if not partial_df.empty:
                            pc     = {str(c).strip().lower(): str(c).strip() for c in partial_df.columns}
                            pp_col = pc.get('pallet', 'Pallet')
                            pq_col = pc.get('partial qty', 'Partial Qty')
                            pg_col = pc.get('gen pallet id', 'Gen Pallet ID')
                            pl_col = pc.get('load id', 'Load ID')
                            pa_col = pc.get('actual qty', 'Actual Qty')
                            _pdf   = partial_df[[pp_col, pq_col, pg_col, pl_col, pa_col]].copy()
                            _pdf[pq_col] = pd.to_numeric(_pdf[pq_col], errors='coerce').fillna(0)
                            _pdf[pa_col] = pd.to_numeric(_pdf[pa_col], errors='coerce').fillna(0)
                            for row in _pdf.itertuples(index=False):
                                opallet  = str(getattr(row, pp_col.replace(' ', '_'), '')).strip()
                                gpallet  = str(getattr(row, pg_col.replace(' ', '_'), '')).strip()
                                pqty     = float(getattr(row, pq_col.replace(' ', '_'), 0))
                                loadid_p = str(getattr(row, pl_col.replace(' ', '_'), '')).strip()
                                aqty     = float(getattr(row, pa_col.replace(' ', '_'), 0))
                                if opallet:
                                    if opallet not in partial_map:
                                        partial_map[opallet] = []
                                    partial_map[opallet].append({'gen_pallet': gpallet, 'partial_qty': pqty, 'load_id': loadid_p, 'mpd_actual': aqty})
                                if gpallet and opallet:
                                    gen_to_orig[gpallet] = opallet

                        damage_pallets = set()
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            damage_pallets = set(str(p).strip() for p in dmg_df['Pallet'].dropna())

                        def build_row(inv_row, override_pallet=None, override_actual_qty=None):
                            row = {}
                            for h in REPORT_HEADERS:
                                if h == 'Pallet':
                                    row[h] = override_pallet if override_pallet is not None else (inv_row[_inv_pal_col] if _inv_pal_col in inv_row.index else '')
                                elif h == 'Actual Qty':
                                    row[h] = override_actual_qty if override_actual_qty is not None else (inv_row[_inv_aq_col] if _inv_aq_col in inv_row.index else '')
                                elif h == 'Client So 2':
                                    cs_col = inv_col_map_r.get('client so', 'Client So')
                                    row[h] = inv_row[cs_col] if cs_col in inv_row.index else ''
                                elif h in inv_row.index:
                                    row[h] = inv_row[h]
                                else:
                                    fb = inv_col_map_r.get(h.strip().lower())
                                    row[h] = inv_row[fb] if fb and fb in inv_row.index else ''
                            return row

                        fmt_rows = []
                        for _, inv_row in inv_data.iterrows():
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce')
                            if pd.isna(inv_actual_qty): inv_actual_qty = 0
                            is_damaged   = orig_pallet in damage_pallets
                            total_picked = pick_qty_map.get(orig_pallet, 0)

                            if orig_pallet in gen_to_orig:
                                real_orig       = gen_to_orig[orig_pallet]
                                matching_partial = next((p for p in partial_map.get(real_orig, []) if p['gen_pallet'] == orig_pallet), None)
                                if matching_partial and abs(inv_actual_qty - matching_partial['partial_qty']) <= 0.01:
                                    row = build_row(inv_row)
                                    row['Pick Quantity']       = matching_partial['partial_qty']
                                    row['Destination Country'] = pick_country_map.get(real_orig, '')
                                    row['Order NO']            = matching_partial['load_id']
                                    for rmk in damage_remarks:
                                        row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                    row['ATS'] = ''
                                    fmt_rows.append(row)
                                continue

                            partials = partial_map.get(orig_pallet, [])
                            if partials:
                                last_p       = partials[-1]
                                last_balance = last_p['mpd_actual'] - last_p['partial_qty']
                                if last_balance <= 0.01:
                                    tally_qty  = last_p['mpd_actual']
                                    tally_type = 'picked'
                                else:
                                    tally_qty  = last_balance
                                    tally_type = 'balance'

                                if abs(inv_actual_qty - tally_qty) <= 0.01:
                                    row = build_row(inv_row)
                                    if tally_type == 'balance':
                                        row['Pick Quantity']       = ''
                                        row['Destination Country'] = ''
                                        row['Order NO']            = ''
                                        for rmk in damage_remarks:
                                            row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                        row['ATS'] = int(inv_actual_qty) if not is_damaged else ''
                                    else:
                                        row['Pick Quantity']       = last_p['partial_qty']
                                        row['Destination Country'] = pick_country_map.get(orig_pallet, '')
                                        row['Order NO']            = last_p['load_id']
                                        for rmk in damage_remarks:
                                            row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                        row['ATS'] = ''
                                    fmt_rows.append(row)
                                else:
                                    mpd_actual        = partials[0]['mpd_actual'] if partials[0]['mpd_actual'] > 0 else inv_actual_qty
                                    total_partial_qty = sum(p['partial_qty'] for p in partials)
                                    for par_entry in partials:
                                        row = build_row(inv_row, override_pallet=par_entry['gen_pallet'], override_actual_qty=par_entry['partial_qty'])
                                        row['Pick Quantity']       = par_entry['partial_qty']
                                        row['Destination Country'] = pick_country_map.get(orig_pallet, '')
                                        row['Order NO']            = par_entry['load_id']
                                        for rmk in damage_remarks:
                                            row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                        row['ATS'] = ''
                                        fmt_rows.append(row)
                                    balance_qty = max(0.0, mpd_actual - total_partial_qty)
                                    if balance_qty > 0 and not is_damaged:
                                        bal_row = build_row(inv_row, override_pallet=orig_pallet, override_actual_qty=balance_qty)
                                        bal_row['Pick Quantity']       = ''
                                        bal_row['Destination Country'] = ''
                                        bal_row['Order NO']            = ''
                                        for rmk in damage_remarks:
                                            bal_row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                        bal_row['ATS'] = int(balance_qty)
                                        fmt_rows.append(bal_row)
                            else:
                                row = build_row(inv_row)
                                row['Pick Quantity']       = pick_qty_map.get(orig_pallet, '')
                                row['Destination Country'] = pick_country_map.get(orig_pallet, '')
                                row['Order NO']            = pick_loadid_map.get(orig_pallet, '')
                                for rmk in damage_remarks:
                                    row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                ats_qty   = inv_actual_qty - total_picked
                                row['ATS'] = int(ats_qty) if (not is_damaged and ats_qty > 0) else ''
                                fmt_rows.append(row)

                        final_cols = REPORT_HEADERS.copy()
                        final_cols += ['Pick Quantity', 'Destination Country', 'Order NO']
                        final_cols += damage_remarks
                        final_cols += ['ATS']
                        fmt_df = pd.DataFrame(fmt_rows, columns=final_cols)

                        inv_total_qty = pd.to_numeric(inv_data[_inv_aq_col], errors='coerce').fillna(0).sum()
                        rpt_total_qty = pd.to_numeric(fmt_df['Actual Qty'], errors='coerce').fillna(0).sum()
                        qty_match     = abs(inv_total_qty - rpt_total_qty) < 0.01

                        inv_pallet_qty = (
                            inv_data.groupby(_inv_pal_col)[_inv_aq_col]
                            .apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum()).to_dict()
                        )

                        import re as _re
                        _gen_pat = _re.compile(r'^(.+)-P(\d+)$')
                        def _base_pallet(p):
                            m = _gen_pat.match(str(p).strip())
                            return m.group(1) if m else str(p).strip()

                        _rpt_pal = fmt_df['Pallet'].astype(str).str.strip().apply(_base_pallet)
                        _rpt_qty = pd.to_numeric(fmt_df['Actual Qty'], errors='coerce').fillna(0)
                        rpt_pallet_qty = _rpt_qty.groupby(_rpt_pal).sum().to_dict()

                        mismatch_pallets = []
                        try:
                            _inv_pals = inv_data[_inv_pal_col].astype(str).str.strip().apply(_base_pallet)
                            _inv_qtys = pd.to_numeric(inv_data[_inv_aq_col], errors='coerce').fillna(0)
                            orig_total_inv = _inv_qtys.groupby(_inv_pals).sum().to_dict()
                            all_pallets = set(orig_total_inv) | set(rpt_pallet_qty)
                            for pal in all_pallets:
                                inv_q = orig_total_inv.get(pal, 0.0)
                                rpt_q = rpt_pallet_qty.get(pal, 0.0)
                                if abs(inv_q - rpt_q) > 0.01:
                                    mismatch_pallets.append({'Pallet': pal, 'Inventory Actual Qty': inv_q, 'Report Actual Qty': rpt_q, 'Difference': inv_q - rpt_q})
                        except Exception as _mm_err:
                            st.warning(f"⚠️ Mismatch check error: {_mm_err}")

                        total_pick_qty = pd.to_numeric(fmt_df['Pick Quantity'], errors='coerce').fillna(0).sum()
                        total_ats_qty  = pd.to_numeric(fmt_df['ATS'], errors='coerce').fillna(0).sum()
                        total_dmg_qty  = sum(pd.to_numeric(fmt_df[r], errors='coerce').fillna(0).sum() for r in damage_remarks)
                        total_lines    = len(fmt_df)
                        partial_lines  = sum(1 for r in fmt_rows if r.get('Order NO', '') != '')

                        if qty_match:
                            st.success(f"✅ Actual Qty Match! Inventory: **{int(inv_total_qty)}** = Report: **{int(rpt_total_qty)}**")
                        else:
                            st.error(f"⚠️ Actual Qty Mismatch! Inventory: **{int(inv_total_qty)}** ≠ Report: **{int(rpt_total_qty)}** (diff: {int(inv_total_qty - rpt_total_qty)})")

                        st.markdown("#### 📊 Report Summary")
                        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
                        sc1.metric("Total Lines",   total_lines)
                        sc2.metric("Pick Qty",      int(total_pick_qty))
                        sc3.metric("ATS Qty",       int(total_ats_qty))
                        sc4.metric("Damage Qty",    int(total_dmg_qty))
                        sc5.metric("Partial Lines", partial_lines)

                        accounted = total_pick_qty + total_ats_qty + total_dmg_qty
                        if abs(inv_total_qty - accounted) < 0.01:
                            st.success(f"✅ Qty Reconciled: Pick({int(total_pick_qty)}) + ATS({int(total_ats_qty)}) + Damage({int(total_dmg_qty)}) = {int(accounted)}")
                        else:
                            st.warning(f"⚠️ Unaccounted Qty: {int(inv_total_qty - accounted)} | Pick+ATS+Damage={int(accounted)} vs Inventory={int(inv_total_qty)}")

                        if mismatch_pallets:
                            st.markdown("#### 🔍 Pallet Qty Mismatch Details")
                            st.dataframe(pd.DataFrame(mismatch_pallets), use_container_width=True)

                        st.dataframe(fmt_df.astype(str), use_container_width=True)

                        out_fmt = io.BytesIO()
                        with pd.ExcelWriter(out_fmt, engine='xlsxwriter') as writer:
                            fmt_df.to_excel(writer, sheet_name='Pick_Report', index=False)
                            wb     = writer.book
                            ws_fmt = writer.sheets['Pick_Report']
                            ws_summ_sheet = wb.add_worksheet('Summary')
                            bold    = wb.add_format({'bold': True, 'font_size': 11})
                            val_fmt = wb.add_format({'font_size': 11, 'num_format': '#,##0'})
                            ok_fmt  = wb.add_format({'bold': True, 'font_color': '#27ae60', 'font_size': 11})
                            err_fmt = wb.add_format({'bold': True, 'font_color': '#e74c3c', 'font_size': 11})
                            summary_rows = [
                                ('Inventory Total Actual Qty', int(inv_total_qty)),
                                ('Report Total Actual Qty',    int(rpt_total_qty)),
                                ('Qty Match', 'YES ✅' if qty_match else 'NO ⚠️'),
                                ('', ''), ('Pick Quantity', int(total_pick_qty)),
                                ('ATS Quantity', int(total_ats_qty)), ('Damage Quantity', int(total_dmg_qty)),
                                ('Total Accounted', int(accounted)), ('Unaccounted', int(inv_total_qty - accounted)),
                                ('', ''), ('Total Report Lines', total_lines), ('Partial Lines', partial_lines),
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
                            if mismatch_pallets:
                                mm_sheet = wb.add_worksheet('Qty_Mismatch')
                                mm_df2   = pd.DataFrame(mismatch_pallets)
                                mm_hdr   = wb.add_format({'bold': True, 'bg_color': '#e74c3c', 'font_color': '#fff'})
                                for ci, col in enumerate(mm_df2.columns):
                                    mm_sheet.write(0, ci, col, mm_hdr)
                                    mm_sheet.set_column(ci, ci, 22)
                                for ri2, row2 in mm_df2.iterrows():
                                    for ci2, val2 in enumerate(row2):
                                        mm_sheet.write(ri2+1, ci2, val2)

                            hdr_fmt      = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#ffffff', 'border': 1, 'font_size': 10})
                            pick_col_fmt = wb.add_format({'bg_color': '#E8F5E9', 'border': 1, 'font_size': 10})
                            dmg_col_fmt  = wb.add_format({'bg_color': '#FFE0E0', 'border': 1, 'font_size': 10})
                            ats_fmt      = wb.add_format({'bg_color': '#E3F2FD', 'border': 1, 'font_size': 10, 'bold': True})
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
                                    for ri in range(1, len(fmt_df)+1):
                                        ws_fmt.write(ri, ci, str(fmt_df.iloc[ri-1][col_name]), ats_fmt)
                                else:
                                    for ri in range(1, len(fmt_df)+1):
                                        ws_fmt.write(ri, ci, str(fmt_df.iloc[ri-1][col_name]), normal_fmt)
                            ws_fmt.freeze_panes(1, 0)

                        st.download_button(
                            "⬇️ Download Formatted Pick Report",
                            data=out_fmt.getvalue(),
                            file_name=f"Pick_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel", use_container_width=True
                        )
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

        with del_tab1:
            st.info("Load ID, Pallet සහ Actual Qty අඩංගු Excel හෝ CSV ගොනුවක් Upload කිරීමෙන් ගැලපෙන දත්ත Master_Pick_Data එකෙන් පමණක් මකා දැමිය හැක.")
            st.markdown("""
            **📋 Upload File Required Headers:**
            | Column | Description |
            |---|---|
            | `Load Id` | Generated Load ID |
            | `Pallet` | Pallet ID |
            | `Actual Qty` | Pick Quantity |
            """)
            del_file = st.file_uploader("Upload Data to Delete", type=['csv', 'xlsx'], key="del_file_uploader")
            if del_file:
                if st.button("🗑️ Delete Matching Records", type="primary"):
                    with st.spinner("Deleting Data..."):
                        del_df = pd.read_csv(del_file) if del_file.name.endswith('.csv') else pd.read_excel(del_file)
                        del_df.columns = del_df.columns.str.strip().str.upper()
                        if not all(col in del_df.columns for col in ['LOAD ID', 'PALLET', 'ACTUAL QTY']):
                            st.error("Uploaded file must contain 'Load ID', 'Pallet', and 'Actual Qty' columns.")
                            st.stop()
                        keys = [
                            (str(r['LOAD ID']).strip(), str(r['PALLET']).strip(), r['ACTUAL QTY'])
                            for _, r in del_df.iterrows()
                        ]
                        _sb_delete_mpd_by_match_keys(keys)
                        st.success(f"✅ {len(keys)} records deleted from Master_Pick_Data! (Load_History නොවෙනස්ව ඇත)")
                        show_confetti()

        with del_tab2:
            st.info("Load ID එකක් ටයිප් කිරීමෙන් හෝ list එකක් upload කිරීමෙන් Master_Pick_Data එකෙන් delete කළ හැක.")
            lid_method = st.radio("Delete Method:", ["⌨️ Type Load ID", "📂 Upload Load ID List (Excel/CSV)"], horizontal=True, key="lid_del_method")
            if lid_method == "⌨️ Type Load ID":
                del_load_id = st.text_input("🆔 Enter Load ID to Delete:")
                load_ids_to_delete = [del_load_id.strip()] if del_load_id.strip() else []
            else:
                st.caption("Excel/CSV file හි **`Load Id`** column එකක් තිබිය යුතුය.")
                lid_file = st.file_uploader("📂 Upload Load ID List", type=['csv', 'xlsx'], key="lid_file_uploader")
                load_ids_to_delete = []
                if lid_file:
                    lid_df = pd.read_csv(lid_file) if lid_file.name.endswith('.csv') else pd.read_excel(lid_file)
                    lid_col = next((c for c in lid_df.columns if str(c).strip().lower() == 'load id'), None)
                    if lid_col:
                        load_ids_to_delete = [str(v).strip() for v in lid_df[lid_col].dropna().unique() if str(v).strip()]
                        st.success(f"✅ {len(load_ids_to_delete)} Load IDs found")
                        st.dataframe(pd.DataFrame(load_ids_to_delete, columns=['Load ID']), use_container_width=True, height=200)
                    else:
                        st.error("❌ File හි 'Load Id' column හමු නොවීය.")

            if load_ids_to_delete:
                preview_df = get_safe_dataframe(None, "Master_Pick_Data")
                if not preview_df.empty and 'Generated Load ID' in preview_df.columns:
                    preview = preview_df[preview_df['Generated Load ID'].astype(str).str.strip().isin(load_ids_to_delete)]
                    if not preview.empty:
                        st.warning(f"⚠️ **{len(load_ids_to_delete)}** Load ID(s) සඳහා **{len(preview)}** records delete වේ.")
                        st.dataframe(preview[['Generated Load ID', 'Pallet', 'Actual Qty']].astype(str), use_container_width=True, height=200)
                    else:
                        st.info("ගැලපෙන records Master_Pick_Data හි හමු නොවීය.")

                if st.button("🗑️ Delete by Load ID", type="primary", key="del_lid_btn"):
                    with st.spinner("Deleting..."):
                        _sb_delete_mpd_by_load_id(load_ids_to_delete)
                        ids_str = ', '.join(load_ids_to_delete[:3]) + ('...' if len(load_ids_to_delete) > 3 else '')
                        st.success(f"✅ Load ID(s) [{ids_str}] — Master_Pick_Data records deleted! (Load_History නොවෙනස්ව ඇත)")
                        show_confetti()

        with del_tab3:
            st.info("Batch ID එකට අදාල සියලු records Master_Pick_Data එකෙන් delete කළ හැක.")
            mpd_for_batch = get_safe_dataframe(None, "Master_Pick_Data")
            batch_col_mpd = next((c for c in (mpd_for_batch.columns if not mpd_for_batch.empty else []) if str(c).strip().lower() == 'batch id'), None)
            if not mpd_for_batch.empty and batch_col_mpd:
                available_batches_mpd = [b for b in mpd_for_batch[batch_col_mpd].dropna().unique().tolist() if str(b).strip()]
                if available_batches_mpd:
                    del_batch_id = st.selectbox("🗂️ Select Batch ID to Delete:", available_batches_mpd, key="del_batch_sel")
                    if del_batch_id:
                        preview_batch = mpd_for_batch[mpd_for_batch[batch_col_mpd].astype(str).str.strip() == str(del_batch_id).strip()]
                        if not preview_batch.empty:
                            st.warning(f"⚠️ Batch ID **{del_batch_id}** හි **{len(preview_batch)}** records delete වේ.")
                            bc1, bc2, bc3 = st.columns(3)
                            bc1.metric("Records", len(preview_batch))
                            load_id_col_b = next((c for c in preview_batch.columns if str(c).strip().lower() == 'generated load id'), None)
                            if load_id_col_b: bc2.metric("Load IDs", preview_batch[load_id_col_b].nunique())
                            aq_col_b = next((c for c in preview_batch.columns if str(c).strip().lower() == 'actual qty'), None)
                            if aq_col_b: bc3.metric("Total Qty", int(pd.to_numeric(preview_batch[aq_col_b], errors='coerce').sum()))
                        if st.button("🗑️ Delete by Batch ID", type="primary", key="del_batch_btn"):
                            with st.spinner("Deleting..."):
                                _sb_delete_mpd_by_batch(del_batch_id)
                                st.success(f"✅ Batch ID **{del_batch_id}** — records deleted! (Load_History නොවෙනස්ව ඇත)")
                                show_confetti()
                else:
                    st.info("Master_Pick_Data හි Batch IDs නොමැත.")
            else:
                st.info("Master_Pick_Data හි data නොමැත.")

        with del_tab4:
            st.info("Pallet ID එකෙන් Master_Pick_Data, Master_Partial_Data, Damage_Items සියල්ලෙන් delete කළ හැක.")
            _del_batch = APIManager.batch_read(None, ["Master_Pick_Data", "Master_Partial_Data", "Damage_Items"])
            _mpd_pal   = _del_batch["Master_Pick_Data"]
            _mpart_pal = _del_batch["Master_Partial_Data"]
            _dmg_pal   = _del_batch["Damage_Items"]

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
                        if df.empty or col not in df.columns: return 0
                        return len(df[df[col].astype(str).str.strip() == str(val).strip()])
                    mpd_count   = _count_rows(_mpd_pal, 'Pallet', del_pallet)
                    mpart_count = _count_rows(_mpart_pal, 'Pallet', del_pallet)
                    mpart_gen   = _count_rows(_mpart_pal, 'Gen Pallet ID', del_pallet) if 'Gen Pallet ID' in _mpart_pal.columns else 0
                    dmg_count   = _count_rows(_dmg_pal, 'Pallet', del_pallet)
                    st.warning(f"⚠️ Pallet **{del_pallet}** හා සම්බන්ධ records:")
                    pc1, pc2, pc3 = st.columns(3)
                    pc1.metric("Master_Pick_Data",    mpd_count)
                    pc2.metric("Master_Partial_Data", mpart_count + mpart_gen)
                    pc3.metric("Damage_Items",        dmg_count)
                    if st.button("🗑️ Delete Pallet from All Sheets", type="primary", key="del_pallet_btn"):
                        with st.spinner("Deleting..."):
                            _sb_delete_by_pallet(del_pallet)
                            st.success(f"✅ Pallet **{del_pallet}** deleted from all tables!")
                            show_confetti()
            else:
                st.info("Delete කළ හැකි Pallets නොමැත.")

    # ==========================================
    # TAB 5: DAMAGE ITEMS
    # ==========================================
    elif choice == "🩹 Damage Items":
        st.title("🩹 Damage Items Management")
        st.info("Damage, defective හෝ unavailable items Pallet/Actual Qty/Remark සහිතව upload කරන්න. Pallets pick operations වලින් automatically exclude වේ.")

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
                        pallet_col = next((c for c in dmg_preview.columns if 'pallet' in c.lower()), None)
                        qty_col    = next((c for c in dmg_preview.columns if 'actual qty' in c.lower() or 'qty' in c.lower()), None)
                        remark_col = next((c for c in dmg_preview.columns if 'remark' in c.lower()), None)
                        if not pallet_col or not qty_col:
                            st.error("File එකේ අවම වශයෙන් 'Pallet' සහ 'Actual Qty' columns තිබිය යුතුය.")
                        else:
                            rows_to_add = []
                            for _, row in dmg_preview.iterrows():
                                rows_to_add.append([
                                    str(row.get(pallet_col, '')),
                                    str(row.get(qty_col, '')),
                                    str(row.get(remark_col, '')) if remark_col else 'Damage',
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    current_user
                                ])
                            APIManager.append_rows_to_sheet(
                                None, "Damage_Items", SHEET_HEADERS["Damage_Items"], rows_to_add
                            )
                            st.success(f"✅ Damage Items {len(rows_to_add)} ක් සාර්ථකව save කරන ලදී!")
                            show_confetti()

        with dmg_tab2:
            st.subheader("Damage Items Records")
            dmg_df = get_safe_dataframe(None, "Damage_Items")
            if dmg_df.empty:
                st.info("Damage Items records නොමැත.")
            else:
                st.metric("Total Damage Records", len(dmg_df))
                st.dataframe(dmg_df.astype(str), use_container_width=True)
                out_dmg = io.BytesIO()
                with pd.ExcelWriter(out_dmg, engine='xlsxwriter') as writer:
                    dmg_df.to_excel(writer, sheet_name='Damage_Items', index=False)
                st.download_button(
                    "⬇️ Download Damage Records", data=out_dmg.getvalue(),
                    file_name=f"Damage_Items_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.ms-excel"
                )
                st.divider()
                st.subheader("🗑️ Remove Damage Record")
                if 'Pallet' in dmg_df.columns:
                    remove_pallet = st.selectbox("Select Pallet to Remove from Damage List:", dmg_df['Pallet'].dropna().unique())
                    if st.button("Remove Damage Record"):
                        sb = get_supabase_client()
                        sb.table("damage_items").delete().eq("pallet", str(remove_pallet).strip()).execute()
                        APIManager.invalidate("Damage_Items")
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
                    sb = get_supabase_client()
                    users_data = get_safe_dataframe(None, "Users")
                    if not users_data.empty and n_user in users_data['Username'].values:
                        st.error("මෙම Username එක දැනටමත් ඇත.")
                    else:
                        sb.table("users").insert({"username": n_user, "password": n_pass, "role": n_role}).execute()
                        APIManager.invalidate("Users")
                        st.success("User සාර්ථකව ඇතුලත් කරන ලදී!")

        with col_adm2:
            st.subheader("⚠️ Database Management")
            st.warning("මෙමඟින් පද්ධතියේ පරණ දත්ත සම්පූර්ණයෙන්ම මකා දමයි.")
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
                            _sb_clear_table(s_name)
                        st.success(f"✅ {sheet_to_clear} සාර්ථකව Reset කරන ලදී.")
                    except Exception as e:
                        st.error(f"Error clearing data: {e}")
                else:
                    st.error("කරුණාකර CONFIRM ලෙස Type කරන්න.")

footer_branding()
