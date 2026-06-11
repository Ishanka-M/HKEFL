import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io
import time

# --- 1. System Config ---
st.set_page_config(page_title="Advanced WMS Picking System", layout="wide", page_icon="📦")

# --- Master_Pick_Data Headers (exact match to inventory file columns + WMS fields) ---
INVENTORY_HEADERS = [
    'Wh Id', 'Client Code', 'Pallet', 'Invoice Number', 'Location Id', 'Item Number',
    'Description', 'Lot Number', 'Actual Qty', 'Unavailable Qty', 'Uom', 'Status',
    'Mlp', 'Stored Attribute Id', 'Fifo Date', 'Expiration Date', 'Grn Number',
    'Gate Pass Id', 'Cust Dec No', 'Color', 'Size', 'Style', 'Supplier', 'Plant',
    'Client So', 'Client So Line', 'Po Cust Dec', 'Customer Ref Number', 'Item Id',
    'Invoice Number 1', 'Transaction', 'Order Type', 'Order Number', 'Store Order Number',
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
    # ── UPDATED: Invoice Number + Grn Number added to Master_Partial_Data ──
    "Master_Partial_Data": ['Batch ID', 'SO Number', 'Pallet', 'Supplier', 'Load ID', 'Country Name',
                             'Actual Qty', 'Partial Qty', 'Gen Pallet ID', 'Balance Qty',
                             'Location Id', 'Lot Number', 'Color', 'Size', 'Style', 'Customer Po Number',
                             'Vendor Name', 'Invoice Number', 'Grn Number'],
    "Master_Pick_Data": MASTER_PICK_HEADERS,
    "Damage_Items": ['Pallet', 'Actual Qty', 'Remark', 'Date Added', 'Added By'],
    "Vendor_Maintain": ['Vendor Name', 'Country'],
}

# --- DB column name mapping: app header → supabase column ---
PICK_COL_MAP = {
    'Wh Id': 'wh_id', 'Client Code': 'client_code', 'Pallet': 'pallet',
    'Invoice Number': 'invoice_number', 'Location Id': 'location_id', 'Item Number': 'item_number',
    'Description': 'description', 'Lot Number': 'lot_number', 'Actual Qty': 'actual_qty',
    'Unavailable Qty': 'unavailable_qty', 'Uom': 'uom', 'Status': 'status', 'Mlp': 'mlp',
    'Stored Attribute Id': 'stored_attribute_id', 'Fifo Date': 'fifo_date',
    'Expiration Date': 'expiration_date', 'Grn Number': 'grn_number', 'Gate Pass Id': 'gate_pass_id',
    'Cust Dec No': 'cust_dec_no', 'Color': 'color', 'Size': 'size', 'Style': 'style',
    'Supplier': 'supplier', 'Plant': 'plant', 'Client So': 'client_so', 'Client So Line': 'client_so_line',
    'Po Cust Dec': 'po_cust_dec', 'Customer Ref Number': 'customer_ref_number', 'Item Id': 'item_id',
    'Invoice Number 1': 'invoice_number1', 'Transaction': 'transaction', 'Order Type': 'order_type',
    'Order Number': 'order_number', 'Store Order Number': 'store_order_number',
    'Customer Po Number': 'customer_po_number', 'Partial Order Flag': 'partial_order_flag',
    'Order Date': 'order_date', 'Load Id': 'load_id', 'Asn Number': 'asn_number',
    'Po Number': 'po_number', 'Supplier Hu': 'supplier_hu', 'New Item Number': 'new_item_number',
    'Asn Line Number': 'asn_line_number', 'Received Gross Weight': 'received_gross_weight',
    'Current Gross Weight': 'current_gross_weight', 'Received Net Weight': 'received_net_weight',
    'Current Net Weight': 'current_net_weight', 'Supplier Desc': 'supplier_desc', 'Cbm': 'cbm',
    'Container Type': 'container_type', 'Display Item Number': 'display_item_number',
    'Old Item Number': 'old_item_number', 'Inventory Type': 'inventory_type', 'Type Qc': 'type_qc',
    'Vendor Name': 'vendor_name', 'Manufacture Date': 'manufacture_date', 'Suom': 'suom',
    'S Qty': 's_qty', 'Pick Id': 'pick_id', 'Downloaded Date': 'downloaded_date',
    'Batch ID': 'batch_id', 'SO Number': 'so_number', 'Generated Load ID': 'generated_load_id',
    'Country Name': 'country_name', 'Pick Quantity': 'pick_quantity', 'Remark': 'remark',
}
PICK_COL_MAP_REV = {v: k for k, v in PICK_COL_MAP.items()}

# ── UPDATED: Invoice Number + Grn Number added to PARTIAL_COL_MAP ──
PARTIAL_COL_MAP = {
    'Batch ID': 'batch_id', 'SO Number': 'so_number', 'Pallet': 'pallet', 'Supplier': 'supplier',
    'Load ID': 'load_id', 'Country Name': 'country_name', 'Actual Qty': 'actual_qty',
    'Partial Qty': 'partial_qty', 'Gen Pallet ID': 'gen_pallet_id', 'Balance Qty': 'balance_qty',
    'Location Id': 'location_id', 'Lot Number': 'lot_number', 'Color': 'color', 'Size': 'size',
    'Style': 'style', 'Customer Po Number': 'customer_po_number',
    'Vendor Name': 'vendor_name',
    'Invoice Number': 'invoice_number',
    'Grn Number': 'grn_number',
}
PARTIAL_COL_MAP_REV = {v: k for k, v in PARTIAL_COL_MAP.items()}

HISTORY_COL_MAP = {
    'Batch ID': 'batch_id', 'Generated Load ID': 'generated_load_id', 'SO Number': 'so_number',
    'Country Name': 'country_name', 'SHIP MODE': 'ship_mode', 'Date': 'date', 'Pick Status': 'pick_status',
}
HISTORY_COL_MAP_REV = {v: k for k, v in HISTORY_COL_MAP.items()}

SUMMARY_COL_MAP = {
    'Batch ID': 'batch_id', 'SO Number': 'so_number', 'Load ID': 'load_id', 'UPC': 'upc',
    'Country': 'country', 'Ship Mode': 'ship_mode', 'Requested': 'requested', 'Picked': 'picked',
    'Variance': 'variance', 'Status': 'status',
}
SUMMARY_COL_MAP_REV = {v: k for k, v in SUMMARY_COL_MAP.items()}

DAMAGE_COL_MAP = {
    'Pallet': 'pallet', 'Actual Qty': 'actual_qty', 'Remark': 'remark',
    'Date Added': 'date_added', 'Added By': 'added_by',
}
DAMAGE_COL_MAP_REV = {v: k for k, v in DAMAGE_COL_MAP.items()}

USERS_COL_MAP = {
    'Username': 'username', 'Password': 'password', 'Role': 'role',
}
USERS_COL_MAP_REV = {v: k for k, v in USERS_COL_MAP.items()}

VENDOR_COL_MAP = {
    'Vendor Name': 'vendor_name',
    'Country':     'country',
}
VENDOR_COL_MAP_REV = {v: k for k, v in VENDOR_COL_MAP.items()}

# ── NEW: inventory_status table ────────────────────────────────────────────────
# Uploaded Inventory එක මෙම වෙනම DB එකට save වේ. 'Status' (proc_status) column එක
# logic වලින් 'D' ලෙස update වේ. Inventory upload කරද්දී මෙම table එක clear වේ.
INVSTATUS_COL_MAP = {
    'Wh Id': 'wh_id', 'Client Code': 'client_code', 'Pallet': 'pallet',
    'Invoice Number': 'invoice_number', 'Location Id': 'location_id',
    'Item Number': 'item_number', 'Description': 'description', 'Lot Number': 'lot_number',
    'Actual Qty': 'actual_qty', 'Uom': 'uom', 'Fifo Date': 'fifo_date',
    'Grn Number': 'grn_number', 'Color': 'color', 'Size': 'size', 'Style': 'style',
    'Supplier': 'supplier', 'Client So': 'client_so', 'Client So 2': 'client_so_2',
    'Supplier Hu': 'supplier_hu', 'Inventory Type': 'inventory_type',
    'Vendor Name': 'vendor_name', 'Pick Id': 'pick_id',
    # ── report output / processing fields ──
    'Pick Quantity': 'pick_quantity', 'Allocated': 'allocated',
    'Destination Country': 'destination_country', 'Order NO': 'order_no',
    'ATS': 'ats', 'QC Repair': 'qc_repair', 'COO': 'coo', 'Balance Qty': 'balance_qty',
    'Status': 'proc_status', 'Row Order': 'row_order',
}
INVSTATUS_COL_MAP_REV = {v: k for k, v in INVSTATUS_COL_MAP.items()}

# CREATE TABLE SQL (Supabase) — මෙය එක් වරක් run කරන්න:
INVENTORY_STATUS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS inventory_status (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wh_id               text, client_code text, pallet text, invoice_number text,
    location_id         text, item_number text, description text, lot_number text,
    actual_qty          numeric, uom text, fifo_date text, grn_number text,
    color               text, size text, style text, supplier text, client_so text,
    client_so_2         text, supplier_hu text, inventory_type text, vendor_name text,
    pick_id             text,
    pick_quantity       numeric, allocated numeric, destination_country text,
    order_no            text, ats numeric, qc_repair numeric, coo text,
    balance_qty         numeric, proc_status text, row_order bigint,
    created_at          timestamptz DEFAULT now()
);
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def show_confetti():
    st.markdown("""
    <style>
    @keyframes confetti-fall {
        0%   { transform: translateY(-20px) rotate(0deg); opacity:1; }
        100% { transform: translateY(100vh) rotate(720deg); opacity:0; }
    }
    .confetti-piece {
        position: fixed; width: 10px; height: 10px; top: -20px;
        animation: confetti-fall linear forwards; z-index: 9999; border-radius: 2px;
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
    .footer { position:fixed; left:0; bottom:0; width:100%; background-color:transparent;
              color:#888888; text-align:center; font-size:13px; padding:10px;
              font-weight:bold; z-index:100; }
    </style>
    <div class="footer">Developed by Ishanka Madusanka</div>
    """, unsafe_allow_html=True)


# ── Supabase Client ───────────────────────────────────────────────────────────

@st.cache_resource
def get_supabase_client() -> Client:
    url  = st.secrets["supabase"]["url"]
    key  = st.secrets["supabase"]["service_role_key"]   # service role — bypasses RLS
    return create_client(url, key)


# ── DB Manager ────────────────────────────────────────────────────────────────

class DBManager:
    _cache: dict = {}
    _cache_ttl: int = 30

    _COL_MAP = {
        "master_pick_data":    PICK_COL_MAP,
        "master_partial_data": PARTIAL_COL_MAP,
        "load_history":        HISTORY_COL_MAP,
        "summary_data":        SUMMARY_COL_MAP,
        "damage_items":        DAMAGE_COL_MAP,
        "users":               USERS_COL_MAP,
        "vendor_maintain":     VENDOR_COL_MAP,
        "inventory_status":    INVSTATUS_COL_MAP,
    }
    _COL_MAP_REV = {
        "master_pick_data":    PICK_COL_MAP_REV,
        "master_partial_data": PARTIAL_COL_MAP_REV,
        "load_history":        HISTORY_COL_MAP_REV,
        "summary_data":        SUMMARY_COL_MAP_REV,
        "damage_items":        DAMAGE_COL_MAP_REV,
        "users":               USERS_COL_MAP_REV,
        "vendor_maintain":     VENDOR_COL_MAP_REV,
        "inventory_status":    INVSTATUS_COL_MAP_REV,
    }

    @classmethod
    def _table_key(cls, sheet_name: str) -> str:
        return sheet_name.lower().replace(" ", "_")

    @classmethod
    def invalidate(cls, sheet_name=None):
        if sheet_name:
            cls._cache.pop(cls._table_key(sheet_name), None)
        else:
            cls._cache.clear()

    @classmethod
    def _db_to_app_df(cls, table_key: str, records: list) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df.drop(columns=[c for c in ['id', 'created_at'] if c in df.columns], inplace=True, errors='ignore')
        col_rev = cls._COL_MAP_REV.get(table_key, {})
        df.rename(columns=col_rev, inplace=True)
        return df

    @classmethod
    def _app_row_to_db(cls, table_key: str, row: dict) -> dict:
        col_map = cls._COL_MAP.get(table_key, {})
        out = {}
        for k, v in row.items():
            db_col = col_map.get(k, k.lower().replace(" ", "_"))
            if v in (None, 'nan', 'None', ''):
                out[db_col] = None
            else:
                out[db_col] = v
        return out

    @classmethod
    def read_table(cls, table_name: str, force: bool = False) -> pd.DataFrame:
        key = cls._table_key(table_name)
        now = time.time()
        if not force and key in cls._cache:
            ts, df = cls._cache[key]
            if (now - ts) < cls._cache_ttl:
                return df.copy()
        try:
            sb = get_supabase_client()
            all_records = []
            offset = 0
            page_size = 1000
            while True:
                res = sb.table(key).select("*").range(offset, offset + page_size - 1).execute()
                batch = res.data or []
                all_records.extend(batch)
                if len(batch) < page_size:
                    break
                offset += page_size
            df = cls._db_to_app_df(key, all_records)
            cls._cache[key] = (time.time(), df)
            return df.copy()
        except Exception as e:
            st.warning(f"DB read error ({table_name}): {e}")
            if key in cls._cache:
                return cls._cache[key][1].copy()
            return pd.DataFrame()

    @classmethod
    def batch_read(cls, table_names: list, force: bool = False) -> dict:
        return {name: cls.read_table(name, force=force) for name in table_names}

    @classmethod
    def insert_rows(cls, table_name: str, rows: list) -> bool:
        if not rows:
            return True
        key = cls._table_key(table_name)
        try:
            sb = get_supabase_client()
            db_rows = [cls._app_row_to_db(key, r) for r in rows]
            chunk_size = 500
            for i in range(0, len(db_rows), chunk_size):
                sb.table(key).insert(db_rows[i:i+chunk_size]).execute()
            cls.invalidate(table_name)
            return True
        except Exception as e:
            st.error(f"DB insert error ({table_name}): {e}")
            return False

    @classmethod
    def delete_where(cls, table_name: str, column: str, values: list) -> int:
        if not values:
            return 0
        key = cls._table_key(table_name)
        col_map = cls._COL_MAP.get(key, {})
        db_col = col_map.get(column, column.lower().replace(" ", "_"))
        try:
            sb = get_supabase_client()
            res = sb.table(key).delete().in_(db_col, [str(v) for v in values]).execute()
            cls.invalidate(table_name)
            deleted = len(res.data) if res.data else 0
            return deleted
        except Exception as e:
            st.error(f"DB delete error ({table_name}): {e}")
            return 0

    @classmethod
    def delete_where_eq(cls, table_name: str, column: str, value) -> int:
        key = cls._table_key(table_name)
        col_map = cls._COL_MAP.get(key, {})
        db_col = col_map.get(column, column.lower().replace(" ", "_"))
        try:
            sb = get_supabase_client()
            res = sb.table(key).delete().eq(db_col, str(value)).execute()
            cls.invalidate(table_name)
            return len(res.data) if res.data else 0
        except Exception as e:
            st.error(f"DB delete error ({table_name}): {e}")
            return 0

    @classmethod
    def delete_match_keys(cls, table_name: str, keys: list, key_cols: list) -> int:
        if not keys:
            return 0
        key = cls._table_key(table_name)
        try:
            df = cls.read_table(table_name, force=True)
            if df.empty:
                return 0
            initial = len(df)
            def _make_key(row):
                parts = []
                for c in key_cols:
                    val = str(row.get(c, '')).strip()
                    if c == 'Actual Qty':
                        try:
                            f = float(val)
                            val = str(f)
                        except:
                            pass
                    parts.append(val)
                return "_".join(parts)
            df_keys = df.apply(_make_key, axis=1)
            filtered = df[~df_keys.isin(set(keys))].reset_index(drop=True)
            deleted = initial - len(filtered)
            if deleted > 0:
                cls._overwrite_table(table_name, filtered)
            return deleted
        except Exception as e:
            st.error(f"DB composite delete error: {e}")
            return 0

    @classmethod
    def _overwrite_table(cls, table_name: str, df: pd.DataFrame):
        key = cls._table_key(table_name)
        try:
            sb = get_supabase_client()
            sb.table(key).delete().neq('id', -1).execute()
            cls.invalidate(table_name)
            if not df.empty:
                col_map = cls._COL_MAP.get(key, {})
                rows = []
                for _, row in df.iterrows():
                    db_row = {}
                    for c in df.columns:
                        db_col = col_map.get(c, c.lower().replace(" ", "_"))
                        val = row[c]
                        if pd.isna(val) or str(val) in ('nan', 'None', ''):
                            db_row[db_col] = None
                        else:
                            db_row[db_col] = val
                    rows.append(db_row)
                chunk_size = 500
                for i in range(0, len(rows), chunk_size):
                    sb.table(key).insert(rows[i:i+chunk_size]).execute()
            cls.invalidate(table_name)
        except Exception as e:
            st.error(f"DB overwrite error ({table_name}): {e}")

    @classmethod
    def update_cell(cls, table_name: str, match_col: str, match_val, update_col: str, new_val) -> bool:
        key = cls._table_key(table_name)
        col_map = cls._COL_MAP.get(key, {})
        db_match = col_map.get(match_col, match_col.lower().replace(" ", "_"))
        db_update = col_map.get(update_col, update_col.lower().replace(" ", "_"))
        try:
            sb = get_supabase_client()
            sb.table(key).update({db_update: new_val}).eq(db_match, str(match_val)).execute()
            cls.invalidate(table_name)
            return True
        except Exception as e:
            st.error(f"DB update error: {e}")
            return False

    @classmethod
    def clear_table(cls, table_name: str) -> bool:
        """Delete ALL rows from a table (used to reset inventory_status on each upload)."""
        key = cls._table_key(table_name)
        try:
            sb = get_supabase_client()
            sb.table(key).delete().neq('id', -1).execute()
            cls.invalidate(table_name)
            return True
        except Exception as e:
            st.warning(f"DB clear error ({table_name}): {e}")
            return False

    @classmethod
    def replace_table(cls, table_name: str, df: pd.DataFrame) -> bool:
        """Clear a table then insert the given DataFrame (clear + bulk insert)."""
        try:
            cls._overwrite_table(table_name, df)
            return True
        except Exception as e:
            st.error(f"DB replace error ({table_name}): {e}")
            return False


# ── Drop-in compatibility helpers ──────────────────────────────────────────────

def get_safe_dataframe(sh, sheet_name, retries=3):
    return DBManager.read_table(sheet_name)


# ── Vendor lookup helper ──────────────────────────────────────────────────────

def get_vendor_country_map() -> dict:
    """Returns {vendor_name_lower: country} from vendor_maintain table."""
    try:
        vdf = DBManager.read_table("vendor_maintain")
        if not vdf.empty and 'Vendor Name' in vdf.columns and 'Country' in vdf.columns:
            return {
                str(v).strip().lower(): str(c).strip()
                for v, c in zip(vdf['Vendor Name'], vdf['Country'])
                if str(v).strip()
            }
    except:
        pass
    return {}


# ── UPDATED: Partial data lookup helper (gen_pallet_id → invoice/grn/vendor) ──

def get_partial_lookup_maps():
    """
    Returns three dicts keyed by gen_pallet_id (lowercase stripped):
      - invoice_map:  {gen_pallet_id: invoice_number}
      - grn_map:      {gen_pallet_id: grn_number}
      - vendor_map:   {pallet: vendor_name}  (also keyed by gen_pallet_id)
    And two dicts keyed by original pallet:
      - pallet_invoice_map: {pallet: invoice_number}
      - pallet_grn_map:     {pallet: grn_number}
    """
    invoice_map = {}
    grn_map = {}
    vendor_map = {}
    pallet_invoice_map = {}
    pallet_grn_map = {}
    try:
        part_df = DBManager.read_table("master_partial_data")
        if not part_df.empty:
            for _, r in part_df.iterrows():
                pallet_key   = str(r.get('Pallet', '')).strip()
                gen_pallet   = str(r.get('Gen Pallet ID', '')).strip()
                inv_num      = str(r.get('Invoice Number', '')).strip()
                grn_num      = str(r.get('Grn Number', '')).strip()
                vnd_name     = str(r.get('Vendor Name', '')).strip()

                # keyed by gen_pallet_id
                if gen_pallet and gen_pallet not in ('', 'nan', 'None'):
                    if inv_num and inv_num not in ('', 'nan', 'None'):
                        invoice_map[gen_pallet] = inv_num
                    if grn_num and grn_num not in ('', 'nan', 'None'):
                        grn_map[gen_pallet] = grn_num
                    if vnd_name and vnd_name not in ('', 'nan', 'None'):
                        vendor_map[gen_pallet] = vnd_name

                # keyed by original pallet
                if pallet_key and pallet_key not in ('', 'nan', 'None'):
                    if inv_num and inv_num not in ('', 'nan', 'None'):
                        pallet_invoice_map[pallet_key] = inv_num
                    if grn_num and grn_num not in ('', 'nan', 'None'):
                        pallet_grn_map[pallet_key] = grn_num
                    if vnd_name and vnd_name not in ('', 'nan', 'None'):
                        vendor_map[pallet_key] = vnd_name
    except:
        pass
    return invoice_map, grn_map, vendor_map, pallet_invoice_map, pallet_grn_map


# ── 2. User Management & Login ─────────────────────────────────────────────────

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
                users_df = DBManager.read_table("users")
            except Exception as e:
                st.error(f"Database connection error: {e}")
                return False

            user = st.text_input("Username", key="login_user")
            pw   = st.text_input("Password", type="password", key="login_pw")

            if st.button("Login", type="primary", use_container_width=True):
                user_match = users_df[(users_df['Username'] == user) & (users_df['Password'] == str(pw))]
                if not user_match.empty:
                    st.session_state['logged_in'] = True
                    st.session_state['role']      = user_match.iloc[0]['Role']
                    st.session_state['username']  = user
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


# ── 3. Inventory Logic ─────────────────────────────────────────────────────────

def get_damage_pallets():
    try:
        dmg_df = DBManager.read_table("damage_items")
        if not dmg_df.empty and 'Pallet' in dmg_df.columns and 'Actual Qty' in dmg_df.columns:
            dmg_df['Actual Qty'] = pd.to_numeric(dmg_df['Actual Qty'], errors='coerce').fillna(0)
            dmg_summary = dmg_df.groupby('Pallet')['Actual Qty'].sum().reset_index()
            dmg_summary.columns = ['Pallet', 'Damage_Qty']
            return dmg_summary
    except:
        pass
    return pd.DataFrame(columns=['Pallet', 'Damage_Qty'])


def reconcile_inventory(inv_df):
    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_lower = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    pallet_col = inv_col_lower.get('pallet', 'Pallet')
    actual_col = inv_col_lower.get('actual qty', 'Actual Qty')
    if actual_col not in inv_df.columns:
        actual_col = next((c for c in inv_df.columns if 'actual' in c.lower()), actual_col)

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)

    try:
        pick_history = DBManager.read_table("master_pick_data")
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
        dmg_summary = get_damage_pallets()
        if not dmg_summary.empty:
            damage_pallet_set = set(dmg_summary['Pallet'].astype(str).str.strip().tolist())
            inv_df = inv_df[~inv_df[pallet_col].astype(str).str.strip().isin(damage_pallet_set)].reset_index(drop=True)
    except Exception as e:
        st.warning(f"Damage Exclude Error: {e}")

    inv_df[actual_col] = pd.to_numeric(inv_df[actual_col], errors='coerce').fillna(0)
    inv_df = inv_df[inv_df[actual_col] > 0].reset_index(drop=True)
    return inv_df


def process_picking(inv_df, req_df, batch_id, inv_original=None):
    pick_rows, partial_rows, summary = [], [], []

    inv_df = inv_df.copy()
    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_col_map = {str(c).strip().lower(): str(c).strip() for c in inv_df.columns}

    supplier_col      = next((inv_col_map[k] for k in inv_col_map if k == 'supplier'), None)
    pick_id_col       = next((inv_col_map[k] for k in inv_col_map if k in ('pick id', 'pickid')), None)
    pallet_col        = next((inv_col_map[k] for k in inv_col_map if k == 'pallet'), 'Pallet')
    vendor_name_col   = next((inv_col_map[k] for k in inv_col_map if k == 'vendor name'), None)
    invoice_number_col = next((inv_col_map[k] for k in inv_col_map if k == 'invoice number'), None)
    grn_number_col    = next((inv_col_map[k] for k in inv_col_map if k == 'grn number'), None)

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
        existing_partial = DBManager.read_table("master_partial_data")
        if not existing_partial.empty and 'Gen Pallet ID' in existing_partial.columns:
            existing_gen_pallet_ids = set(existing_partial['Gen Pallet ID'].astype(str).tolist())
    except:
        pass
    # ── UPDATED: Also check old_history table for unique gen_pallet_id ──
    try:
        old_hist_df = DBManager.read_table("old_history")
        if not old_hist_df.empty and 'Gen Pallet ID' in old_hist_df.columns:
            existing_gen_pallet_ids.update(old_hist_df['Gen Pallet ID'].astype(str).tolist())
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
        so_num    = str(current_reqs['SO Number'].iloc[0])
        ship_mode = str(current_reqs['SHIP MODE: (SEA/AIR)'].iloc[0]) if 'SHIP MODE: (SEA/AIR)' in current_reqs.columns else ""

        for _, req in current_reqs.iterrows():
            raw_upc = req['Product UPC']
            try:
                f_upc = float(raw_upc)
                upc = str(int(f_upc)) if f_upc == int(f_upc) else str(f_upc)
            except (ValueError, TypeError):
                upc = str(raw_upc).strip()

            needed  = float(req['PICK QTY'])
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

                    # ── Vendor Name from inventory ──
                    vendor_name_val = ''
                    if vendor_name_col and vendor_name_col in item.index:
                        vendor_name_val = str(item[vendor_name_col]).strip()
                    p_row['Vendor Name'] = vendor_name_val

                    # ── Invoice Number from inventory ──
                    invoice_number_val = ''
                    if invoice_number_col and invoice_number_col in item.index:
                        invoice_number_val = str(item[invoice_number_col]).strip()

                    # ── GRN Number from inventory ──
                    grn_number_val = ''
                    if grn_number_col and grn_number_col in item.index:
                        grn_number_val = str(item[grn_number_col]).strip()

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
                    orig_qty   = orig_qty_map.get(pallet_val, current_avail)
                    is_partial = (take < current_avail) or (orig_qty > take)

                    if is_partial:
                        def _get(col_name):
                            c = inv_col_map.get(col_name.lower())
                            return str(item[c]) if c and c in item.index else ''

                        gen_pallet_id = make_unique_gen_pallet_id(pallet_val)
                        pick_rows[-1]['Remark']        = 'Partial'
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
                            'Vendor Name':        vendor_name_val,
                            # ── UPDATED: Invoice Number + Grn Number saved ──
                            'Invoice Number':     invoice_number_val,
                            'Grn Number':         grn_number_val,
                        })
                    else:
                        pick_rows[-1]['Gen Pallet ID'] = ''

                    temp_inv.at[idx, actual_qty_col] -= take
                    needed     -= take
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


def generate_inventory_details_report(inv_df):
    try:
        pick_df = DBManager.read_table("master_pick_data")

        damage_lookup = {}
        try:
            dmg_df = DBManager.read_table("damage_items")
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

        # ── Load vendor_maintain for country lookup ──
        vendor_country_map = get_vendor_country_map()

        # ── UPDATED: Load partial data maps (vendor, invoice, grn) keyed by pallet & gen_pallet_id ──
        inv_invoice_map, inv_grn_map, partial_vendor_map, pallet_invoice_map, pallet_grn_map = get_partial_lookup_maps()

        pick_by_pallet = {}
        if not pick_df.empty and 'Pallet' in pick_df.columns:
            pick_df['_pkey'] = pick_df['Pallet'].astype(str).str.strip()
            for pkey, grp in pick_df.groupby('_pkey'):
                pick_by_pallet[pkey] = grp.to_dict('records')

        report_rows = []
        for _, inv_row in inv_df.iterrows():
            pallet = str(inv_row.get('Pallet', '')).strip()

            # ── Resolve Vendor Name ──
            vendor_name_inv = str(inv_row.get('Vendor Name', '')).strip() if 'Vendor Name' in inv_row.index else ''
            if not vendor_name_inv:
                vendor_name_inv = partial_vendor_map.get(pallet, '')

            # ── Resolve Invoice Number from inventory row, fallback to partial data ──
            invoice_number_inv = str(inv_row.get('Invoice Number', '')).strip() if 'Invoice Number' in inv_row.index else ''
            if not invoice_number_inv or invoice_number_inv in ('nan', 'None'):
                invoice_number_inv = pallet_invoice_map.get(pallet, '')

            # ── Resolve GRN Number from inventory row, fallback to partial data ──
            grn_number_inv = str(inv_row.get('Grn Number', '')).strip() if 'Grn Number' in inv_row.index else ''
            if not grn_number_inv or grn_number_inv in ('nan', 'None'):
                grn_number_inv = pallet_grn_map.get(pallet, '')

            # ── Resolve Country from vendor_maintain ──
            country_from_vendor = vendor_country_map.get(vendor_name_inv.lower(), '') if vendor_name_inv else ''

            if pallet in damage_lookup:
                row = inv_row.copy()
                row['Batch ID'] = row['SO Number'] = row['Generated Load ID'] = ''
                row['Country Name'] = row['Pick Quantity'] = ''
                row['Remark']            = damage_lookup[pallet]
                row['Allocation Status'] = 'Damage'
                row['Vendor Name']       = vendor_name_inv
                row['Invoice Number']    = invoice_number_inv
                row['Grn Number']        = grn_number_inv
                row['Vendor Country']    = country_from_vendor
                report_rows.append(row)
                continue

            pallet_pick_rows = pick_by_pallet.get(pallet)
            if pallet_pick_rows:
                for pick_rec in pallet_pick_rows:
                    row = inv_row.copy()
                    row['Batch ID']           = pick_rec.get('Batch ID', '')
                    row['SO Number']          = pick_rec.get('SO Number', '')
                    row['Generated Load ID']  = pick_rec.get('Generated Load ID', pick_rec.get('Load Id', ''))
                    row['Country Name']       = pick_rec.get('Country Name', '')
                    row['Pick Quantity']      = pick_rec.get('Pick Quantity', pick_rec.get('Actual Qty', ''))
                    row['Remark']             = pick_rec.get('Remark', 'Allocated')
                    row['Allocation Status']  = 'Picked'
                    row['Vendor Name']        = pick_rec.get('Vendor Name', vendor_name_inv)

                    # ── UPDATED: Fill Invoice Number blank from master_pick_data or partial ──
                    pick_inv = str(pick_rec.get('Invoice Number', '')).strip()
                    if not pick_inv or pick_inv in ('nan', 'None'):
                        pick_inv = invoice_number_inv
                    row['Invoice Number'] = pick_inv

                    # ── UPDATED: Fill GRN Number blank from master_pick_data or partial ──
                    pick_grn = str(pick_rec.get('Grn Number', '')).strip()
                    if not pick_grn or pick_grn in ('nan', 'None'):
                        pick_grn = grn_number_inv
                    row['Grn Number'] = pick_grn

                    row['Vendor Country']     = country_from_vendor
                    report_rows.append(row)
            else:
                row = inv_row.copy()
                row['Batch ID'] = row['SO Number'] = row['Generated Load ID'] = ''
                row['Country Name'] = row['Pick Quantity'] = row['Remark'] = ''
                row['Allocation Status'] = 'Available'
                row['Vendor Name']       = vendor_name_inv
                row['Invoice Number']    = invoice_number_inv
                row['Grn Number']        = grn_number_inv
                row['Vendor Country']    = country_from_vendor
                report_rows.append(row)

        return pd.DataFrame(report_rows)

    except Exception as e:
        st.error(f"Report Generation Error: {e}")
        return pd.DataFrame()


# ── 4. App UI & Navigation ─────────────────────────────────────────────────────

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

    if current_role == 'ADMIN':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report",
                "🔄 Revert/Delete Picks", "🩹 Damage Items", "🏷️ Vendor Maintain", "⚙️ Admin Settings"]
    elif current_role == 'SYSUSER':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report",
                "🔄 Revert/Delete Picks", "🩹 Damage Items", "🏷️ Vendor Maintain"]
    else:
        menu = ["📊 Dashboard & Tracking", "📋 Inventory Details Report",
                "🔄 Revert/Delete Picks", "🩹 Damage Items"]

    choice = st.sidebar.radio("Navigation Menu", menu)

    # ==========================================================================
    # TAB 1: PICKING OPERATIONS
    # ==========================================================================
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
                        st.error("⚠️ Files swapped! Inventory file සහ Customer Requirement file correct slots හි upload කරන්න.")
                        st.stop()
                    if missing_req:
                        st.error(f"❌ Customer Requirement file හි required columns නොමැත: **{', '.join(missing_req)}**")
                        st.stop()
                    if missing_inv:
                        st.error(f"❌ Inventory file හි required columns නොමැත: **{', '.join(missing_inv)}**")
                        st.stop()

                    req_col_map = {str(c).strip().lower(): str(c).strip() for c in req.columns}
                    req = req.rename(columns={
                        req_col_map.get('so number', 'SO Number'):                 'SO Number',
                        req_col_map.get('country name', 'Country Name'):           'Country Name',
                        req_col_map.get('ship mode: (sea/air)', 'SHIP MODE: (SEA/AIR)'): 'SHIP MODE: (SEA/AIR)',
                        req_col_map.get('product upc', 'Product UPC'):             'Product UPC',
                        req_col_map.get('pick qty', 'PICK QTY'):                   'PICK QTY',
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

                    hist_df = DBManager.read_table("load_history")

                    req['SO Number']           = req['SO Number'].astype(str).str.strip()
                    req['Country Name']        = req['Country Name'].astype(str).str.strip()
                    req['SHIP MODE: (SEA/AIR)'] = req['SHIP MODE: (SEA/AIR)'].astype(str).str.strip()
                    req['Group'] = req['SO Number'] + "_" + req['Country Name'] + "_" + req['SHIP MODE: (SEA/AIR)']

                    load_id_map = {}
                    so_counts   = {}
                    new_hist_entries = []

                    if not hist_df.empty and 'SO Number' in hist_df.columns and 'Generated Load ID' in hist_df.columns and 'SHIP MODE' in hist_df.columns:
                        for _, hist_row in hist_df.iterrows():
                            so = str(hist_row.get('SO Number', '')).strip()
                            ship_m = str(hist_row.get('SHIP MODE', '')).strip().upper()
                            ship_p = {'SEA': 'S', 'AIR': 'A'}.get(ship_m, ship_m[:3] if ship_m else 'X')
                            so_ship_k = f"{so}_{ship_p}"
                            so_counts[so_ship_k] = so_counts.get(so_ship_k, 0) + 1

                    existing_load_ids = set()
                    if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
                        existing_load_ids = set(hist_df['Generated Load ID'].astype(str).tolist())

                    for group, data in req.groupby('Group'):
                        so_num    = data['SO Number'].iloc[0]
                        ship_mode_val = str(data['SHIP MODE: (SEA/AIR)'].iloc[0]).strip().upper()
                        # ── Ship Mode prefix: SEA → S, AIR → A, others → first 3 chars ──
                        ship_prefix = {'SEA': 'S', 'AIR': 'A'}.get(ship_mode_val, ship_mode_val[:3] if ship_mode_val else 'X')
                        # ── Track count per SO + Ship Mode combination ──
                        so_ship_key = f"{so_num}_{ship_prefix}"
                        base_count  = so_counts.get(so_ship_key, 0)
                        count = base_count
                        while True:
                            count += 1
                            candidate_lid = f"SO-{so_num}-{ship_prefix}-{count:03d}"
                            if candidate_lid not in existing_load_ids:
                                break
                        so_counts[so_ship_key] = count
                        existing_load_ids.add(candidate_lid)
                        load_id_map[group] = candidate_lid

                        new_hist_entries.append({
                            'Batch ID':           batch_id,
                            'Generated Load ID':  candidate_lid,
                            'SO Number':          so_num,
                            'Country Name':       data['Country Name'].iloc[0],
                            'SHIP MODE':          data['SHIP MODE: (SEA/AIR)'].iloc[0],
                            'Date':               datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'Pick Status':        'Pending',
                        })

                    req['Generated Load ID'] = req['Group'].map(load_id_map)

                    if new_hist_entries:
                        DBManager.insert_rows("load_history", new_hist_entries)

                    inv_original = inv.copy()
                    inv = reconcile_inventory(inv)

                    pick_df, part_df, summ_df = process_picking(inv, req, batch_id, inv_original=inv_original)

                    # ── Cannot-Pick Diagnostic ──────────────────────────────────
                    cannot_pick_rows = []
                    try:
                        inv_file.seek(0)
                        inv_orig = pd.read_csv(inv_file, keep_default_na=False, na_values=['']) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file, keep_default_na=False, na_values=[''])
                        inv_orig.columns = [str(c).strip() for c in inv_orig.columns]
                        inv_orig_col  = {str(c).strip().lower(): str(c).strip() for c in inv_orig.columns}
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
                        inv_orig[orig_sup_col]    = inv_orig[orig_sup_col].apply(norm_sup)

                        mpd_df = DBManager.read_table("master_pick_data")
                        mpd_picked = {}
                        if not mpd_df.empty and 'Pallet' in mpd_df.columns and 'Actual Qty' in mpd_df.columns:
                            mpd_df['Actual Qty'] = pd.to_numeric(mpd_df['Actual Qty'], errors='coerce').fillna(0)
                            for p, grp in mpd_df.groupby('Pallet'):
                                mpd_picked[str(p).strip()] = grp['Actual Qty'].sum()

                        dmg_df    = DBManager.read_table("damage_items")
                        dmg_pallets = set()
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            dmg_pallets = set(dmg_df['Pallet'].astype(str).str.strip().tolist())

                        inv_orig_by_upc = {}
                        if orig_sup_col in inv_orig.columns:
                            for upc_key, grp in inv_orig.groupby(orig_sup_col):
                                inv_orig_by_upc[str(upc_key).strip()] = grp

                        for _, summ_row in summ_df.iterrows():
                            upc       = str(summ_row.get('UPC', ''))
                            picked    = float(summ_row.get('Picked', 0))
                            requested = float(summ_row.get('Requested', 0))
                            if picked >= requested:
                                continue
                            missing    = requested - picked
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
                                    reason = f"⚠️ Partially picked — available balance: {int(avail)}"
                                elif orig_qty == 0:
                                    reason = "❌ Actual Qty = 0 in inventory"
                                else:
                                    reason = f"❓ Available={int(avail)} but not picked (check UPC match)"

                                cannot_pick_rows.append({
                                    'UPC': upc, 'Pallet': pallet,
                                    'Inv Actual Qty': int(orig_qty),
                                    'Already Picked': int(already),
                                    'Available Now': int(avail),
                                    'Requested': int(requested),
                                    'Shortage': int(missing),
                                    'Reason': reason,
                                })
                    except Exception:
                        pass

                    # ── Save to Supabase ────────────────────────────────────────
                    if not pick_df.empty:
                        pick_save_df = pick_df[MASTER_PICK_HEADERS] if all(c in pick_df.columns for c in MASTER_PICK_HEADERS) else pick_df[[c for c in MASTER_PICK_HEADERS if c in pick_df.columns]]
                        rows_to_insert = pick_save_df.astype(object).where(pd.notnull(pick_save_df), None).to_dict('records')
                        DBManager.insert_rows("master_pick_data", rows_to_insert)

                    if not part_df.empty:
                        mpd_headers = SHEET_HEADERS["Master_Partial_Data"]
                        for col in mpd_headers:
                            if col not in part_df.columns:
                                part_df[col] = None
                        rows_to_insert = part_df[mpd_headers].astype(object).where(pd.notnull(part_df[mpd_headers]), None).to_dict('records')
                        DBManager.insert_rows("master_partial_data", rows_to_insert)

                    # Summary_Data — deduplicate by Load ID
                    existing_summ = DBManager.read_table("summary_data")
                    if not existing_summ.empty and 'Load ID' in existing_summ.columns and not summ_df.empty:
                        new_load_ids = set(summ_df['Load ID'].astype(str).tolist())
                        DBManager.delete_where("summary_data", "Load ID", list(new_load_ids))
                    if not summ_df.empty:
                        DBManager.insert_rows("summary_data", summ_df.astype(object).where(pd.notnull(summ_df), None).to_dict('records'))

                    # ── Build Excel output ──────────────────────────────────────
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
                                    pick_df_excel[c] = pick_df_excel[inv_c].where(pick_df_excel[inv_c].notna(), pick_df_excel.get(c, ''))
                                    pick_df_excel.drop(columns=[inv_c], inplace=True)
                            EXCEL_PICK_COLS = MASTER_PICK_HEADERS + ['Gen Pallet ID']
                            for col in EXCEL_PICK_COLS:
                                if col not in pick_df_excel.columns:
                                    pick_df_excel[col] = ''
                            pick_df_excel = pick_df_excel[EXCEL_PICK_COLS].drop(columns=[c for c in ['_pallet_key'] if c in pick_df_excel.columns], errors='ignore')
                            pick_df_excel.to_excel(writer, sheet_name='Pick_Report', index=False)

                            hdr_fmt     = wb.add_format({'bold': True, 'bg_color': '#1A1A1A', 'font_color': '#FFFFFF', 'border': 1})
                            partial_hdr = wb.add_format({'bold': True, 'bg_color': '#1A6B3C', 'font_color': '#FFFFFF', 'border': 1})
                            int_fmt     = wb.add_format({'num_format': '0'})
                            float_fmt   = wb.add_format({'num_format': '0.######'})
                            BIG_INT_COLS = {'Supplier', 'Invoice Number 1', 'Stored Attribute Id', 'Gate Pass Id', 'Client So Line', 'Asn Line Number', 'S Qty'}
                            FLOAT_COLS   = {'Received Gross Weight', 'Current Gross Weight', 'Received Net Weight', 'Current Net Weight', 'Cbm', 'Container Type'}
                            ws_pick_xl = writer.sheets['Pick_Report']
                            for ci, col_name in enumerate(EXCEL_PICK_COLS):
                                ws_pick_xl.write(0, ci, col_name, partial_hdr if col_name == 'Gen Pallet ID' else hdr_fmt)
                                ws_pick_xl.set_column(ci, ci, 20 if col_name == 'Gen Pallet ID' else 18)
                                if col_name in BIG_INT_COLS:
                                    for ri in range(1, len(pick_df_excel) + 1):
                                        val = pick_df_excel.iloc[ri-1][col_name]
                                        if pd.notna(val) and str(val) not in ('', 'nan', 'None'):
                                            try: ws_pick_xl.write_number(ri, ci, int(float(str(val))), int_fmt)
                                            except: ws_pick_xl.write(ri, ci, str(val))
                                elif col_name in FLOAT_COLS:
                                    for ri in range(1, len(pick_df_excel) + 1):
                                        val = pick_df_excel.iloc[ri-1][col_name]
                                        if pd.notna(val) and str(val) not in ('', 'nan', 'None'):
                                            try: ws_pick_xl.write_number(ri, ci, float(str(val)), float_fmt)
                                            except: ws_pick_xl.write(ri, ci, str(val))
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

                    st.session_state['processed_excel']  = output.getvalue()
                    st.session_state['summary_df']        = summ_df
                    st.session_state['batch_id']          = batch_id
                    st.session_state['show_verification'] = True
                    st.session_state['cannot_pick_rows']  = cannot_pick_rows
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
                    if '✅' in str(row.get('Reason', '')): return ['background-color: #fff3cd'] * len(row)
                    elif '🔴' in str(row.get('Reason', '')): return ['background-color: #ffe0e0'] * len(row)
                    elif '⚠️' in str(row.get('Reason', '')): return ['background-color: #fff8e1'] * len(row)
                    elif '❌' in str(row.get('Reason', '')): return ['background-color: #fce4ec'] * len(row)
                    return [''] * len(row)

                try:
                    st.dataframe(cp_df.style.apply(highlight_reason, axis=1), use_container_width=True, hide_index=True)
                except:
                    st.dataframe(cp_df.astype(str), use_container_width=True, hide_index=True)

                out_cp = io.BytesIO()
                with pd.ExcelWriter(out_cp, engine='xlsxwriter') as writer:
                    cp_df.to_excel(writer, sheet_name='Cannot_Pick', index=False)
                st.download_button("⬇️ Download Cannot-Pick Report", data=out_cp.getvalue(),
                    file_name=f"Cannot_Pick_{st.session_state['batch_id']}.xlsx",
                    mime="application/vnd.ms-excel")

            verify_check = st.checkbox("✅ මම Customer Requirement එක සහ Picked Data නිවැරදිදැයි පරීක්ෂා කළෙමි.")
            if verify_check:
                st.download_button("⬇️ Download Verified Processed Report",
                    data=st.session_state['processed_excel'],
                    file_name=f"WMS_{st.session_state['batch_id']}.xlsx",
                    mime="application/vnd.ms-excel", use_container_width=True)
                show_confetti()

    # ==========================================================================
    # TAB 2: DASHBOARD & TRACKING
    # ==========================================================================
    elif choice == "📊 Dashboard & Tracking":
        col_t1, col_t2 = st.columns([4, 1])
        col_t1.title("📊 Load Tracking & Dashboard")
        if col_t2.button("🔄 Refresh Data", use_container_width=True):
            DBManager.invalidate()
            st.rerun()

        _batch  = DBManager.batch_read(["load_history", "summary_data", "master_pick_data"])
        hist_df = _batch["load_history"]
        summ_df = _batch["summary_data"]
        pick_df = _batch["master_pick_data"]

        total_loads      = hist_df['Generated Load ID'].nunique() if not hist_df.empty and 'Generated Load ID' in hist_df.columns else 0
        total_picks      = len(pick_df) if not pick_df.empty else 0
        pending_loads    = len(hist_df[hist_df['Pick Status'] == 'Pending'])    if not hist_df.empty and 'Pick Status' in hist_df.columns else 0
        processing_loads = len(hist_df[hist_df['Pick Status'] == 'Processing']) if not hist_df.empty and 'Pick Status' in hist_df.columns else 0

        st.subheader("📈 Overall System Summary")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Load IDs",   total_loads)
        m2.metric("Total Picks Made", total_picks)
        m3.metric("Pending Loads",    pending_loads)
        m4.metric("Processing Loads", processing_loads)
        st.divider()

        if total_loads == 0:
            st.info("දැනට පද්ධතියේ කිසිදු දත්තයක් නොමැත. 'Picking Operations' මගින් දත්ත ඇතුලත් කරන්න.")
        else:
            st.subheader("📦 Active Load ID Overview")
            st.caption("Cancelled සහ Completed Load IDs මෙහි නොපෙන්වයි.")

            if not hist_df.empty and 'Generated Load ID' in hist_df.columns and 'Pick Status' in hist_df.columns:
                active_loads = hist_df[~hist_df['Pick Status'].astype(str).isin(['Cancelled', 'Completed'])].copy()

                if active_loads.empty:
                    st.info("සියලු Loads Completed හෝ Cancelled වී ඇත.")
                else:
                    load_ids = active_loads['Generated Load ID'].dropna().unique().tolist()

                    filter_col1, filter_col2 = st.columns([2, 4])
                    status_filter = filter_col1.selectbox("🔽 Filter by Status:",
                        ["All", "Pending", "PL Pending", "Processing"], key="dash_status_filter")
                    if status_filter != "All":
                        filtered_active = active_loads[active_loads['Pick Status'].astype(str) == status_filter]
                        load_ids = filtered_active['Generated Load ID'].dropna().unique().tolist()

                    summ_by_load = {}
                    if not summ_df.empty and 'Load ID' in summ_df.columns:
                        for col in ['Variance', 'Requested', 'Picked']:
                            summ_df[col] = pd.to_numeric(summ_df.get(col, 0), errors='coerce').fillna(0)
                        for lid_s in summ_df['Load ID'].dropna().unique():
                            rows = summ_df[summ_df['Load ID'].astype(str) == str(lid_s)]
                            summ_by_load[str(lid_s)] = {
                                'requested': rows['Requested'].sum(),
                                'picked':    rows['Picked'].sum(),
                                'variance':  rows['Variance'].sum(),
                            }

                    zero_pick_ids, shortage_ids, full_pick_ids = [], [], []
                    for lid in load_ids:
                        s = summ_by_load.get(str(lid), {})
                        req_q    = s.get('requested', 0)
                        picked_q = s.get('picked', 0)
                        var_q    = s.get('variance', 0)
                        if picked_q == 0 and req_q > 0: zero_pick_ids.append(lid)
                        elif var_q > 0:                 shortage_ids.append(lid)
                        else:                           full_pick_ids.append(lid)

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
                                    lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum())
                                pick_qty_by_lid = _qty_grp.to_dict()

                    def render_load_list(id_list, category_color, category_label):
                        # ── Header row ──────────────────────────────────────────────────
                        st.markdown(f"""
<div style="display:grid;grid-template-columns:2fr 1fr 1.2fr 1fr 1fr 1fr 1fr 1.5fr;gap:4px;
     background:{category_color}15;border:1px solid {category_color}40;
     border-radius:8px 8px 0 0;padding:7px 12px;
     font-size:11px;font-weight:700;color:#444;margin-top:4px;">
    <div>Load ID</div><div>SO</div><div>Country</div><div>Ship</div>
    <div>Date</div><div>Lines</div><div>Qty</div><div>Status</div>
</div>""", unsafe_allow_html=True)

                        for lid in id_list:
                            load_row     = active_loads[active_loads['Generated Load ID'] == lid].iloc[0]
                            status       = str(load_row.get('Pick Status', 'Pending'))
                            so_num       = str(load_row.get('SO Number', '-'))
                            country      = str(load_row.get('Country Name', '-'))
                            ship         = str(load_row.get('SHIP MODE', '-'))
                            date         = str(load_row.get('Date', '-'))[:10]
                            lid_key      = str(lid).strip()
                            pick_count   = pick_counts_by_lid.get(lid_key, 0)
                            pick_qty_val = pick_qty_by_lid.get(lid_key, 0)
                            s            = summ_by_load.get(str(lid), {})
                            variance     = s.get('variance', 0)
                            requested    = s.get('requested', 0)
                            picked_q     = s.get('picked', 0)
                            fill_pct     = min(int((picked_q / requested * 100)) if requested > 0 else 0, 100)

                            status_bg  = {
                                'Pending':    '#fff3cd', 'PL Pending': '#fff0d6',
                                'Processing': '#cce5ff', 'Completed':  '#d4edda',
                                'Cancelled':  '#f8d7da',
                            }.get(status, '#f0f0f0')
                            status_col = {
                                'Pending':    '#856404', 'PL Pending': '#7a4f00',
                                'Processing': '#004085', 'Completed':  '#155724',
                                'Cancelled':  '#721c24',
                            }.get(status, '#333')
                            shortage_tag = (
                                f'<span style="font-size:9px;background:#ffe0e0;color:#c0392b;'
                                f'padding:1px 5px;border-radius:4px;margin-left:4px;">⚠️ -{int(variance)}</span>'
                            ) if variance > 0 else ''

                            # ── Info row (HTML grid — no interactive widgets) ─────────────
                            st.markdown(f"""
<div style="display:grid;grid-template-columns:2fr 1fr 1.2fr 1fr 1fr 1fr 1fr 1.5fr;gap:4px;
     border-left:3px solid {category_color};border-bottom:1px solid #eee;
     padding:7px 12px;background:#fff;font-size:11px;color:#333;align-items:center;">
    <div>
        <div style="font-weight:700;color:#1a1a1a;">{lid}{shortage_tag}</div>
        <div style="height:3px;background:#e0e0e0;border-radius:2px;margin-top:3px;">
            <div style="height:3px;width:{fill_pct}%;background:{category_color};border-radius:2px;"></div>
        </div>
        <div style="font-size:9px;color:#888;">{fill_pct}% picked</div>
    </div>
    <div>{so_num}</div><div>{country}</div><div>{ship}</div><div>{date}</div>
    <div><b>{pick_count}</b></div><div><b>{int(pick_qty_val)}</b></div>
    <div><span style="background:{status_bg};color:{status_col};font-size:10px;
         font-weight:700;padding:2px 10px;border-radius:10px;">{status}</span></div>
</div>""", unsafe_allow_html=True)

                            # ── Status update row (selectbox + Save button) ──────────────
                            _u1, _u2 = st.columns([5, 1])
                            safe_idx = STATUS_OPTIONS.index(status) if status in STATUS_OPTIONS else 0
                            new_st = _u1.selectbox("", STATUS_OPTIONS, index=safe_idx,
                                                   key=f"st_{lid}", label_visibility="collapsed")
                            if _u2.button("💾 Save", key=f"upd_{lid}", use_container_width=True):
                                try:
                                    ok = DBManager.update_cell("load_history", "Generated Load ID", str(lid), "Pick Status", new_st)
                                    if ok and new_st == "Cancelled":
                                        mpd = DBManager.read_table("master_pick_data")
                                        lid_col = next((c for c in mpd.columns if str(c).strip().lower() == 'load id'), None)
                                        if not mpd.empty and lid_col:
                                            filtered_mpd = mpd[mpd[lid_col].astype(str).str.strip() != str(lid).strip()]
                                            DBManager._overwrite_table("master_pick_data", filtered_mpd)
                                        st.success(f"✅ {lid} → Cancelled | Master_Pick_Data records deleted.")
                                    elif ok:
                                        st.success(f"✅ {lid} → {new_st}")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(f"Update error: {ex}")

                    if zero_pick_ids:
                        st.markdown("#### 🔴 Not Yet Picked")
                        render_load_list(zero_pick_ids, '#e74c3c', 'Not Yet Picked')
                    if shortage_ids:
                        st.markdown("#### 🟡 Shortage")
                        render_load_list(shortage_ids, '#f39c12', 'Shortage')
                    if full_pick_ids:
                        st.markdown("#### 🟢 Fully Picked")
                        render_load_list(full_pick_ids, '#27ae60', 'Fully Picked')

            st.divider()
            st.subheader("🔍 Search & Download Picks")
            search_by   = st.selectbox("Search By:", ["Load Id", "SO Number", "Pallet"], key="dash_search_by")
            search_term = st.text_input("🔍 Search:", key="dash_search_term")

            col_map_pick = {"Load Id": "generated load id", "SO Number": "so number", "Pallet": "pallet"}
            col_map_summ = {"Load Id": "load id", "SO Number": "so number", "Pallet": None}

            filtered_picks = pd.DataFrame()
            if search_term:
                if not pick_df.empty:
                    actual_col_name = next((c for c in pick_df.columns if str(c).strip().lower() == col_map_pick[search_by]), None)
                    if actual_col_name:
                        if search_by == "Load Id":
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.strip() == str(search_term).strip()]
                        else:
                            filtered_picks = pick_df[pick_df[actual_col_name].astype(str).str.contains(str(search_term).strip(), case=False, na=False)]

                filtered_summ   = pd.DataFrame()
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
                        st.download_button(f"⬇️ Download Pick Report — {search_term}", data=out_pick_dl.getvalue(),
                            file_name=f"Pick_Report_{safe_term}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel", use_container_width=True, type="primary")
                        dl_qty   = pd.to_numeric(filtered_picks.get('Actual Qty', pd.Series()), errors='coerce').sum()
                        dl_lines = len(filtered_picks)
                        mc1, mc2 = st.columns(2)
                        mc1.metric("Pick Lines",    dl_lines)
                        mc2.metric("Total Pick Qty", int(dl_qty))
                    else:
                        st.info("Download කිරීමට data නොමැත.")

    # ==========================================================================
    # TAB 3: INVENTORY DETAILS REPORT
    # ==========================================================================
    elif choice == "📋 Inventory Details Report":
        st.title("📋 Inventory Details Report")
        inv_report_file = st.file_uploader("Upload Inventory File", type=['csv', 'xlsx'], key="inv_report_uploader")

        if inv_report_file:
            tab_basic, tab_formatted = st.tabs(["📋 Basic Report", "📊 Formatted Pick Report"])

            with tab_basic:
                st.caption("Inventory file allocation status report (Picked / Available / Damage)")
                if st.button("🔍 Generate Basic Report", type="primary", use_container_width=True, key="gen_basic"):
                    with st.spinner("Generating..."):
                        inv_data   = pd.read_csv(inv_report_file, keep_default_na=False, na_values=['']) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file, keep_default_na=False, na_values=[''])
                        report_df  = generate_inventory_details_report(inv_data)
                        if not report_df.empty:
                            st.success(f"✅ Total rows: {len(report_df)}")
                            col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                            if 'Allocation Status' in report_df.columns:
                                col_r1.metric("Total Lines", len(report_df))
                                col_r2.metric("✅ Picked",   len(report_df[report_df['Allocation Status'] == 'Picked']))
                                col_r3.metric("🟢 Available", len(report_df[report_df['Allocation Status'] == 'Available']))
                                col_r4.metric("🔴 Damage",    len(report_df[report_df['Allocation Status'] == 'Damage']))
                            st.dataframe(report_df.astype(str), use_container_width=True)
                            out_basic = io.BytesIO()
                            with pd.ExcelWriter(out_basic, engine='xlsxwriter') as writer:
                                report_df.to_excel(writer, sheet_name='Inventory_Details', index=False)
                                wb = writer.book; ws_b = writer.sheets['Inventory_Details']
                                for fmt, col_val in [('#FFE0E0', 'Damage'), ('#E8F5E9', 'Picked'), ('#E3F2FD', 'Available')]:
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
                st.caption(
                    "නව Logic Flow: Inventory → **inventory_status** DB (upload කරද්දී clear + save, "
                    "අලුත් **Status** column එකක්). සියලු data **Z→A** (id අග→මුල) order එකේ check කරයි. "
                    "L1/L2: Pick Id≠0 → Pick Quantity update. L3: Pick Id=0 → Allocated update. "
                    "L4/L5: Pick Id=0 partial pallet → gen_pallet_id split + Balance. "
                    "L6: Damage එහෙම්ම තබයි. L7: Status≠D → ATS update."
                )

                with st.expander("🗄️ inventory_status table setup (මුල් වරට පමණක් — Supabase)", expanded=False):
                    st.caption("Supabase එකේ මෙම table එක නැත්නම්, මෙම SQL එක එක් වරක් run කරන්න:")
                    st.code(INVENTORY_STATUS_TABLE_SQL, language="sql")

                with st.expander("📂 Master Data CSV Override (Supabase connect නැත්නම්)", expanded=False):
                    st.caption("Supabase ඇත්නම් upload නොකළත් හරි. CSV upload කළොත් ඒ data priority ලැබේ.")
                    col_csv1, col_csv2 = st.columns(2)
                    col_csv3, col_csv4 = st.columns(2)
                    csv_pick_file    = col_csv1.file_uploader("master_pick_data CSV",    type=['csv'], key="fmt_csv_pick")
                    csv_partial_file = col_csv2.file_uploader("master_partial_data CSV", type=['csv'], key="fmt_csv_partial")
                    csv_damage_file  = col_csv3.file_uploader("damage_items CSV",        type=['csv'], key="fmt_csv_damage")
                    csv_vendor_file  = col_csv4.file_uploader("vendor_maintain CSV",     type=['csv'], key="fmt_csv_vendor")

                save_to_db = st.checkbox(
                    "💾 inventory_status DB එකට save කරන්න (upload එකේදී clear → processed rows insert)",
                    value=True, key="fmt_save_db"
                )

                if st.button("📊 Generate Formatted Pick Report", type="primary", use_container_width=True, key="gen_fmt"):
                    with st.spinner("Inventory process කරමින්..."):

                        # ════════════════════════════════════════════════════════════
                        # Helpers
                        # ════════════════════════════════════════════════════════════
                        def _is_blank(v):
                            return v is None or str(v).strip() in ('', 'nan', 'None', 'NaN')

                        def _to_num(v):
                            n = pd.to_numeric(v, errors='coerce')
                            return 0.0 if pd.isna(n) else float(n)

                        def _pid_filled(v):
                            return str(v).strip() not in ('', '0', '0.0', 'nan', 'None', 'NaN')

                        import re as _re
                        _gen_pat = _re.compile(r'^(.+)-P\d+$')
                        def _base_pallet(p):
                            m = _gen_pat.match(str(p).strip())
                            return m.group(1) if m else str(p).strip()

                        def _load_csv_rev(uploaded_file, rev_map):
                            try:
                                uploaded_file.seek(0)
                                df = pd.read_csv(uploaded_file)
                                df.drop(columns=[c for c in ['id', 'created_at'] if c in df.columns],
                                        inplace=True, errors='ignore')
                                df.rename(columns=rev_map, inplace=True)
                                return df
                            except Exception as _e:
                                st.warning(f"CSV read error: {_e}")
                                return pd.DataFrame()

                        # ════════════════════════════════════════════════════════════
                        # STEP 1 — Read uploaded Inventory
                        # ════════════════════════════════════════════════════════════
                        inv_report_file.seek(0)
                        if inv_report_file.name.endswith('.csv'):
                            inv_data = pd.read_csv(inv_report_file, keep_default_na=False, na_values=[''])
                        else:
                            inv_data = pd.read_excel(inv_report_file, keep_default_na=False, na_values=[''])
                        inv_data.columns = [str(c).strip() for c in inv_data.columns]
                        _inv_lc = {c.lower(): c for c in inv_data.columns}

                        def _icol(name):
                            return _inv_lc.get(name.strip().lower())

                        _C_PALLET = _icol('pallet')
                        _C_AQTY   = _icol('actual qty')
                        _C_PID    = _icol('pick id')

                        if _C_PALLET is None or _C_AQTY is None:
                            st.error("❌ Inventory file එකේ 'Pallet' සහ 'Actual Qty' columns තිබිය යුතුය.")
                            st.stop()
                        if _C_PID is None:
                            st.warning("⚠️ 'Pick Id' column නෑ — සියලු rows unpicked (Pick Id=0) ලෙස සලකයි.")

                        # ════════════════════════════════════════════════════════════
                        # STEP 2 — Load master data (DB or CSV override)
                        # ════════════════════════════════════════════════════════════
                        if csv_pick_file:
                            mpd_df = _load_csv_rev(csv_pick_file, PICK_COL_MAP_REV)
                            st.info("ℹ️ master_pick_data: CSV use කරයි")
                        else:
                            try:    mpd_df = DBManager.read_table("master_pick_data")
                            except: mpd_df = pd.DataFrame()

                        if csv_partial_file:
                            part_df = _load_csv_rev(csv_partial_file, PARTIAL_COL_MAP_REV)
                            st.info("ℹ️ master_partial_data: CSV use කරයි")
                        else:
                            try:    part_df = DBManager.read_table("master_partial_data")
                            except: part_df = pd.DataFrame()

                        if csv_damage_file:
                            dmg_df = _load_csv_rev(csv_damage_file, DAMAGE_COL_MAP_REV)
                            st.info("ℹ️ damage_items: CSV use කරයි")
                        else:
                            try:    dmg_df = DBManager.read_table("damage_items")
                            except: dmg_df = pd.DataFrame()

                        if csv_vendor_file:
                            vendor_df = _load_csv_rev(csv_vendor_file, VENDOR_COL_MAP_REV)
                            st.info("ℹ️ vendor_maintain: CSV use කරයි")
                        else:
                            try:    vendor_df = DBManager.read_table("vendor_maintain")
                            except: vendor_df = pd.DataFrame()

                        # ── MPD column resolve ──
                        def _col(df, name, default=None):
                            lc = {str(c).strip().lower(): str(c) for c in df.columns}
                            return lc.get(name.strip().lower(), default)

                        # ════════════════════════════════════════════════════════════
                        # STEP 3 — Build lookup maps
                        # ════════════════════════════════════════════════════════════
                        # MPD exact map: (pallet, qty) → {country, order}   [Logic 1 & 3]
                        mpd_exact = {}
                        if not mpd_df.empty:
                            mp_p = _col(mpd_df, 'pallet', 'Pallet')
                            mp_q = _col(mpd_df, 'actual qty', 'Actual Qty')
                            mp_c = _col(mpd_df, 'country name', 'Country Name')
                            mp_o = _col(mpd_df, 'generated load id', 'Generated Load ID')
                            for _, r in mpd_df.iterrows():
                                pal = str(r.get(mp_p, '')).strip()
                                if not pal:
                                    continue
                                key = (pal, round(_to_num(r.get(mp_q, 0)), 6))
                                if key not in mpd_exact:
                                    mpd_exact[key] = {
                                        'country': str(r.get(mp_c, '')).strip(),
                                        'order':   str(r.get(mp_o, '')).strip(),
                                    }

                        # Partial maps
                        # gen_exact: (gen_pallet_id, partial_qty) → {country, order}  [Logic 2]
                        # part_by_pallet: orig pallet → [ {gen, pqty, country, order, invoice, grn, vendor} ]  [Logic 4/5]
                        gen_exact = {}
                        part_by_pallet = {}
                        if not part_df.empty:
                            pp = _col(part_df, 'pallet', 'Pallet')
                            pg = _col(part_df, 'gen pallet id', 'Gen Pallet ID')
                            pq = _col(part_df, 'partial qty', 'Partial Qty')
                            pc = _col(part_df, 'country name', 'Country Name')
                            po = _col(part_df, 'load id', 'Load ID')
                            pi = _col(part_df, 'invoice number', 'Invoice Number')
                            pgr = _col(part_df, 'grn number', 'Grn Number')
                            pv = _col(part_df, 'vendor name', 'Vendor Name')
                            for _, r in part_df.iterrows():
                                opal = str(r.get(pp, '')).strip()
                                gpal = str(r.get(pg, '')).strip()
                                pqty = _to_num(r.get(pq, 0))
                                entry = {
                                    'gen': gpal, 'pqty': pqty,
                                    'country': str(r.get(pc, '')).strip(),
                                    'order':   str(r.get(po, '')).strip(),
                                    'invoice': str(r.get(pi, '')).strip(),
                                    'grn':     str(r.get(pgr, '')).strip(),
                                    'vendor':  str(r.get(pv, '')).strip(),
                                }
                                if gpal:
                                    gen_exact[(gpal, round(pqty, 6))] = entry
                                if opal:
                                    part_by_pallet.setdefault(opal, []).append(entry)

                        # Damage maps  [Logic 6]
                        damage_qty = {}          # (pallet, remark) → qty
                        damage_pallets = set()
                        damage_remarks = []
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            _d = dmg_df.copy()
                            _d['_pal'] = _d['Pallet'].astype(str).str.strip()
                            _d['_rmk'] = _d['Remark'].astype(str).str.strip() if 'Remark' in _d.columns else 'QC Repair'
                            _d['_q']   = pd.to_numeric(_d.get('Actual Qty', 0), errors='coerce').fillna(0)
                            _d.loc[_d['_rmk'].isin(['', 'nan', 'None']), '_rmk'] = 'QC Repair'
                            damage_remarks = sorted(_d['_rmk'].unique().tolist())
                            for (p, rmk), q in _d.groupby(['_pal', '_rmk'])['_q'].sum().items():
                                damage_qty[(p, rmk)] = q
                            damage_pallets = set(_d['_pal'])
                        if not damage_remarks:
                            damage_remarks = ['QC Repair']

                        # Vendor → Country map (COO)
                        vendor_country = {}
                        if not vendor_df.empty and 'Vendor Name' in vendor_df.columns and 'Country' in vendor_df.columns:
                            for v, c in zip(vendor_df['Vendor Name'], vendor_df['Country']):
                                if not _is_blank(v) and not _is_blank(c):
                                    vendor_country[str(v).strip().lower()] = str(c).strip()

                        # ════════════════════════════════════════════════════════════
                        # STEP 4 — Build inventory_status records (one per inv row)
                        #          + Z→A order (id අග→මුල = reverse upload order)
                        # ════════════════════════════════════════════════════════════
                        BASE_HEADERS = ['Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                                        'Client So', 'Pallet', 'Supplier Hu', 'Supplier', 'Lot Number',
                                        'Style', 'Color', 'Size', 'Client So 2', 'Inventory Type', 'Actual Qty']

                        def base_row(inv_row, pallet=None, actual_qty=None):
                            row = {}
                            for h in BASE_HEADERS:
                                if h == 'Pallet':
                                    row[h] = pallet if pallet is not None else str(inv_row.get(_C_PALLET, '')).strip()
                                elif h == 'Actual Qty':
                                    row[h] = actual_qty if actual_qty is not None else _to_num(inv_row.get(_C_AQTY, 0))
                                elif h == 'Client So 2':
                                    cs = _icol('client so')
                                    row[h] = str(inv_row.get(cs, '')).strip() if cs else ''
                                else:
                                    src = _icol(h)
                                    row[h] = str(inv_row.get(src, '')).strip() if src else ''
                            # report fields (blank defaults)
                            row['Pick Quantity'] = ''
                            row['Allocated'] = ''
                            row['Destination Country'] = ''
                            row['Order NO'] = ''
                            row['ATS'] = ''
                            row['Balance Qty'] = ''
                            row['COO'] = ''
                            row['Pick Id'] = str(inv_row.get(_C_PID, '0')).strip() if _C_PID else '0'
                            for rmk in damage_remarks:
                                row[rmk] = ''
                            row['Status'] = ''
                            # original inventory pallet (survives splits/balance) — diff key only
                            row['_src_pallet'] = str(inv_row.get(_C_PALLET, '')).strip()
                            return row

                        def set_coo(row):
                            vn = str(row.get('Vendor Name', '')).strip()
                            row['COO'] = vendor_country.get(vn.lower(), '') if vn else ''

                        # Z→A: reverse the upload order (last id first)
                        inv_records = list(inv_data.iterrows())[::-1]

                        out_rows = []        # final report rows
                        used_gens = set()    # gen_pallet_ids already allocated (across rows)

                        # ════════════════════════════════════════════════════════════
                        # STEP 5 — Run logics (Z→A) over each inventory row
                        # ════════════════════════════════════════════════════════════
                        for _, inv_row in inv_records:
                            pallet = str(inv_row.get(_C_PALLET, '')).strip()
                            aqty   = _to_num(inv_row.get(_C_AQTY, 0))
                            pid    = str(inv_row.get(_C_PID, '0')).strip() if _C_PID else '0'
                            picked = _pid_filled(pid)

                            # ── Logic 6: Damage — keep as-is, exclude from all other logics ──
                            if pallet in damage_pallets:
                                r = base_row(inv_row)
                                for rmk in damage_remarks:
                                    q = damage_qty.get((pallet, rmk), '')
                                    r[rmk] = q if q != '' else ''
                                # if no remark-specific qty, put full Actual Qty under first remark
                                if all(_is_blank(r[rk]) for rk in damage_remarks):
                                    r[damage_remarks[0]] = aqty
                                r['Status'] = 'D'
                                set_coo(r)
                                out_rows.append(r)
                                continue

                            matched = False

                            if picked:
                                # ── Logic 1: Pick Id≠0 → MPD (pallet+qty) exact ──
                                hit = mpd_exact.get((pallet, round(aqty, 6)))
                                if hit:
                                    r = base_row(inv_row)
                                    r['Pick Quantity'] = aqty
                                    r['Destination Country'] = hit['country']
                                    r['Order NO'] = hit['order']
                                    r['Status'] = 'D'
                                    set_coo(r)
                                    out_rows.append(r)
                                    matched = True

                                # ── Logic 2: Pick Id≠0 & not D → Partial (gen+pqty) exact ──
                                if not matched:
                                    hit = gen_exact.get((pallet, round(aqty, 6)))
                                    if hit:
                                        r = base_row(inv_row)
                                        r['Pick Quantity'] = aqty
                                        r['Destination Country'] = hit['country']
                                        r['Order NO'] = hit['order']
                                        if _is_blank(r['Invoice Number']) and not _is_blank(hit['invoice']):
                                            r['Invoice Number'] = hit['invoice']
                                        if _is_blank(r['Grn Number']) and not _is_blank(hit['grn']):
                                            r['Grn Number'] = hit['grn']
                                        if _is_blank(r['Vendor Name']) and not _is_blank(hit['vendor']):
                                            r['Vendor Name'] = hit['vendor']
                                        r['Status'] = 'D'
                                        set_coo(r)
                                        out_rows.append(r)
                                        matched = True
                            else:
                                # ── Logic 3: Pick Id=0 & not D → MPD (pallet+qty) exact → Allocated ──
                                hit = mpd_exact.get((pallet, round(aqty, 6)))
                                if hit:
                                    r = base_row(inv_row)
                                    r['Allocated'] = aqty
                                    r['Destination Country'] = hit['country']
                                    r['Order NO'] = hit['order']
                                    r['Status'] = 'D'
                                    set_coo(r)
                                    out_rows.append(r)
                                    matched = True

                                # ── Logic 4 & 5: Pick Id=0 & not D → Partial pallet match → split ──
                                if not matched and pallet in part_by_pallet:
                                    # gen_pallet_ids z→a (descending) — not yet used
                                    gens = [e for e in part_by_pallet[pallet]
                                            if e['gen'] and e['gen'] not in used_gens]
                                    gens = sorted(gens, key=lambda e: str(e['gen']), reverse=True)
                                    if gens:
                                        budget = aqty            # can't split more than Actual Qty
                                        allocated_total = 0.0
                                        first = gens[0]
                                        for e in gens:
                                            if budget <= 0:
                                                break
                                            give = min(e['pqty'], budget)
                                            if give <= 0:
                                                continue
                                            r = base_row(inv_row, pallet=e['gen'], actual_qty=give)
                                            r['Allocated'] = give
                                            r['Destination Country'] = e['country']
                                            r['Order NO'] = e['order']
                                            if _is_blank(r['Invoice Number']) and not _is_blank(e['invoice']):
                                                r['Invoice Number'] = e['invoice']
                                            if _is_blank(r['Grn Number']) and not _is_blank(e['grn']):
                                                r['Grn Number'] = e['grn']
                                            if _is_blank(r['Vendor Name']) and not _is_blank(e['vendor']):
                                                r['Vendor Name'] = e['vendor']
                                            r['Status'] = 'D'
                                            set_coo(r)
                                            out_rows.append(r)
                                            used_gens.add(e['gen'])
                                            allocated_total += give
                                            budget -= give

                                        # ── Logic 5: leftover balance stays on original pallet ──
                                        leftover = round(aqty - allocated_total, 6)
                                        if leftover > 0:
                                            br = base_row(inv_row, pallet=pallet, actual_qty=leftover)
                                            br['Balance Qty'] = leftover
                                            br['ATS'] = leftover    # balance = available to ship
                                            br['Destination Country'] = first['country']
                                            br['Order NO'] = first['order']
                                            br['Status'] = 'D'
                                            set_coo(br)
                                            out_rows.append(br)
                                        matched = True

                            # ── Logic 7: not matched (Status≠D) → ATS ──
                            if not matched:
                                r = base_row(inv_row)
                                r['ATS'] = aqty
                                # Status stays blank (≠D) — pure ATS line
                                set_coo(r)
                                out_rows.append(r)

                        # ════════════════════════════════════════════════════════════
                        # STEP 6 — Assign Row Order (Z→A) & build report dataframe
                        # ════════════════════════════════════════════════════════════
                        for i, r in enumerate(out_rows):
                            r['Row Order'] = i + 1

                        report_cols = (BASE_HEADERS
                                       + ['Pick Quantity', 'Allocated', 'Destination Country', 'Order NO']
                                       + damage_remarks + ['ATS', 'COO'])
                        fmt_df = pd.DataFrame(out_rows)
                        for c in report_cols:
                            if c not in fmt_df.columns:
                                fmt_df[c] = ''
                        # drop blank-pallet rows
                        fmt_df = fmt_df[fmt_df['Pallet'].astype(str).str.strip()
                                        .replace({'nan': '', 'None': ''}) != ''].reset_index(drop=True)
                        report_df = fmt_df[report_cols].copy()

                        # ════════════════════════════════════════════════════════════
                        # STEP 7 — Save to inventory_status DB (clear + insert)
                        # ════════════════════════════════════════════════════════════
                        if save_to_db:
                            db_cols = [c for c in INVSTATUS_COL_MAP.keys() if c in fmt_df.columns]
                            # map dynamic damage remark → QC Repair column for DB
                            db_df = fmt_df.copy()
                            if 'QC Repair' not in db_df.columns:
                                qc_total = pd.Series([0.0] * len(db_df))
                                for rmk in damage_remarks:
                                    if rmk in db_df.columns:
                                        qc_total = qc_total + pd.to_numeric(db_df[rmk], errors='coerce').fillna(0)
                                db_df['QC Repair'] = qc_total.replace(0, '')
                            db_cols = [c for c in INVSTATUS_COL_MAP.keys() if c in db_df.columns]
                            db_out = db_df[db_cols].copy()
                            try:
                                ok = DBManager.replace_table("inventory_status", db_out)
                                if ok:
                                    st.success(f"✅ inventory_status DB clear → **{len(db_out)}** rows save කළා (Status column සමඟ).")
                            except Exception as _dberr:
                                st.warning(f"⚠️ DB save skip කළා: {_dberr}")

                        # ════════════════════════════════════════════════════════════
                        # STEP 8 — Totals & metrics
                        # ════════════════════════════════════════════════════════════
                        inv_total_qty = pd.to_numeric(inv_data[_C_AQTY], errors='coerce').fillna(0).sum()
                        rpt_total_qty = pd.to_numeric(report_df['Actual Qty'], errors='coerce').fillna(0).sum()
                        total_pick  = pd.to_numeric(report_df['Pick Quantity'], errors='coerce').fillna(0).sum()
                        total_alloc = pd.to_numeric(report_df['Allocated'], errors='coerce').fillna(0).sum()
                        total_ats   = pd.to_numeric(report_df['ATS'], errors='coerce').fillna(0).sum()
                        total_dmg   = sum(pd.to_numeric(report_df[r], errors='coerce').fillna(0).sum()
                                          for r in damage_remarks if r in report_df.columns)
                        accounted   = total_pick + total_alloc + total_ats + total_dmg
                        qty_match   = abs(inv_total_qty - rpt_total_qty) < 0.01

                        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
                        sc1.metric("Report Lines", len(report_df))
                        sc2.metric("Pick Qty", int(total_pick))
                        sc3.metric("Allocated Qty", int(total_alloc))
                        sc4.metric("ATS Qty", int(total_ats))
                        sc5.metric("Damage Qty", int(total_dmg))

                        if qty_match:
                            st.success(f"✅ Qty Match: Inventory({int(inv_total_qty)}) = Report({int(rpt_total_qty)})")
                        else:
                            st.warning(f"⚠️ Qty Mismatch: Inventory({int(inv_total_qty)}) ≠ Report({int(rpt_total_qty)}) | Diff={int(inv_total_qty - rpt_total_qty)}")

                        _d_status = 'D' if 'Status' not in fmt_df.columns else None
                        d_count = int((fmt_df['Status'].astype(str).str.upper() == 'D').sum()) if 'Status' in fmt_df.columns else 0
                        st.info(f"🔖 Status = D : **{d_count}** rows | ATS (Status≠D) : **{len(fmt_df) - d_count}** rows")

                        # ════════════════════════════════════════════════════════════
                        # STEP 9 — Input vs Output differences (per base-pallet)
                        # ════════════════════════════════════════════════════════════
                        in_by_pallet = {}
                        for _, ir in inv_data.iterrows():
                            p = str(ir.get(_C_PALLET, '')).strip()
                            if p:
                                in_by_pallet[p] = in_by_pallet.get(p, 0.0) + _to_num(ir.get(_C_AQTY, 0))

                        # output keyed by ORIGINAL inventory pallet (_src_pallet) — survives splits/balance
                        out_by_pallet = {}
                        for _, rr in fmt_df.iterrows():
                            sp = str(rr.get('_src_pallet', '')).strip()
                            if sp:
                                out_by_pallet[sp] = out_by_pallet.get(sp, 0.0) + _to_num(rr.get('Actual Qty', 0))

                        diff_rows = []
                        for p in sorted(set(in_by_pallet) | set(out_by_pallet)):
                            iq = round(in_by_pallet.get(p, 0.0), 4)
                            oq = round(out_by_pallet.get(p, 0.0), 4)
                            diff = round(iq - oq, 4)
                            if abs(diff) > 0.001:
                                if p not in out_by_pallet:   note = 'Output එකේ නෑ'
                                elif p not in in_by_pallet:  note = 'Input එකේ නෑ'
                                else:                        note = 'Qty වෙනස්'
                                diff_rows.append({'Pallet': p, 'Input Qty': iq,
                                                  'Output Qty': oq, 'Difference': diff, 'Note': note})
                        diff_df = pd.DataFrame(diff_rows)

                        if diff_df.empty:
                            st.success("✅ Input vs Output — සියලු pallets tally වෙයි (වෙනසක් නෑ).")
                        else:
                            st.warning(f"⚠️ Input vs Output වෙනස්කම්: **{len(diff_df)}** pallets (Differences sheet බලන්න).")
                            st.dataframe(diff_df.astype(str), use_container_width=True)

                        st.divider()
                        st.dataframe(report_df.astype(str), use_container_width=True, height=420)

                        # ════════════════════════════════════════════════════════════
                        # STEP 10 — Excel export (Pick_Report + Summary + Differences)
                        # ════════════════════════════════════════════════════════════
                        total_row = {}
                        for c in report_cols:
                            if c in (['Actual Qty', 'Pick Quantity', 'Allocated', 'ATS'] + damage_remarks):
                                total_row[c] = pd.to_numeric(report_df[c], errors='coerce').fillna(0).sum()
                            elif c == 'Vendor Name':
                                total_row[c] = 'TOTAL'
                            else:
                                total_row[c] = ''
                        report_with_total = pd.concat([report_df, pd.DataFrame([total_row])], ignore_index=True)

                        out_fmt = io.BytesIO()
                        with pd.ExcelWriter(out_fmt, engine='xlsxwriter') as writer:
                            report_with_total.to_excel(writer, sheet_name='Pick_Report', index=False)
                            wb = writer.book
                            ws = writer.sheets['Pick_Report']

                            hdr_xl   = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#fff', 'border': 1, 'font_size': 10})
                            pick_xl  = wb.add_format({'bg_color': '#E8F5E9', 'border': 1, 'font_size': 10})
                            alloc_xl = wb.add_format({'bg_color': '#E3F2FD', 'border': 1, 'font_size': 10, 'bold': True})
                            dmg_xl   = wb.add_format({'bg_color': '#FFE0E0', 'border': 1, 'font_size': 10})
                            ats_xl   = wb.add_format({'bg_color': '#FFF3CD', 'border': 1, 'font_size': 10, 'bold': True})
                            vnd_xl   = wb.add_format({'bg_color': '#FFF9C4', 'border': 1, 'font_size': 10})
                            norm_xl  = wb.add_format({'border': 1, 'font_size': 10})
                            for ci, col_name in enumerate(report_cols):
                                ws.write(0, ci, col_name, hdr_xl)
                                ws.set_column(ci, ci, 15)
                                for ri in range(1, len(report_df) + 1):
                                    val = str(report_with_total.iloc[ri - 1][col_name])
                                    if   col_name in ['Pick Quantity', 'Destination Country', 'Order NO']: ws.write(ri, ci, val, pick_xl)
                                    elif col_name == 'Allocated': ws.write(ri, ci, val, alloc_xl)
                                    elif col_name in damage_remarks: ws.write(ri, ci, val, dmg_xl)
                                    elif col_name == 'ATS': ws.write(ri, ci, val, ats_xl)
                                    elif col_name in ['Vendor Name', 'COO']: ws.write(ri, ci, val, vnd_xl)
                                    else: ws.write(ri, ci, val, norm_xl)
                            tot_idx = len(report_df) + 1
                            tot_num = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#FFD700', 'border': 1, 'font_size': 10, 'num_format': '#,##0'})
                            tot_str = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#FFD700', 'border': 1, 'font_size': 10})
                            for ci, col_name in enumerate(report_cols):
                                v = report_with_total.iloc[-1][col_name]
                                try:    ws.write(tot_idx, ci, float(v) if v != '' else '', tot_num)
                                except: ws.write(tot_idx, ci, str(v), tot_str)
                            ws.freeze_panes(1, 0)

                            # ── Summary sheet ──
                            ws_s = wb.add_worksheet('Summary')
                            b_xl = wb.add_format({'bold': True, 'font_size': 11})
                            v_xl = wb.add_format({'font_size': 11, 'num_format': '#,##0'})
                            ok_xl = wb.add_format({'bold': True, 'font_color': '#27ae60', 'font_size': 11})
                            er_xl = wb.add_format({'bold': True, 'font_color': '#e74c3c', 'font_size': 11})
                            ws_s.set_column(0, 0, 28); ws_s.set_column(1, 1, 18)
                            summ = [
                                ('Inventory Total Actual Qty', int(inv_total_qty)),
                                ('Report Total Actual Qty', int(rpt_total_qty)),
                                ('Qty Match', 'YES' if qty_match else 'NO'), ('', ''),
                                ('Pick Quantity', int(total_pick)),
                                ('Allocated Qty', int(total_alloc)),
                                ('ATS Quantity', int(total_ats)),
                                ('Damage Quantity', int(total_dmg)),
                                ('Total Accounted', int(accounted)),
                                ('Unaccounted', int(inv_total_qty - accounted)), ('', ''),
                                ('Total Report Lines', len(report_df)),
                                ('Status = D Lines', d_count),
                                ('ATS Lines', len(report_df) - d_count),
                                ('Input/Output Diff Pallets', len(diff_df)),
                            ]
                            for ri, (lab, val) in enumerate(summ):
                                ws_s.write(ri, 0, lab, b_xl)
                                if isinstance(val, int): ws_s.write(ri, 1, val, v_xl)
                                elif val == 'YES': ws_s.write(ri, 1, val, ok_xl)
                                elif val == 'NO':  ws_s.write(ri, 1, val, er_xl)
                                else: ws_s.write(ri, 1, val)

                            # ── Differences sheet ──
                            ws_d = wb.add_worksheet('Differences')
                            d_hdr = wb.add_format({'bold': True, 'bg_color': '#e74c3c', 'font_color': '#fff', 'border': 1, 'font_size': 10})
                            d_ok  = wb.add_format({'bold': True, 'bg_color': '#27ae60', 'font_color': '#fff', 'font_size': 11})
                            d_row = wb.add_format({'border': 1, 'font_size': 10})
                            d_bad = wb.add_format({'border': 1, 'font_size': 10, 'font_color': '#e74c3c', 'bold': True})
                            if not diff_df.empty:
                                for ci, col in enumerate(diff_df.columns):
                                    ws_d.write(0, ci, col, d_hdr); ws_d.set_column(ci, ci, 20)
                                for ri2, row2 in diff_df.iterrows():
                                    for ci2, val2 in enumerate(row2):
                                        ws_d.write(ri2 + 1, ci2, val2,
                                                   d_bad if diff_df.columns[ci2] == 'Difference' else d_row)
                            else:
                                ws_d.write(0, 0, '✅ No Input/Output Differences', d_ok)
                                ws_d.set_column(0, 0, 40)

                        st.download_button("⬇️ Download Formatted Pick Report", data=out_fmt.getvalue(),
                            file_name=f"Pick_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel", use_container_width=True)
                        show_confetti()


    # ==========================================================================
    # TAB 5: DAMAGE ITEMS
    # ==========================================================================
    elif choice == "🩹 Damage Items":
        st.title("🩹 Damage Items Management")
        st.info("Damage items Pallet/Actual Qty/Remark සහිතව upload කරන්න. මෙම Pallets pick operations වලින් automatically exclude වේ.")

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
                            st.error("File must have 'Pallet' and 'Actual Qty' columns.")
                        else:
                            rows_to_add = []
                            for _, row in dmg_preview.iterrows():
                                rows_to_add.append({
                                    'Pallet':     str(row.get(pallet_col, '')),
                                    'Actual Qty': str(row.get(qty_col, '')),
                                    'Remark':     str(row.get(remark_col, '')) if remark_col else 'Damage',
                                    'Date Added': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    'Added By':   current_user,
                                })
                            DBManager.insert_rows("damage_items", rows_to_add)
                            st.success(f"✅ {len(rows_to_add)} Damage Items saved! These pallets will be excluded from picks.")
                            show_confetti()

        with dmg_tab2:
            st.subheader("Damage Items Records")
            dmg_df = DBManager.read_table("damage_items")
            if dmg_df.empty:
                st.info("Damage Items records නොමැත.")
            else:
                st.metric("Total Damage Records", len(dmg_df))
                st.dataframe(dmg_df.astype(str), use_container_width=True)
                out_dmg = io.BytesIO()
                with pd.ExcelWriter(out_dmg, engine='xlsxwriter') as writer:
                    dmg_df.to_excel(writer, sheet_name='Damage_Items', index=False)
                st.download_button("⬇️ Download Damage Records", data=out_dmg.getvalue(),
                    file_name=f"Damage_Items_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.ms-excel")
                st.divider()
                st.subheader("🗑️ Remove Damage Record")
                if 'Pallet' in dmg_df.columns:
                    remove_pallet = st.selectbox("Select Pallet to Remove:", dmg_df['Pallet'].dropna().unique())
                    if st.button("Remove Damage Record"):
                        DBManager.delete_where_eq("damage_items", "Pallet", remove_pallet)
                        st.success(f"✅ Pallet **{remove_pallet}** removed from Damage list.")
                        st.rerun()

    # ==========================================================================
    # TAB 6: VENDOR MAINTAIN
    # ==========================================================================
    elif choice == "🏷️ Vendor Maintain":
        st.title("🏷️ Vendor Maintain")
        st.info("Vendor Name සහ ඒ vendor ට අදාළ Country මෙහි register කරන්න. Reports වල Vendor Country column automatically populate වේ.")

        vnd_tab1, vnd_tab2 = st.tabs(["➕ Add / Update Vendor", "📋 View All Vendors"])

        with vnd_tab1:
            st.subheader("Add New Vendor")
            with st.form("add_vendor_form"):
                new_vnd_name    = st.text_input("Vendor Name")
                new_vnd_country = st.text_input("Country")
                vnd_submitted   = st.form_submit_button("💾 Save Vendor", type="primary")
                if vnd_submitted:
                    if not new_vnd_name.strip():
                        st.error("Vendor Name is required.")
                    elif not new_vnd_country.strip():
                        st.error("Country is required.")
                    else:
                        existing_vnd = DBManager.read_table("vendor_maintain")
                        if not existing_vnd.empty and 'Vendor Name' in existing_vnd.columns:
                            dup = existing_vnd[existing_vnd['Vendor Name'].astype(str).str.strip().str.lower() == new_vnd_name.strip().lower()]
                            if not dup.empty:
                                # Update existing
                                DBManager._overwrite_table(
                                    "vendor_maintain",
                                    existing_vnd.assign(**{
                                        'Country': existing_vnd.apply(
                                            lambda r: new_vnd_country.strip() if str(r['Vendor Name']).strip().lower() == new_vnd_name.strip().lower() else r['Country'],
                                            axis=1
                                        )
                                    })
                                )
                                st.success(f"✅ Vendor **{new_vnd_name}** updated → Country: **{new_vnd_country}**")
                            else:
                                DBManager.insert_rows("vendor_maintain", [{'Vendor Name': new_vnd_name.strip(), 'Country': new_vnd_country.strip()}])
                                st.success(f"✅ Vendor **{new_vnd_name}** added!")
                        else:
                            DBManager.insert_rows("vendor_maintain", [{'Vendor Name': new_vnd_name.strip(), 'Country': new_vnd_country.strip()}])
                            st.success(f"✅ Vendor **{new_vnd_name}** added!")
                        DBManager.invalidate("vendor_maintain")
                        show_confetti()

            st.divider()
            st.subheader("📤 Bulk Upload Vendors (CSV/Excel)")
            st.caption("File columns: **Vendor Name**, **Country**")
            vnd_file = st.file_uploader("Upload Vendor List", type=['csv', 'xlsx'], key="vnd_bulk_uploader")
            if vnd_file:
                vnd_upload_df = pd.read_csv(vnd_file) if vnd_file.name.endswith('.csv') else pd.read_excel(vnd_file)
                vnd_upload_df.columns = [str(c).strip() for c in vnd_upload_df.columns]
                st.dataframe(vnd_upload_df.astype(str), use_container_width=True)
                vnd_col_l = {c.lower(): c for c in vnd_upload_df.columns}
                vn_col = vnd_col_l.get('vendor name')
                cn_col_v = vnd_col_l.get('country')
                if not vn_col or not cn_col_v:
                    st.error("❌ File must have 'Vendor Name' and 'Country' columns.")
                else:
                    if st.button("💾 Bulk Save Vendors", type="primary"):
                        rows_v = [
                            {'Vendor Name': str(r[vn_col]).strip(), 'Country': str(r[cn_col_v]).strip()}
                            for _, r in vnd_upload_df.iterrows()
                            if str(r.get(vn_col, '')).strip()
                        ]
                        # Overwrite with merge logic: keep existing + add new, update existing
                        existing_vnd2 = DBManager.read_table("vendor_maintain")
                        if not existing_vnd2.empty and 'Vendor Name' in existing_vnd2.columns:
                            existing_map = {str(v).strip().lower(): i for i, v in enumerate(existing_vnd2['Vendor Name'])}
                            for rv in rows_v:
                                k = rv['Vendor Name'].lower()
                                if k in existing_map:
                                    existing_vnd2.at[existing_map[k], 'Country'] = rv['Country']
                                else:
                                    existing_vnd2 = pd.concat([existing_vnd2, pd.DataFrame([rv])], ignore_index=True)
                            DBManager._overwrite_table("vendor_maintain", existing_vnd2)
                        else:
                            DBManager.insert_rows("vendor_maintain", rows_v)
                        DBManager.invalidate("vendor_maintain")
                        st.success(f"✅ {len(rows_v)} vendors saved!")
                        show_confetti()

        with vnd_tab2:
            st.subheader("Vendor List")
            vnd_df = DBManager.read_table("vendor_maintain")
            if vnd_df.empty:
                st.info("Vendor records නොමැත. 'Add / Update Vendor' tab හි vendors add කරන්න.")
            else:
                st.metric("Total Vendors", len(vnd_df))
                st.dataframe(vnd_df.astype(str), use_container_width=True)
                out_vnd = io.BytesIO()
                with pd.ExcelWriter(out_vnd, engine='xlsxwriter') as writer:
                    vnd_df.to_excel(writer, sheet_name='Vendor_Maintain', index=False)
                st.download_button("⬇️ Download Vendor List", data=out_vnd.getvalue(),
                    file_name=f"Vendor_List_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.ms-excel")

                st.divider()
                st.subheader("🗑️ Remove Vendor")
                if 'Vendor Name' in vnd_df.columns:
                    rem_vendor = st.selectbox("Select Vendor to Remove:", vnd_df['Vendor Name'].dropna().unique())
                    if st.button("Remove Vendor", type="primary"):
                        DBManager.delete_where_eq("vendor_maintain", "Vendor Name", rem_vendor)
                        DBManager.invalidate("vendor_maintain")
                        st.success(f"✅ Vendor **{rem_vendor}** removed.")
                        st.rerun()

    # ==========================================================================
    # TAB 7: ADMIN SETTINGS
    # ==========================================================================
    elif choice == "🔄 Revert/Delete Picks":
        st.title("🔄 Revert / Delete Picked Data")
        del_tab1, del_tab2, del_tab3, del_tab4 = st.tabs([
            "📁 Upload File to Delete", "🆔 Delete by Load ID Only",
            "🗂️ Delete by Batch ID",   "📦 Delete by Pallet"
        ])

        # ── Helper: archive deleted rows to old_history ────────────────────────
        def _archive_to_old_history(deleted_pick_df, deleted_partial_df, reason="Deleted"):
            """deleted rows → old_history table に insert"""
            OLD_HISTORY_COL_MAP = {
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
                'Item Id': 'item_id', 'Invoice Number 1': 'invoice_number1',
                'Transaction': 'transaction', 'Order Type': 'order_type',
                'Order Number': 'order_number', 'Store Order Number': 'store_order_number',
                'Customer Po Number': 'customer_po_number', 'Partial Order Flag': 'partial_order_flag',
                'Order Date': 'order_date', 'Load Id': 'load_id', 'Asn Number': 'asn_number',
                'Po Number': 'po_number', 'Supplier Hu': 'supplier_hu',
                'New Item Number': 'new_item_number', 'Asn Line Number': 'asn_line_number',
                'Received Gross Weight': 'received_gross_weight',
                'Current Gross Weight': 'current_gross_weight',
                'Received Net Weight': 'received_net_weight',
                'Current Net Weight': 'current_net_weight', 'Supplier Desc': 'supplier_desc',
                'Cbm': 'cbm', 'Container Type': 'container_type',
                'Display Item Number': 'display_item_number', 'Old Item Number': 'old_item_number',
                'Inventory Type': 'inventory_type', 'Type Qc': 'type_qc',
                'Vendor Name': 'vendor_name', 'Manufacture Date': 'manufacture_date',
                'Suom': 'suom', 'S Qty': 's_qty', 'Pick Id': 'pick_id',
                'Downloaded Date': 'downloaded_date', 'Batch ID': 'batch_id',
                'SO Number': 'so_number', 'Generated Load ID': 'generated_load_id',
                'Country Name': 'country_name', 'Pick Quantity': 'pick_quantity',
                'Remark': 'remark',
                # partial-specific
                'Partial Qty': 'partial_qty', 'Gen Pallet ID': 'gen_pallet_id',
                'Balance Qty': 'balance_qty',
            }
            try:
                sb = get_supabase_client()
                rows_to_archive = []
                deleted_at_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                def _df_to_archive_rows(df, source_table):
                    rows = []
                    for _, r in df.iterrows():
                        db_row = {'source_table': source_table, 'deleted_at': deleted_at_str,
                                  'deleted_by': current_user, 'delete_reason': reason}
                        for app_col, db_col in OLD_HISTORY_COL_MAP.items():
                            val = r.get(app_col, None)
                            if val is not None and str(val).strip() not in ('nan', 'None', ''):
                                db_row[db_col] = val
                            else:
                                db_row[db_col] = None
                        rows.append(db_row)
                    return rows

                if deleted_pick_df is not None and not deleted_pick_df.empty:
                    rows_to_archive.extend(_df_to_archive_rows(deleted_pick_df, 'master_pick_data'))
                if deleted_partial_df is not None and not deleted_partial_df.empty:
                    rows_to_archive.extend(_df_to_archive_rows(deleted_partial_df, 'master_partial_data'))

                if rows_to_archive:
                    chunk_size = 500
                    for i in range(0, len(rows_to_archive), chunk_size):
                        sb.table('old_history').insert(rows_to_archive[i:i+chunk_size]).execute()
                    DBManager.invalidate('old_history')
                return len(rows_to_archive)
            except Exception as _arch_err:
                st.warning(f"⚠️ Archive to old_history failed: {_arch_err}")
                return 0

        def _cancel_load_history_picks(deleted_pick_df, deleted_partial_df):
            """Per Logic.txt Revert/Delete: after archiving to old_history, find matching
            load_history rows by Generated Load ID and set pick_status = 'Cancelled'."""
            try:
                # Collect all Generated Load IDs from deleted rows
                cancelled_load_ids = set()
                if deleted_pick_df is not None and not deleted_pick_df.empty:
                    for col in ['Generated Load ID', 'Load Id', 'load_id']:
                        if col in deleted_pick_df.columns:
                            cancelled_load_ids.update(
                                deleted_pick_df[col].dropna().astype(str).str.strip().tolist()
                            )
                            break
                if deleted_partial_df is not None and not deleted_partial_df.empty:
                    for col in ['Load ID', 'load_id']:
                        if col in deleted_partial_df.columns:
                            cancelled_load_ids.update(
                                deleted_partial_df[col].dropna().astype(str).str.strip().tolist()
                            )
                            break
                cancelled_load_ids.discard('')
                cancelled_load_ids.discard('nan')
                cancelled_load_ids.discard('None')

                if not cancelled_load_ids:
                    return 0

                sb_c = get_supabase_client()
                _cancelled_count = 0
                for _lid_c in cancelled_load_ids:
                    try:
                        _res_c = sb_c.table('load_history')\
                            .update({'pick_status': 'Cancelled'})\
                            .eq('generated_load_id', _lid_c)\
                            .execute()
                        _cancelled_count += len(_res_c.data) if _res_c.data else 0
                    except Exception:
                        pass
                if _cancelled_count > 0:
                    DBManager.invalidate('load_history')
                    st.info(f"🚫 load_history: **{_cancelled_count}** entries marked as **Cancelled** (Load IDs: {', '.join(list(cancelled_load_ids)[:3])}{'...' if len(cancelled_load_ids) > 3 else ''})")
                return _cancelled_count
            except Exception as _cl_err:
                st.warning(f"⚠️ Cancel load_history status failed: {_cl_err}")
                return 0

        with del_tab1:
            st.info("Load ID, Pallet සහ Actual Qty අඩංගු Excel/CSV file upload කිරීමෙන් Master_Pick_Data **සහ Master_Partial_Data** හි matching records delete කර **old_history** table update වේ.")
            del_file = st.file_uploader("Upload Data to Delete", type=['csv', 'xlsx'], key="del_file_uploader")
            if del_file:
                if st.button("🗑️ Delete Matching Records", type="primary"):
                    with st.spinner("Deleting..."):
                        del_df = pd.read_csv(del_file) if del_file.name.endswith('.csv') else pd.read_excel(del_file)
                        del_df.columns = del_df.columns.str.strip().str.upper()
                        if not all(col in del_df.columns for col in ['LOAD ID', 'PALLET', 'ACTUAL QTY']):
                            st.error("File must contain 'Load ID', 'Pallet', and 'Actual Qty' columns.")
                            st.stop()

                        master_pick_df = DBManager.read_table("master_pick_data")
                        deleted_pick_rows = pd.DataFrame()
                        if not master_pick_df.empty:
                            initial_len = len(master_pick_df)
                            temp_master = master_pick_df.copy()
                            temp_master.columns = temp_master.columns.str.strip().str.upper()
                            gen_lid_col = 'GENERATED LOAD ID' if 'GENERATED LOAD ID' in temp_master.columns else 'LOAD ID'
                            temp_master['MATCH_KEY'] = (
                                temp_master[gen_lid_col].astype(str).str.strip() + "_" +
                                temp_master['PALLET'].astype(str).str.strip() + "_" +
                                pd.to_numeric(temp_master['ACTUAL QTY'], errors='coerce').fillna(0).astype(str)
                            )
                            del_df['MATCH_KEY'] = (
                                del_df['LOAD ID'].astype(str).str.strip() + "_" +
                                del_df['PALLET'].astype(str).str.strip() + "_" +
                                pd.to_numeric(del_df['ACTUAL QTY'], errors='coerce').fillna(0).astype(str)
                            )
                            keys_to_delete    = del_df['MATCH_KEY'].tolist()
                            mask_del          = temp_master['MATCH_KEY'].isin(keys_to_delete)
                            deleted_pick_rows = master_pick_df[mask_del]
                            filtered_master   = master_pick_df[~mask_del]
                            deleted_count     = initial_len - len(filtered_master)
                            if deleted_count > 0:
                                DBManager._overwrite_table("master_pick_data", filtered_master)
                                st.success(f"✅ {deleted_count} records deleted from Master_Pick_Data!")
                            else:
                                st.warning("⚠️ Matching records not found in Master_Pick_Data.")

                        # ── Also delete matching pallets from master_partial_data ──
                        del_pallets = set(del_df['PALLET'].astype(str).str.strip().tolist())
                        del_load_ids_file = set(del_df['LOAD ID'].astype(str).str.strip().tolist())
                        mpart_df = DBManager.read_table("master_partial_data")
                        deleted_partial_rows = pd.DataFrame()
                        if not mpart_df.empty and 'Pallet' in mpart_df.columns:
                            mpart_mask = mpart_df['Pallet'].astype(str).str.strip().isin(del_pallets)
                            if 'Load ID' in mpart_df.columns:
                                mpart_mask &= mpart_df['Load ID'].astype(str).str.strip().isin(del_load_ids_file)
                            deleted_partial_rows = mpart_df[mpart_mask]
                            if not deleted_partial_rows.empty:
                                filtered_mpart = mpart_df[~mpart_mask]
                                DBManager._overwrite_table("master_partial_data", filtered_mpart)
                                st.success(f"✅ {len(deleted_partial_rows)} records deleted from Master_Partial_Data!")

                        archived = _archive_to_old_history(deleted_pick_rows, deleted_partial_rows, reason="File Upload Delete")
                        if archived > 0:
                            st.info(f"📁 {archived} rows archived to old_history.")
                        _cancel_load_history_picks(deleted_pick_rows, deleted_partial_rows)
                        show_confetti()

        with del_tab2:
            st.info("Load ID(s) delete කිරීමෙන් Master_Pick_Data **සහ Master_Partial_Data** records remove කර **old_history** update වේ. Load_History නොවෙනස්ව.")
            lid_method = st.radio("Delete Method:", ["⌨️ Type Load ID", "📂 Upload Load ID List (Excel/CSV)"], horizontal=True, key="lid_del_method")
            if lid_method == "⌨️ Type Load ID":
                del_load_id = st.text_input("🆔 Enter Load ID to Delete:")
                load_ids_to_delete = [del_load_id.strip()] if del_load_id.strip() else []
            else:
                lid_file = st.file_uploader("📂 Upload Load ID List", type=['csv', 'xlsx'], key="lid_file_uploader")
                load_ids_to_delete = []
                if lid_file:
                    lid_df  = pd.read_csv(lid_file) if lid_file.name.endswith('.csv') else pd.read_excel(lid_file)
                    lid_col = next((c for c in lid_df.columns if str(c).strip().lower() == 'load id'), None)
                    if lid_col:
                        load_ids_to_delete = [str(v).strip() for v in lid_df[lid_col].dropna().unique() if str(v).strip()]
                        st.success(f"✅ {len(load_ids_to_delete)} Load IDs found")
                        st.dataframe(pd.DataFrame(load_ids_to_delete, columns=['Load ID']), use_container_width=True, height=200)
                    else:
                        st.error("❌ 'Load Id' column not found.")

            if load_ids_to_delete:
                master_pick_df = DBManager.read_table("master_pick_data")
                lid_col_mpd    = next((c for c in (master_pick_df.columns if not master_pick_df.empty else []) if str(c).strip().lower() in ('load id', 'generated load id')), 'Load Id')
                if not master_pick_df.empty and lid_col_mpd in master_pick_df.columns:
                    preview = master_pick_df[master_pick_df[lid_col_mpd].astype(str).str.strip().isin(load_ids_to_delete)]
                    if not preview.empty:
                        st.warning(f"⚠️ {len(preview)} records will be deleted from Master_Pick_Data.")
                        show_cols = [c for c in ['Generated Load ID', 'Pallet', 'Actual Qty'] if c in preview.columns]
                        st.dataframe(preview[show_cols].astype(str), use_container_width=True, height=200)

                if st.button("🗑️ Delete by Load ID", type="primary", key="del_lid_btn"):
                    with st.spinner("Deleting..."):
                        master_pick_df = DBManager.read_table("master_pick_data", force=True)
                        lid_col_del    = next((c for c in (master_pick_df.columns if not master_pick_df.empty else []) if str(c).strip().lower() == 'generated load id'), 'Load Id')
                        deleted_pick_rows2 = pd.DataFrame()
                        deleted_pick2      = 0
                        if not master_pick_df.empty and lid_col_del in master_pick_df.columns:
                            mask_lid2          = master_pick_df[lid_col_del].astype(str).str.strip().isin(load_ids_to_delete)
                            deleted_pick_rows2 = master_pick_df[mask_lid2]
                            filtered           = master_pick_df[~mask_lid2]
                            deleted_pick2      = len(master_pick_df) - len(filtered)
                            DBManager._overwrite_table("master_pick_data", filtered)

                        # ── Also delete from master_partial_data by Load ID ──
                        mpart_df2 = DBManager.read_table("master_partial_data", force=True)
                        deleted_partial_rows2 = pd.DataFrame()
                        if not mpart_df2.empty and 'Load ID' in mpart_df2.columns:
                            mpart_mask2           = mpart_df2['Load ID'].astype(str).str.strip().isin(load_ids_to_delete)
                            deleted_partial_rows2 = mpart_df2[mpart_mask2]
                            if not deleted_partial_rows2.empty:
                                DBManager._overwrite_table("master_partial_data", mpart_df2[~mpart_mask2])
                                st.success(f"✅ {len(deleted_partial_rows2)} records deleted from Master_Partial_Data!")

                        ids_str = ', '.join(load_ids_to_delete[:3]) + ('...' if len(load_ids_to_delete) > 3 else '')
                        st.success(f"✅ Load ID(s) [{ids_str}] — {deleted_pick2} records deleted from Master_Pick_Data!")
                        archived2 = _archive_to_old_history(deleted_pick_rows2, deleted_partial_rows2, reason=f"Delete by Load ID: {ids_str}")
                        if archived2 > 0:
                            st.info(f"📁 {archived2} rows archived to old_history.")
                        _cancel_load_history_picks(deleted_pick_rows2, deleted_partial_rows2)
                        show_confetti()

        with del_tab3:
            st.info("Batch ID delete කිරීමෙන් ඒ batch හි Master_Pick_Data **සහ Master_Partial_Data** records remove කර **old_history** update වේ.")
            mpd_for_batch = DBManager.read_table("master_pick_data")
            batch_col_mpd = next((c for c in (mpd_for_batch.columns if not mpd_for_batch.empty else []) if str(c).strip().lower() == 'batch id'), None)
            if not mpd_for_batch.empty and batch_col_mpd:
                available_batches_mpd = [b for b in mpd_for_batch[batch_col_mpd].dropna().unique().tolist() if str(b).strip()]
                if available_batches_mpd:
                    del_batch_id = st.selectbox("🗂️ Select Batch ID:", available_batches_mpd, key="del_batch_sel")
                    if del_batch_id:
                        preview_batch = mpd_for_batch[mpd_for_batch[batch_col_mpd].astype(str).str.strip() == str(del_batch_id).strip()]
                        if not preview_batch.empty:
                            st.warning(f"⚠️ Batch **{del_batch_id}** හි **{len(preview_batch)}** records will be deleted.")
                            bc1, bc2, bc3 = st.columns(3)
                            bc1.metric("Records", len(preview_batch))
                            load_id_col_b = next((c for c in preview_batch.columns if str(c).strip().lower() == 'generated load id'), None)
                            if load_id_col_b: bc2.metric("Load IDs", preview_batch[load_id_col_b].nunique())
                            aq_col_b = next((c for c in preview_batch.columns if str(c).strip().lower() == 'actual qty'), None)
                            if aq_col_b: bc3.metric("Total Qty", int(pd.to_numeric(preview_batch[aq_col_b], errors='coerce').sum()))
                    if st.button("🗑️ Delete by Batch ID", type="primary", key="del_batch_btn"):
                        with st.spinner("Deleting..."):
                            mpd_latest    = DBManager.read_table("master_pick_data", force=True)
                            batch_col_lat = next((c for c in (mpd_latest.columns if not mpd_latest.empty else []) if str(c).strip().lower() == 'batch id'), None)
                            deleted_batch_pick_rows = pd.DataFrame()
                            deleted_batch = 0
                            if not mpd_latest.empty and batch_col_lat:
                                mask_batch3              = mpd_latest[batch_col_lat].astype(str).str.strip() == str(del_batch_id).strip()
                                deleted_batch_pick_rows  = mpd_latest[mask_batch3]
                                filtered_batch           = mpd_latest[~mask_batch3]
                                deleted_batch            = len(mpd_latest) - len(filtered_batch)
                                DBManager._overwrite_table("master_pick_data", filtered_batch)

                            # ── Also delete from master_partial_data by Batch ID ──
                            mpart_df3 = DBManager.read_table("master_partial_data", force=True)
                            deleted_batch_partial_rows = pd.DataFrame()
                            if not mpart_df3.empty and 'Batch ID' in mpart_df3.columns:
                                mpart_mask3                = mpart_df3['Batch ID'].astype(str).str.strip() == str(del_batch_id).strip()
                                deleted_batch_partial_rows = mpart_df3[mpart_mask3]
                                if not deleted_batch_partial_rows.empty:
                                    DBManager._overwrite_table("master_partial_data", mpart_df3[~mpart_mask3])
                                    st.success(f"✅ {len(deleted_batch_partial_rows)} records deleted from Master_Partial_Data!")

                            st.success(f"✅ Batch **{del_batch_id}** — {deleted_batch} records deleted from Master_Pick_Data!")
                            archived3 = _archive_to_old_history(deleted_batch_pick_rows, deleted_batch_partial_rows, reason=f"Delete by Batch ID: {del_batch_id}")
                            if archived3 > 0:
                                st.info(f"📁 {archived3} rows archived to old_history.")
                            _cancel_load_history_picks(deleted_batch_pick_rows, deleted_batch_partial_rows)
                            show_confetti()
                else:
                    st.info("No Batch IDs found in Master_Pick_Data.")
            else:
                st.info("Master_Pick_Data හි data නොමැත.")

        with del_tab4:
            st.info("Pallet delete කිරීමෙන් Master_Pick_Data, Master_Partial_Data, Damage_Items වලින් records remove කර **old_history** update වේ.")
            _del_batch = DBManager.batch_read(["master_pick_data", "master_partial_data", "damage_items"])
            _mpd_pal   = _del_batch["master_pick_data"]
            _mpart_pal = _del_batch["master_partial_data"]
            _dmg_pal   = _del_batch["damage_items"]
            all_pallets_set = set()
            for _df, _col in [(_mpd_pal, 'Pallet'), (_mpart_pal, 'Pallet'), (_dmg_pal, 'Pallet')]:
                if not _df.empty and _col in _df.columns:
                    all_pallets_set.update(_df[_col].dropna().astype(str).str.strip().tolist())
            if not _mpart_pal.empty and 'Gen Pallet ID' in _mpart_pal.columns:
                all_pallets_set.update(_mpart_pal['Gen Pallet ID'].dropna().astype(str).str.strip().tolist())
            all_pallets_list = sorted([p for p in all_pallets_set if p])

            if all_pallets_list:
                del_pallet = st.selectbox("📦 Select Pallet:", all_pallets_list, key="del_pallet_sel")
                if del_pallet:
                    def _count_rows(df, col, val):
                        if df.empty or col not in df.columns: return 0
                        return len(df[df[col].astype(str).str.strip() == str(val).strip()])
                    mpd_count       = _count_rows(_mpd_pal,   'Pallet', del_pallet)
                    mpart_count     = _count_rows(_mpart_pal, 'Pallet', del_pallet)
                    mpart_gen_count = _count_rows(_mpart_pal, 'Gen Pallet ID', del_pallet) if not _mpart_pal.empty and 'Gen Pallet ID' in _mpart_pal.columns else 0
                    dmg_count       = _count_rows(_dmg_pal,   'Pallet', del_pallet)
                    st.warning(f"⚠️ Pallet **{del_pallet}** records:")
                    pc1, pc2, pc3 = st.columns(3)
                    pc1.metric("Master_Pick_Data",    mpd_count)
                    pc2.metric("Master_Partial_Data", mpart_count + mpart_gen_count)
                    pc3.metric("Damage_Items",        dmg_count)

                    if st.button("🗑️ Delete Pallet from All Tables", type="primary", key="del_pallet_btn"):
                        with st.spinner("Deleting..."):
                            results = []
                            del_pallet_pick_rows    = pd.DataFrame()
                            del_pallet_partial_rows = pd.DataFrame()

                            mpd_fresh = DBManager.read_table("master_pick_data", force=True)
                            if not mpd_fresh.empty and 'Pallet' in mpd_fresh.columns:
                                mask_p4              = mpd_fresh['Pallet'].astype(str).str.strip() == str(del_pallet).strip()
                                del_pallet_pick_rows = mpd_fresh[mask_p4]
                                filtered_mpd         = mpd_fresh[~mask_p4]
                                deleted              = len(mpd_fresh) - len(filtered_mpd)
                                DBManager._overwrite_table("master_pick_data", filtered_mpd)
                                results.append(f"Master_Pick_Data: {deleted} records")

                            mpart_fresh = DBManager.read_table("master_partial_data", force=True)
                            if not mpart_fresh.empty:
                                mask = pd.Series([True] * len(mpart_fresh))
                                if 'Pallet'        in mpart_fresh.columns: mask &= mpart_fresh['Pallet'].astype(str).str.strip()        != str(del_pallet).strip()
                                if 'Gen Pallet ID' in mpart_fresh.columns: mask &= mpart_fresh['Gen Pallet ID'].astype(str).str.strip() != str(del_pallet).strip()
                                del_pallet_partial_rows = mpart_fresh[~mask]
                                filtered_mpart          = mpart_fresh[mask]
                                deleted                 = len(mpart_fresh) - len(filtered_mpart)
                                DBManager._overwrite_table("master_partial_data", filtered_mpart)
                                results.append(f"Master_Partial_Data: {deleted} records")

                            dmg_fresh = DBManager.read_table("damage_items", force=True)
                            if not dmg_fresh.empty and 'Pallet' in dmg_fresh.columns:
                                filtered_dmg = dmg_fresh[dmg_fresh['Pallet'].astype(str).str.strip() != str(del_pallet).strip()]
                                deleted = len(dmg_fresh) - len(filtered_dmg)
                                DBManager._overwrite_table("damage_items", filtered_dmg)
                                results.append(f"Damage_Items: {deleted} records")

                            archived4 = _archive_to_old_history(del_pallet_pick_rows, del_pallet_partial_rows, reason=f"Delete by Pallet: {del_pallet}")
                            if archived4 > 0:
                                results.append(f"old_history: {archived4} archived")
                            _cancel_load_history_picks(del_pallet_pick_rows, del_pallet_partial_rows)
                            st.success(f"✅ Pallet **{del_pallet}** deleted — {' | '.join(results)}")
                            show_confetti()
            else:
                st.info("Delete කළ හැකි Pallets නොමැත.")

    # ==========================================================================
    # TAB 7: ADMIN SETTINGS
    # ==========================================================================
    elif choice == "⚙️ Admin Settings":
        st.title("⚙️ System Administration")

        adm_tab1, adm_tab2, adm_tab3 = st.tabs(["👥 User Management", "🗄️ Database Management", "📜 Old History"])

        with adm_tab1:
            st.subheader("👥 Add New User")
            with st.form("add_user_form"):
                n_user = st.text_input("New Username")
                n_pass = st.text_input("New Password", type="password")
                n_role = st.selectbox("Role", ["user", "SysUser", "admin"])
                submitted = st.form_submit_button("Add User")
                if submitted and n_user and n_pass:
                    users_data = DBManager.read_table("users")
                    if not users_data.empty and n_user in users_data['Username'].values:
                        st.error("This Username already exists.")
                    else:
                        DBManager.insert_rows("users", [{'Username': n_user, 'Password': n_pass, 'Role': n_role}])
                        st.success("User successfully added!")

        with adm_tab2:
            st.subheader("⚠️ Database Management")
            st.warning("This will permanently delete all records from the selected table.")
            sheet_to_clear = st.selectbox("Select Data to Clear:", [
                "master_pick_data", "master_partial_data", "summary_data", "load_history",
                "damage_items", "vendor_maintain", "old_history", "old_history_master", "ALL_DATA"
            ])
            confirm = st.text_input("Type 'CONFIRM' to proceed:")
            if st.button("🗑️ Clear Selected Data", type="primary"):
                if confirm == 'CONFIRM':
                    tables = ["master_pick_data", "master_partial_data", "summary_data", "load_history",
                              "damage_items", "vendor_maintain", "old_history", "old_history_master"] if sheet_to_clear == "ALL_DATA" else [sheet_to_clear]
                    try:
                        sb = get_supabase_client()
                        for t in tables:
                            sb.table(t).delete().neq('id', -1).execute()
                            DBManager.invalidate(t)
                        st.success(f"✅ {sheet_to_clear} successfully cleared.")
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.error("Please type CONFIRM to proceed.")

            st.divider()
            st.subheader("📋 SQL Schema Reference")
            with st.expander("📄 View SQL Schemas for New Tables", expanded=False):
                st.markdown("#### `old_history` table — deleted pick & partial records archive")
                st.code("""
-- old_history: deleted records from master_pick_data & master_partial_data
CREATE TABLE old_history (
    id                    BIGSERIAL PRIMARY KEY,
    source_table          TEXT,           -- 'master_pick_data' or 'master_partial_data'
    deleted_at            TIMESTAMPTZ DEFAULT NOW(),
    deleted_by            TEXT,
    delete_reason         TEXT,
    -- master_pick_data / inventory fields
    wh_id                 TEXT,
    client_code           TEXT,
    pallet                TEXT,
    invoice_number        TEXT,
    location_id           TEXT,
    item_number           TEXT,
    description           TEXT,
    lot_number            TEXT,
    actual_qty            NUMERIC,
    unavailable_qty       NUMERIC,
    uom                   TEXT,
    status                TEXT,
    mlp                   TEXT,
    stored_attribute_id   TEXT,
    fifo_date             TEXT,
    expiration_date       TEXT,
    grn_number            TEXT,
    gate_pass_id          TEXT,
    cust_dec_no           TEXT,
    color                 TEXT,
    size                  TEXT,
    style                 TEXT,
    supplier              TEXT,
    plant                 TEXT,
    client_so             TEXT,
    client_so_line        TEXT,
    po_cust_dec           TEXT,
    customer_ref_number   TEXT,
    item_id               TEXT,
    invoice_number1       TEXT,
    transaction           TEXT,
    order_type            TEXT,
    order_number          TEXT,
    store_order_number    TEXT,
    customer_po_number    TEXT,
    partial_order_flag    TEXT,
    order_date            TEXT,
    load_id               TEXT,
    asn_number            TEXT,
    po_number             TEXT,
    supplier_hu           TEXT,
    new_item_number       TEXT,
    asn_line_number       TEXT,
    received_gross_weight NUMERIC,
    current_gross_weight  NUMERIC,
    received_net_weight   NUMERIC,
    current_net_weight    NUMERIC,
    supplier_desc         TEXT,
    cbm                   NUMERIC,
    container_type        TEXT,
    display_item_number   TEXT,
    old_item_number       TEXT,
    inventory_type        TEXT,
    type_qc               TEXT,
    vendor_name           TEXT,
    manufacture_date      TEXT,
    suom                  TEXT,
    s_qty                 NUMERIC,
    pick_id               TEXT,
    downloaded_date       TEXT,
    batch_id              TEXT,
    so_number             TEXT,
    generated_load_id     TEXT,
    country_name          TEXT,
    pick_quantity         NUMERIC,
    remark                TEXT,
    -- master_partial_data specific fields
    partial_qty           NUMERIC,
    gen_pallet_id         TEXT,
    balance_qty           NUMERIC,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_old_history_pallet     ON old_history(pallet);
CREATE INDEX idx_old_history_load_id    ON old_history(generated_load_id);
CREATE INDEX idx_old_history_batch_id   ON old_history(batch_id);
CREATE INDEX idx_old_history_gen_pallet ON old_history(gen_pallet_id);
CREATE INDEX idx_old_history_source     ON old_history(source_table);
""", language="sql")

                st.markdown("#### `old_history_master` table — pallet master reference data")
                st.code("""
-- old_history_master: pallet-level reference for blank-fill in Formatted Pick Report
CREATE TABLE old_history_master (
    id              BIGSERIAL PRIMARY KEY,
    pallet          TEXT NOT NULL,
    vendor_name     TEXT,
    invoice_number  TEXT,
    fifo_date       TEXT,
    grn_number      TEXT,
    client_so       TEXT,
    supplier_hu     TEXT,
    supplier        TEXT,
    lot_number      TEXT,
    style           TEXT,
    color           TEXT,
    size            TEXT,
    client_so_2     TEXT,
    uploaded_by     TEXT,
    uploaded_at     TIMESTAMPTZ DEFAULT NOW(),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_old_history_master_pallet ON old_history_master(pallet);
""", language="sql")

        with adm_tab3:
            st.subheader("📜 Old History — Pallet Master Reference")
            st.info("Pallet reference data upload කරන්න. Formatted Pick Report generate වෙද්දී blank/nan fields සඳහා මෙම data use කරේ.")

            oh_sub1, oh_sub2 = st.tabs(["📤 Upload Master Reference", "📋 View / Search Records"])

            with oh_sub1:
                st.caption("Excel file columns: **Pallet**, Vendor Name, Invoice Number, Fifo Date, Grn Number, Client So, Supplier Hu, Supplier, Lot Number, Style, Color, Size, Client So 2")
                oh_file = st.file_uploader("Upload Old History Master Excel", type=['csv', 'xlsx'], key="oh_uploader")
                if oh_file:
                    oh_preview = pd.read_csv(oh_file) if oh_file.name.endswith('.csv') else pd.read_excel(oh_file)
                    oh_preview.columns = [str(c).strip() for c in oh_preview.columns]
                    st.dataframe(oh_preview.head(20).astype(str), use_container_width=True)
                    st.caption(f"Total rows: {len(oh_preview)}")

                    oh_col_l = {c.lower(): c for c in oh_preview.columns}
                    pallet_oh_col = oh_col_l.get('pallet')
                    if not pallet_oh_col:
                        st.error("❌ 'Pallet' column not found in uploaded file.")
                    else:
                        if st.button("💾 Save to old_history_master", type="primary", key="oh_save_btn"):
                            with st.spinner("Saving..."):
                                OH_FIELD_MAP = {
                                    'pallet': 'pallet', 'vendor name': 'vendor_name',
                                    'invoice number': 'invoice_number', 'fifo date': 'fifo_date',
                                    'grn number': 'grn_number', 'client so': 'client_so',
                                    'supplier hu': 'supplier_hu', 'supplier': 'supplier',
                                    'lot number': 'lot_number', 'style': 'style',
                                    'color': 'color', 'size': 'size', 'client so 2': 'client_so_2',
                                }
                                rows_oh = []
                                for _, r in oh_preview.iterrows():
                                    db_row = {'uploaded_by': current_user}
                                    for app_col, db_col in OH_FIELD_MAP.items():
                                        src_col = oh_col_l.get(app_col)
                                        if src_col:
                                            val = r.get(src_col, None)
                                            db_row[db_col] = None if (val is None or str(val).strip() in ('', 'nan', 'None')) else str(val).strip()
                                    if db_row.get('pallet'):
                                        rows_oh.append(db_row)

                                if rows_oh:
                                    try:
                                        sb = get_supabase_client()
                                        # Upsert by pallet (unique index)
                                        chunk_size = 500
                                        for i in range(0, len(rows_oh), chunk_size):
                                            sb.table('old_history_master').upsert(
                                                rows_oh[i:i+chunk_size], on_conflict='pallet'
                                            ).execute()
                                        DBManager.invalidate('old_history_master')
                                        st.success(f"✅ {len(rows_oh)} records saved to old_history_master!")
                                        show_confetti()
                                    except Exception as _oh_err:
                                        st.error(f"❌ Save error: {_oh_err}")
                                else:
                                    st.warning("No valid rows found (Pallet column empty?).")

            with oh_sub2:
                st.caption("old_history_master table view & search")
                col_oh1, col_oh2 = st.columns([3, 1])
                oh_search = col_oh1.text_input("🔍 Search by Pallet:", key="oh_search")
                if col_oh2.button("🔄 Refresh", key="oh_refresh"):
                    DBManager.invalidate('old_history_master')

                try:
                    oh_view_df = DBManager.read_table("old_history_master")
                    if not oh_view_df.empty:
                        if oh_search.strip():
                            oh_view_df = oh_view_df[
                                oh_view_df.get('Pallet', oh_view_df.get('pallet', pd.Series())).astype(str)
                                .str.contains(oh_search.strip(), case=False, na=False)
                            ]
                        st.metric("Records", len(oh_view_df))
                        st.dataframe(oh_view_df.astype(str), use_container_width=True)
                        out_oh = io.BytesIO()
                        with pd.ExcelWriter(out_oh, engine='xlsxwriter') as writer:
                            oh_view_df.to_excel(writer, sheet_name='Old_History_Master', index=False)
                        st.download_button("⬇️ Download old_history_master", data=out_oh.getvalue(),
                            file_name=f"Old_History_Master_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.ms-excel")
                    else:
                        st.info("old_history_master records නොමැත. Upload tab හි data add කරන්න.")
                except Exception as _oh_view_err:
                    st.warning(f"old_history_master read error: {_oh_view_err}")


footer_branding()
