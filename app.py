import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import io
import time
import re

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

# ── NEW: old_history_master — pallet reference (blank-fill source) ────────────
OLDMASTER_COL_MAP = {
    'Pallet': 'pallet', 'Vendor Name': 'vendor_name', 'Invoice Number': 'invoice_number',
    'Fifo Date': 'fifo_date', 'Grn Number': 'grn_number', 'Client So': 'client_so',
    'Supplier Hu': 'supplier_hu', 'Supplier': 'supplier', 'Lot Number': 'lot_number',
    'Style': 'style', 'Color': 'color', 'Size': 'size', 'Client So 2': 'client_so_2',
    'Uploaded By': 'uploaded_by', 'Uploaded At': 'uploaded_at',
}
OLDMASTER_COL_MAP_REV = {v: k for k, v in OLDMASTER_COL_MAP.items()}

# ── NEW: old_history — deleted pick/partial archive (blank-fill source) ───────
# මෙය කලින් DBManager._COL_MAP එකේ තිබුනේ නෑ — ඒ නිසා read_table("old_history")
# raw snake_case columns return කළා. දැන් app headers ලෙස rename වේ.
OLDHIST_COL_MAP = {
    'Source Table': 'source_table', 'Deleted At': 'deleted_at', 'Deleted By': 'deleted_by',
    'Delete Reason': 'delete_reason',
    'Wh Id': 'wh_id', 'Client Code': 'client_code', 'Pallet': 'pallet',
    'Invoice Number': 'invoice_number', 'Location Id': 'location_id',
    'Item Number': 'item_number', 'Description': 'description', 'Lot Number': 'lot_number',
    'Actual Qty': 'actual_qty', 'Unavailable Qty': 'unavailable_qty', 'Uom': 'uom',
    'Status': 'status', 'Mlp': 'mlp', 'Stored Attribute Id': 'stored_attribute_id',
    'Fifo Date': 'fifo_date', 'Expiration Date': 'expiration_date', 'Grn Number': 'grn_number',
    'Gate Pass Id': 'gate_pass_id', 'Cust Dec No': 'cust_dec_no', 'Color': 'color',
    'Size': 'size', 'Style': 'style', 'Supplier': 'supplier', 'Plant': 'plant',
    'Client So': 'client_so', 'Client So Line': 'client_so_line', 'Po Cust Dec': 'po_cust_dec',
    'Customer Ref Number': 'customer_ref_number', 'Item Id': 'item_id',
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
    'Partial Qty': 'partial_qty', 'Gen Pallet ID': 'gen_pallet_id', 'Balance Qty': 'balance_qty',
}
OLDHIST_COL_MAP_REV = {v: k for k, v in OLDHIST_COL_MAP.items()}


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
    'Remark': 'remark',                       # ── FIX: Remark එක DB එකට save වුනේ නෑ ──
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
    remark              text,
    created_at          timestamptz DEFAULT now()
);

-- දැනටමත් table එක තියෙනවා නම් මේ දෙක පමණක් run කරන්න:
ALTER TABLE inventory_status ADD COLUMN IF NOT EXISTS remark      text;
ALTER TABLE inventory_status ADD COLUMN IF NOT EXISTS location_id text;
"""


# ── DB column → SQL type (missing column එකක් හම්බුනොත් ALTER statement හදන්න) ──
MISSING_COL_TYPES = {
    'actual_qty': 'numeric', 'unavailable_qty': 'numeric', 'pick_quantity': 'numeric',
    'allocated': 'numeric', 'ats': 'numeric', 'qc_repair': 'numeric',
    'balance_qty': 'numeric', 'partial_qty': 'numeric', 's_qty': 'numeric',
    'requested': 'numeric', 'picked': 'numeric', 'variance': 'numeric',
    'cbm': 'numeric', 'asn_line_number': 'numeric',
    'received_gross_weight': 'numeric', 'current_gross_weight': 'numeric',
    'received_net_weight': 'numeric', 'current_net_weight': 'numeric',
    'row_order': 'bigint',
}


# ── Helpers ───────────────────────────────────────────────────────────────────

# 🐛 FIX: pandas version එක අනුව missing value එකක් str() කරද්දී 'nan', 'None',
# 'NaT', '<NA>' යනාදී විවිධ ආකාරයෙන් එනවා. පරණ code එක check කළේ 'nan'/'None'
# විතරයි — ඒ නිසා pandas 2.x/3.x nullable dtypes එක්ක '<NA>' කියන string එක
# report එකේ real value එකක් විදිහට යනවා. දැන් ඔක්කොම blank ලෙස handle වේ.
BLANK_TOKENS = {'', 'nan', 'none', 'nat', '<na>'}


def sv(v) -> str:
    """Safe string value — missing/NA tokens සියල්ල '' බවට පත් කරයි."""
    if v is None:
        return ''
    try:
        if pd.isna(v):
            return ''
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return '' if s.lower() in BLANK_TOKENS else s


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
    # ⚡ SPEED FIX: Streamlit සෑම rerun එකකදීම මුළු script එකම නැවත run වන නිසා
    # class-level dict එකක් cache එකක් විදිහට වැඩ කරන්නේ නැහැ (හැම click එකකදීම
    # reset වී Supabase එකට නැවත call යනවා). ඒ නිසා cache එක st.session_state
    # තුළ තබයි — rerun අතරේ survive වේ. TTL එකත් 30s → 300s (5 min).
    _cache_ttl: int = 300

    _COL_MAP = {
        "master_pick_data":    PICK_COL_MAP,
        "master_partial_data": PARTIAL_COL_MAP,
        "load_history":        HISTORY_COL_MAP,
        "summary_data":        SUMMARY_COL_MAP,
        "damage_items":        DAMAGE_COL_MAP,
        "users":               USERS_COL_MAP,
        "vendor_maintain":     VENDOR_COL_MAP,
        "inventory_status":    INVSTATUS_COL_MAP,
        "old_history":         OLDHIST_COL_MAP,
        "old_history_master":  OLDMASTER_COL_MAP,
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
        "old_history":         OLDHIST_COL_MAP_REV,
        "old_history_master":  OLDMASTER_COL_MAP_REV,
    }

    @classmethod
    def _table_key(cls, sheet_name: str) -> str:
        return sheet_name.lower().replace(" ", "_")

    @classmethod
    def _store(cls) -> dict:
        """Rerun අතරේ survive වන cache dict එක (st.session_state තුළ)."""
        if '_db_cache' not in st.session_state:
            st.session_state['_db_cache'] = {}
        return st.session_state['_db_cache']

    @classmethod
    def invalidate(cls, sheet_name=None):
        store = cls._store()
        if sheet_name:
            base = cls._table_key(sheet_name)
            for k in [k for k in list(store.keys()) if k == base or k.startswith(base + "::")]:
                store.pop(k, None)
        else:
            store.clear()

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
        # 🐛 FIX: කලින් `v in (None, 'nan', 'None', '')` කියලා check කළා. ඒකෙ
        # ප්‍රශ්න දෙකක් තිබුනා —
        #   1. v = pd.NA නම් `in` comparison එක TypeError ("boolean value of NA
        #      is ambiguous") throw කරලා insert එකම fail වුනා.
        #   2. v = float('nan') නම් tuple එකේ නැති නිසා NaN එහෙම්මම payload එකට
        #      ගියා → json.dumps එකෙන් `NaN` token එකක් (invalid JSON) → PostgREST
        #      එකෙන් 400. දැන් සියලු NA/blank tokens None බවට පත් වේ.
        col_map = cls._COL_MAP.get(table_key, {})
        out = {}
        for k, v in row.items():
            db_col = col_map.get(k, k.lower().replace(" ", "_"))
            out[db_col] = None if sv(v) == '' else v
        return out

    @classmethod
    def read_table(cls, table_name: str, force: bool = False, columns: list = None) -> pd.DataFrame:
        """
        columns=None  → මුළු table එකම (කලින් වගේම).
        columns=[...] → අවශ්‍ය columns පමණක් fetch කරයි (⚡ Dashboard speed).
        """
        key = cls._table_key(table_name)
        col_map = cls._COL_MAP.get(key, {})
        db_cols = None
        if columns:
            db_cols = [col_map.get(c, str(c).strip().lower().replace(" ", "_")) for c in columns]
        cache_key = key if not db_cols else f"{key}::cols::{','.join(sorted(db_cols))}"

        store = cls._store()
        now = time.time()
        if not force and cache_key in store:
            ts, df = store[cache_key]
            if (now - ts) < cls._cache_ttl:
                return df.copy()
        try:
            sb = get_supabase_client()
            select_expr = ",".join(db_cols) if db_cols else "*"
            all_records = []
            offset = 0
            page_size = 1000
            # ── 🐛 FIX: ORDER BY නැතුව .range() pagination කරද්දී PostgreSQL row
            #    order එක guarantee කරන්නේ නෑ → page අතර rows duplicate/missing විය හැක,
            #    තවද "අලුත්→පරණ" order එකත් වැරදෙනවා. දැන් id අනුව ascending order කරයි,
            #    ඒ නිසා .iloc[::-1] = නියම newest-first. (id නැති table එකකදී fallback.)
            _use_order = True
            while True:
                try:
                    q = sb.table(key).select(select_expr)
                    if _use_order:
                        q = q.order('id', desc=False)
                    res = q.range(offset, offset + page_size - 1).execute()
                except Exception:
                    if not _use_order:
                        raise
                    _use_order = False          # id column නෑ → order නැතුව retry
                    all_records, offset = [], 0
                    continue
                batch = res.data or []
                all_records.extend(batch)
                if len(batch) < page_size:
                    break
                offset += page_size
            df = cls._db_to_app_df(key, all_records)
            store[cache_key] = (time.time(), df)
            return df.copy()
        except Exception as e:
            st.warning(f"DB read error ({table_name}): {e}")
            if cache_key in store:
                return store[cache_key][1].copy()
            return pd.DataFrame()

    @classmethod
    def count_rows(cls, table_name: str, force: bool = False) -> int:
        """⚡ මුළු table එක download නොකර row count එක පමණක් ගනී."""
        key = cls._table_key(table_name)
        cache_key = f"{key}::count"
        store = cls._store()
        now = time.time()
        if not force and cache_key in store:
            ts, val = store[cache_key]
            if (now - ts) < cls._cache_ttl:
                return val
        try:
            sb = get_supabase_client()
            res = sb.table(key).select("id", count="exact").limit(1).execute()
            cnt = int(res.count or 0)
            store[cache_key] = (time.time(), cnt)
            return cnt
        except Exception:
            return 0

    @classmethod
    def batch_read(cls, table_names: list, force: bool = False) -> dict:
        return {name: cls.read_table(name, force=force) for name in table_names}

    # ══════════════════════════════════════════════════════════════════════
    # 🐛 SCHEMA-SAFE WRITES
    #   Supabase table එකේ column එකක් නැති වුනොත් (ALTER TABLE run කරලා නැති,
    #   හෝ PostgREST schema cache එක refresh වෙලා නැති), කලින් insert එක
    #   මුළුමනින්ම fail වුනා — _overwrite_table එකේදී delete කරලා ඉවරයි නිසා
    #   table එකේ data එක නැති වුනා. දැන්:
    #     1. delete කරන්න කලින් pre-flight column check එකක් (non-destructive)
    #     2. නැති columns auto-drop කරලා warning එකක් + හරියටම ඕන SQL එක පෙන්වයි
    #     3. insert එකේදීත් retry safety net එකක්
    # ══════════════════════════════════════════════════════════════════════
    _MISSING_COL_RES = [
        re.compile(r"Could not find the '([^']+)' column", re.IGNORECASE),
        re.compile(r'column\s+(?:[\w"]+\.)?"?(\w+)"?\s+does not exist', re.IGNORECASE),
    ]

    @classmethod
    def _missing_col(cls, err):
        """Error message එකෙන් නැති column එකේ නම extract කරයි."""
        msg = str(err)
        for pat in cls._MISSING_COL_RES:
            m = pat.search(msg)
            if m:
                return m.group(1)
        return None

    @classmethod
    def _filter_valid_columns(cls, key: str, cols: list):
        """DB එකේ ඇත්තටම තියෙන columns පමණක් return කරයි.
        select ... limit(1) probe එකක් — කිසිම data එකක් වෙනස් වෙන්නේ නෑ.
        Returns (valid_cols, dropped_cols)."""
        cols = [c for c in cols]
        dropped = []
        if not cols:
            return cols, dropped
        try:
            sb = get_supabase_client()
        except Exception:
            return cols, dropped
        for _ in range(len(cols) + 1):
            if not cols:
                break
            try:
                sb.table(key).select(",".join(cols)).limit(1).execute()
                return cols, dropped
            except Exception as e:
                bad = cls._missing_col(e)
                if bad is None or bad not in cols:
                    break          # වෙනත් error එකක් — probe එක skip කරයි
                cols.remove(bad)
                dropped.append(bad)
        return cols, dropped

    @classmethod
    def _insert_chunks(cls, key: str, rows: list, chunk_size: int = 500):
        """Rows insert කරයි. නැති column එකක් හම්බුනොත් ඒක drop කරලා,
        දැනටමත් insert වුනු chunks නැවත නොයවා, ඉතුරු ටික insert කරයි.
        Returns dropped column list."""
        if not rows:
            return []
        # ── සියලු rows එකම key set එකකට normalize (uniform payload) ──
        all_keys, seen = [], set()
        for r in rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)
        work = [{k: r.get(k, None) for k in all_keys} for r in rows]

        sb = get_supabase_client()
        dropped, start = [], 0
        while True:
            try:
                i = start
                while i < len(work):
                    sb.table(key).insert(work[i:i + chunk_size]).execute()
                    i += chunk_size
                    start = i                      # progress save
                return dropped
            except Exception as e:
                bad = cls._missing_col(e)
                if bad is None or bad in dropped:
                    raise
                dropped.append(bad)
                work = [{k: v for k, v in r.items() if k != bad} for r in work]

    @classmethod
    def _warn_missing_cols(cls, table_name: str, dropped: list):
        if not dropped:
            return
        alters = "\n".join(
            f"ALTER TABLE {cls._table_key(table_name)} "
            f"ADD COLUMN IF NOT EXISTS {c} {MISSING_COL_TYPES.get(c, 'text')};"
            for c in dropped
        )
        st.warning(
            f"⚠️ **{table_name}** table එකේ මේ columns නෑ — ඒවා නැතුව save කළා: "
            f"**{', '.join(dropped)}**\n\n"
            "Supabase → SQL Editor එකේ මේක run කරන්න (අන්තිම line එක "
            "schema cache එක refresh කරයි):"
        )
        st.code(alters + "\n\nNOTIFY pgrst, 'reload schema';", language="sql")

    @classmethod
    def insert_rows(cls, table_name: str, rows: list) -> bool:
        if not rows:
            return True
        key = cls._table_key(table_name)
        try:
            db_rows = [cls._app_row_to_db(key, r) for r in rows]
            dropped = cls._insert_chunks(key, db_rows)
            cls._warn_missing_cols(table_name, dropped)
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
                if not cls._overwrite_table(table_name, filtered):
                    return 0          # 🐛 FIX: write එක fail නම් 0 return කරයි
            return deleted
        except Exception as e:
            st.error(f"DB composite delete error: {e}")
            return 0

    @classmethod
    def _overwrite_table(cls, table_name: str, df: pd.DataFrame) -> bool:
        """Clear + insert. 🐛 FIX: කලින් delete එක මුලින්ම කරලා, insert එක fail
        වුනොත් table එකේ data එක සම්පූර්ණයෙන් නැති වුනා. දැන් —
          1. delete කරන්න **කලින්** column pre-flight check එකක්
          2. insert fail වුනොත් පරණ data එක restore කරයි.

        🐛 FIX: කලින් මෙය කිසිම දෙයක් return කළේ නෑ — error එකක් ආවත් outer
        except එකෙන් ගිල දාලා silently return වුනා. ඒ නිසා call කරන තැන්වල
        "✅ save කළා" කියලා message එකක් පෙන්නුවා, ඇත්තටම save වෙලා නැති වුනත්.
        දැන් success/failure එක bool එකකින් return කරයි."""
        key = cls._table_key(table_name)
        try:
            sb = get_supabase_client()

            # ── STEP 1: rows build කරයි (delete කරන්න කලින්) ──
            rows = []
            if not df.empty:
                col_map = cls._COL_MAP.get(key, {})
                for _, row in df.iterrows():
                    db_row = {}
                    for c in df.columns:
                        db_col = col_map.get(c, str(c).lower().replace(" ", "_"))
                        val = row[c]
                        try:
                            _na = pd.isna(val)
                        except (TypeError, ValueError):
                            _na = False
                        if _na or str(val).strip() in ('', 'nan', 'None', 'NaN', 'NaT', '<NA>'):
                            db_row[db_col] = None
                        else:
                            db_row[db_col] = val
                    rows.append(db_row)

            # ── STEP 2: PRE-FLIGHT — DB එකේ නැති columns delete කරන්න කලින්ම හොයයි ──
            dropped = []
            if rows:
                valid, dropped = cls._filter_valid_columns(key, list(rows[0].keys()))
                if dropped:
                    keep = set(valid)
                    rows = [{k: v for k, v in r.items() if k in keep} for r in rows]

            # ── STEP 3: පරණ data එක backup (cache එකෙන් — බොහෝ විට free) ──
            backup_rows = []
            try:
                old_df = cls.read_table(table_name)
                if not old_df.empty:
                    backup_rows = [cls._app_row_to_db(key, r) for r in old_df.to_dict('records')]
            except Exception:
                backup_rows = []

            # ── STEP 4: delete → insert ──
            sb.table(key).delete().neq('id', -1).execute()
            cls.invalidate(table_name)
            try:
                if rows:
                    dropped += cls._insert_chunks(key, rows)
            except Exception as ins_err:
                # ── insert fail → පරණ data එක ආපහු දාන්න try කරයි ──
                restored = 0
                if backup_rows:
                    try:
                        vb, _ = cls._filter_valid_columns(key, list(backup_rows[0].keys()))
                        kb = set(vb)
                        cls._insert_chunks(key, [{k: v for k, v in r.items() if k in kb}
                                                 for r in backup_rows])
                        restored = len(backup_rows)
                    except Exception:
                        restored = 0
                cls.invalidate(table_name)
                if restored:
                    st.error(f"❌ {table_name} insert fail වුනා — පරණ rows **{restored}** "
                             f"restore කළා. Error: {ins_err}")
                else:
                    st.error(f"❌ {table_name} insert fail වුනා: {ins_err}")
                raise

            cls._warn_missing_cols(table_name, sorted(set(dropped)))
            cls.invalidate(table_name)
            return True
        except Exception as e:
            st.error(f"DB overwrite error ({table_name}): {e}")
            return False

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
        """Clear a table then insert the given DataFrame (clear + bulk insert).
        Returns True only if the write really succeeded."""
        try:
            return cls._overwrite_table(table_name, df)
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


# ── 2. App Header (Login REMOVED) ──────────────────────────────────────────────

def app_header():
    """Login අයින් කළා — direct app access. Sidebar branding පමණක් render කරයි."""
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

    # ══════════════════════════════════════════════════════════════════════════
    # Gen Pallet ID generator
    #   RULE 1: උපරිම දිග characters 22 (WMS limit) — වැඩි නම් base එක truncate වේ.
    #   RULE 2: Duplicate බැහැ — master_partial_data + old_history + දැන් upload කළ
    #           inventory එකේ real pallets සියල්ලට එරෙහිව check කරයි.
    # ══════════════════════════════════════════════════════════════════════════
    MAX_GEN_PALLET_LEN = 22

    def _clean_id(v):
        return re.sub(r'\s+', '', str(v).strip())

    existing_gen_pallet_ids = set()

    def _collect_ids(df, *col_names):
        if df is None or df.empty:
            return
        for c in col_names:
            if c in df.columns:
                existing_gen_pallet_ids.update(
                    df[c].dropna().astype(str).map(_clean_id).tolist()
                )

    try:
        _collect_ids(DBManager.read_table("master_partial_data"), 'Gen Pallet ID', 'Pallet')
    except:
        pass
    # ── UPDATED: old_history read එකේ column rename map එකක් නැති නිසා raw db name
    #    ('gen_pallet_id') එකත් check කරයි — කලින් මෙය miss වී duplicate විය හැකි විය. ──
    try:
        _collect_ids(DBManager.read_table("old_history"),
                     'Gen Pallet ID', 'gen_pallet_id', 'Pallet', 'pallet')
    except:
        pass
    # ── දැන් upload කළ inventory එකේ pallets — gen id එකක් real pallet එකකට සමාන නොවිය යුතුය ──
    try:
        if pallet_col in inv_df.columns:
            existing_gen_pallet_ids.update(
                inv_df[pallet_col].dropna().astype(str).map(_clean_id).tolist()
            )
    except:
        pass
    existing_gen_pallet_ids.discard('')

    gen_pallet_counter = [0]
    _gen_suffix_pat = re.compile(r'^(.+)-P\d{1,6}$')

    def make_unique_gen_pallet_id(pallet):
        base = _clean_id(pallet)
        # pallet එකම දැනටමත් gen id එකක් නම් (…-P0004), root එකෙන් අලුත් එකක් හදයි
        m = _gen_suffix_pat.match(base)
        root = m.group(1) if m else base
        if not root:
            root = 'PAL'
        while True:
            gen_pallet_counter[0] += 1
            suffix = f"-P{gen_pallet_counter[0]:04d}"          # 9999 ට වැඩි වුවත් ගැලපේ
            avail  = MAX_GEN_PALLET_LEN - len(suffix)
            candidate = (root[:avail] if len(root) > avail else root) + suffix
            if len(candidate) <= MAX_GEN_PALLET_LEN and candidate not in existing_gen_pallet_ids:
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

    for lid in req_df['Generated Load ID'].dropna().unique():
        current_reqs = req_df[req_df['Generated Load ID'] == lid]
        if current_reqs.empty:
            continue
        so_num    = str(current_reqs['SO Number'].iloc[0])
        ship_mode = str(current_reqs['SHIP MODE: (SEA/AIR)'].iloc[0]) if 'SHIP MODE: (SEA/AIR)' in current_reqs.columns else ""

        for _, req in current_reqs.iterrows():
            raw_upc = req['Product UPC']
            try:
                f_upc = float(raw_upc)
                upc = str(int(f_upc)) if f_upc == int(f_upc) else str(f_upc)
            except (ValueError, TypeError):
                upc = str(raw_upc).strip()

            # 🐛 FIX: float() එකෙන් කෙලින්ම convert කරද්දී PICK QTY එකේ "1,200"
            # වගේ අගයක් තිබුනොත් ValueError එකෙන් මුළු batch එකම crash වුනා.
            requested_qty = pd.to_numeric(str(req['PICK QTY']).replace(',', '').strip(),
                                          errors='coerce')
            requested_qty = 0.0 if pd.isna(requested_qty) else float(requested_qty)
            needed  = requested_qty
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

                    # 🐛 FIX: orig_qty_map එකේ keys .str.strip() කරලා තිබුනත් මෙතන
                    # str() විතරයි කළේ — pallet එකේ ඉස්සරහින්/පස්සෙන් space එකක්
                    # තිබුනොත් lookup එක miss වෙලා, balance pick එක partial data
                    # එකට වැටුනේ නෑ. දැන් දෙපැත්තම strip කරයි.
                    pallet_val = str(item[pallet_col]).strip() if pallet_col in item.index else ''
                    orig_qty   = orig_qty_map.get(pallet_val, current_avail)

                    # ══════════════════════════════════════════════════════════
                    # NEW RULE — Gen Pallet ID කවදාද හදන්නේ?
                    #   remaining_after > 0  → pallet එකේ balance එකක් ඉතුරු වේ
                    #                          ⇒ Gen Pallet ID හදයි (split).
                    #   remaining_after == 0 → ඉතුරු balance එකත් මේ pick එකෙන්
                    #                          ඉවර වේ ⇒ Gen Pallet ID නොහදා
                    #                          Pallet එකෙන්ම යවයි.
                    #   (කලින් batch එකකදී split වී තිබුණත් — take < orig_qty —
                    #    balance එක ඉවර නම් අලුත් Gen ID එකක් නොහදයි.)
                    # ══════════════════════════════════════════════════════════
                    remaining_after = round(current_avail - take, 6)
                    is_partial      = remaining_after > 0                       # split → Gen ID
                    is_balance_pick = (remaining_after <= 0) and (take < orig_qty)  # ඉතුරු balance එක

                    if is_partial or is_balance_pick:
                        def _get(col_name):
                            c = inv_col_map.get(col_name.lower())
                            return str(item[c]) if c and c in item.index else ''

                        if is_partial:
                            gen_pallet_id = make_unique_gen_pallet_id(pallet_val)
                            pick_rows[-1]['Remark']        = 'Partial'
                            pick_rows[-1]['Gen Pallet ID'] = gen_pallet_id
                        else:
                            # ── Balance pick: අලුත් ID එකක් නැත, Pallet එකම යොදයි ──
                            gen_pallet_id = pallet_val
                            pick_rows[-1]['Remark']        = 'Balance'
                            pick_rows[-1]['Gen Pallet ID'] = ''

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
                            'Balance Qty':        remaining_after,
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

            variance = requested_qty - picked_qty
            summary.append({
                'Batch ID': batch_id, 'SO Number': so_num, 'Load ID': lid,
                'UPC': upc, 'Country': country, 'Ship Mode': ship_mode,
                'Requested': requested_qty, 'Picked': picked_qty,
                'Variance': variance,
                # 🐛 FIX: float subtraction එකෙන් 1e-13 වගේ ඉතුරුවක් එන්න පුළුවන් —
                # `variance == 0` කියන exact check එකෙන් fully picked line එකක්
                # වැරදියට "Shortage" ලෙස පෙන්නුවා.
                'Status': 'Fully Picked' if abs(variance) < 1e-9 else 'Shortage'
            })

    PICK_REPORT_COLS = MASTER_PICK_HEADERS + ['Gen Pallet ID']
    if pick_rows:
        pick_df = pd.DataFrame(pick_rows, columns=PICK_REPORT_COLS)
        pick_df['Actual Qty'] = pd.to_numeric(pick_df['Actual Qty'], errors='coerce').fillna(0)
        pick_df = pick_df[pick_df['Actual Qty'] > 0].reset_index(drop=True)
    else:
        pick_df = pd.DataFrame(columns=PICK_REPORT_COLS)

    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)


# ── 4. App UI & Navigation ─────────────────────────────────────────────────────

if app_header():
    # ── Login අයින් කළා — user/role තව නෑ. සියලු menu options විවෘතයි. ──
    current_user = "System"

    st.markdown("""
    <style>
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.05rem !important; }
    </style>
    """, unsafe_allow_html=True)

    menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report",
            "🔄 Revert/Delete Picks", "🩹 Damage Items", "🏷️ Vendor Maintain", "⚙️ Admin Settings"]

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

                    # ── Drop blank/incomplete trailing rows (e.g. Excel rows with only
                    # whitespace in SO Number / SHIP MODE but no Product UPC / PICK QTY).
                    # Without this, such rows produce a NaN 'Group' key which pandas'
                    # groupby() silently drops, leaving no Generated Load ID for them and
                    # causing an IndexError later in process_picking(). ──
                    req = req.replace(r'^\s*$', pd.NA, regex=True)
                    req = req.dropna(subset=['Product UPC', 'PICK QTY', 'SO Number'], how='any').reset_index(drop=True)

                    # ── 🐛 FIX: PICK QTY එකේ number එකක් නොවන අගයක් ("1,200", "N/A")
                    # තිබුනොත් process_picking() එකේ float() එකෙන් ValueError එකක්
                    # ඇවිත් මුළු batch එකම crash වුනා. දැන් ඒවා වෙන් කරලා user ට
                    # පෙන්නලා, ඉතුරු rows එක්ක ඉදිරියට යයි. ──
                    _qty_num = pd.to_numeric(
                        req['PICK QTY'].astype(str).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce')
                    _bad_qty = req[_qty_num.isna()]
                    if not _bad_qty.empty:
                        st.warning(f"⚠️ PICK QTY එක number එකක් නොවන rows **{len(_bad_qty)}** ක් "
                                   "skip කළා:")
                        st.dataframe(_bad_qty.astype(str), use_container_width=True)
                    req = req[_qty_num.notna()].reset_index(drop=True)
                    if req.empty:
                        st.error("❌ Customer Requirement file එකේ process කරන්න පුළුවන් rows නෑ.")
                        st.stop()

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

                    hist_save_ok = True
                    if new_hist_entries:
                        hist_save_ok = DBManager.insert_rows("load_history", new_hist_entries)

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
                    # 🐛 FIX: කලින් insert_rows() එකේ return value එක බැලුවේ නෑ —
                    # DB save එක fail වුනත් අන්තිමට "✅ Data Processed!" කියලා
                    # පෙන්නුවා. ඒ නිසා user හිතුවේ picks save වුනා කියලා, ඇත්තටම
                    # master_pick_data එකේ කිසිවක් තිබුනේ නෑ. දැන් fail වුනු table
                    # ලැයිස්තුවක් තියාගෙන අන්තිමට report කරයි.
                    save_failed = [] if hist_save_ok else ["load_history"]
                    if not pick_df.empty:
                        pick_save_df = pick_df[MASTER_PICK_HEADERS] if all(c in pick_df.columns for c in MASTER_PICK_HEADERS) else pick_df[[c for c in MASTER_PICK_HEADERS if c in pick_df.columns]]
                        rows_to_insert = pick_save_df.astype(object).where(pd.notnull(pick_save_df), None).to_dict('records')
                        if not DBManager.insert_rows("master_pick_data", rows_to_insert):
                            save_failed.append("master_pick_data")

                    if not part_df.empty:
                        mpd_headers = SHEET_HEADERS["Master_Partial_Data"]
                        for col in mpd_headers:
                            if col not in part_df.columns:
                                part_df[col] = None
                        rows_to_insert = part_df[mpd_headers].astype(object).where(pd.notnull(part_df[mpd_headers]), None).to_dict('records')
                        if not DBManager.insert_rows("master_partial_data", rows_to_insert):
                            save_failed.append("master_partial_data")

                    # Summary_Data — deduplicate by Load ID
                    existing_summ = DBManager.read_table("summary_data")
                    if not existing_summ.empty and 'Load ID' in existing_summ.columns and not summ_df.empty:
                        new_load_ids = set(summ_df['Load ID'].astype(str).tolist())
                        DBManager.delete_where("summary_data", "Load ID", list(new_load_ids))
                    if not summ_df.empty:
                        if not DBManager.insert_rows("summary_data", summ_df.astype(object).where(pd.notnull(summ_df), None).to_dict('records')):
                            save_failed.append("summary_data")

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
                    if save_failed:
                        st.error(f"❌ Batch **{batch_id}** — මේ tables වලට save වුනේ නෑ: "
                                 f"**{', '.join(save_failed)}**. උඩ error බලන්න. "
                                 "Excel report එක download කරගන්න පුළුවන්, නමුත් DB එක "
                                 "update වුනේ නෑ — නැවත try කරන්න.")
                    else:
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
        st.title("📊 Load Tracking & Dashboard")

        col_t1, col_t2 = st.columns([5, 1])
        col_t1.caption("⚡ දත්ත cache වේ (5 min) — වෙනත් user කෙනෙක් දැම්ම අලුත්ම දත්ත සඳහා **🔄 Refresh** click කරන්න.")
        if col_t2.button("🔄 Refresh", use_container_width=True):
            DBManager.invalidate()
            st.rerun()

        # ── Summary metrics (lightweight) ──────────────────────────────────────
        hist_df = DBManager.read_table("load_history")

        total_loads      = hist_df['Generated Load ID'].nunique() if not hist_df.empty and 'Generated Load ID' in hist_df.columns else 0
        total_picks      = DBManager.count_rows("master_pick_data")
        pending_loads    = len(hist_df[hist_df['Pick Status'].astype(str) == 'Pending'])    if not hist_df.empty and 'Pick Status' in hist_df.columns else 0
        processing_loads = len(hist_df[hist_df['Pick Status'].astype(str) == 'Processing']) if not hist_df.empty and 'Pick Status' in hist_df.columns else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Load IDs",   total_loads)
        m2.metric("Total Picks Made", total_picks)
        m3.metric("Pending Loads",    pending_loads)
        m4.metric("Processing Loads", processing_loads)

        st.divider()

        # ══════════════════════════════════════════════════════════════════════
        # 🔍 UNIVERSAL SEARCH — ඕනෑම data එකක් දාන්න (Load ID / SO / Pallet /
        # Order NO / Country / Invoice / GRN / Lot / Vendor / Batch ... ඕනෑම එකක්)
        # සියලු tables හරහා search කර ඒ ට අදාළ හැම record එකක්ම ගෙනෙයි.
        # ══════════════════════════════════════════════════════════════════════
        st.subheader("🔍 Universal Search")
        st.caption("ඕනෑම දත්තයක් type කරන්න — Load ID, SO Number, Pallet, Order NO, Country, "
                   "Invoice, GRN, Lot Number, Vendor, Batch ID... system එකේ සියලු tables හරහා "
                   "අදාළ හැම record එකක්ම ලැබේ.")

        s_c1, s_c2 = st.columns([4, 1])
        search_term = s_c1.text_input("🔍 Search:", key="uni_search_term",
                                      placeholder="උදා: SO-25747-S-002  |  2015600561-P0012  |  HAT52204  |  Australia")
        match_mode = s_c2.selectbox("Match", ["Contains", "Exact"], key="uni_search_mode")

        SEARCH_TABLES = [
            ("📦 Pick Data",        "master_pick_data"),
            ("🧩 Partial Data",     "master_partial_data"),
            ("🚚 Load History",     "load_history"),
            ("📉 Summary/Variance", "summary_data"),
            ("🩹 Damage Items",     "damage_items"),
            ("📋 Inventory Status", "inventory_status"),
        ]

        if search_term and str(search_term).strip():
            term = str(search_term).strip()
            results = {}

            with st.spinner(f"🔍 '{term}' සොයමින්... (සියලු tables)"):
                for label, tbl in SEARCH_TABLES:
                    try:
                        df = DBManager.read_table(tbl)
                    except Exception:
                        df = pd.DataFrame()
                    if df.empty:
                        continue
                    # ── ඕනෑම column එකක match වුනත් row එක ගනී ──
                    # 🐛 FIX: df.apply(lambda col: col.str...) — duplicate column
                    # names තිබුනොත් col එක Series නොව DataFrame වන නිසා .str crash
                    # වෙනවා. දැන් position අනුව column එකින් එක check කරයි.
                    mask = pd.Series(False, index=df.index)
                    for ci in range(df.shape[1]):
                        col_s = df.iloc[:, ci].astype(str)
                        if match_mode == "Exact":
                            mask |= (col_s.str.strip().str.lower() == term.lower())
                        else:
                            mask |= col_s.str.contains(re.escape(term), case=False, na=False)
                    hit = df[mask].reset_index(drop=True)
                    if not hit.empty:
                        results[label] = hit

            total_hits = sum(len(v) for v in results.values())

            if total_hits == 0:
                st.warning(f"❌ '{term}' සඳහා කිසිදු record එකක් හමු නොවීය.")
            else:
                st.success(f"✅ **{total_hits}** records හමු විය — tables **{len(results)}** ක.")

                # ── Quick hit-count strip ──
                hc = st.columns(len(results))
                for i, (label, df_hit) in enumerate(results.items()):
                    hc[i].metric(label, len(df_hit))

                # ── Result tabs ──
                tab_labels = [f"{lbl} ({len(df_hit)})" for lbl, df_hit in results.items()]
                res_tabs = st.tabs(tab_labels + ["⬇️ Download All"])

                for tab, (label, df_hit) in zip(res_tabs[:-1], results.items()):
                    with tab:
                        st.dataframe(df_hit.astype(str), use_container_width=True, height=420)
                        # ── numeric quick totals if present ──
                        qty_cols = [c for c in df_hit.columns
                                    if str(c).strip().lower() in
                                    ('actual qty', 'pick quantity', 'partial qty', 'allocated',
                                     'ats', 'picked', 'requested', 'variance', 'balance qty')]
                        if qty_cols:
                            tcols = st.columns(min(len(qty_cols), 5))
                            for i, qc in enumerate(qty_cols[:5]):
                                tot = pd.to_numeric(df_hit[qc], errors='coerce').fillna(0).sum()
                                tcols[i].metric(f"Σ {qc}", f"{tot:,.0f}")

                with res_tabs[-1]:
                    out_uni = io.BytesIO()
                    with pd.ExcelWriter(out_uni, engine='xlsxwriter') as writer:
                        wb_u = writer.book
                        hdr_u = wb_u.add_format({'bold': True, 'bg_color': '#1a1a1a',
                                                 'font_color': '#fff', 'border': 1, 'font_size': 10})
                        for label, df_hit in results.items():
                            sheet = re.sub(r'[^A-Za-z0-9 _]', '', label).strip()[:28] or "Sheet"
                            df_hit.to_excel(writer, sheet_name=sheet, index=False)
                            ws_u = writer.sheets[sheet]
                            for ci, cn in enumerate(df_hit.columns):
                                ws_u.write(0, ci, str(cn), hdr_u)
                                ws_u.set_column(ci, ci, 16)
                            ws_u.freeze_panes(1, 0)
                    safe_term = re.sub(r'[^A-Za-z0-9_-]', '_', term)[:40]
                    st.download_button(f"⬇️ Download Search Results — {term}",
                        data=out_uni.getvalue(),
                        file_name=f"Search_{safe_term}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.ms-excel", use_container_width=True, type="primary")
                    st.caption(f"Tables {len(results)} ක records {total_hits} ක් — sheet එකකට table එකක්.")
        else:
            st.info("👆 උඩ box එකේ ඕනෑම දත්තයක් type කරන්න — ඒ ට අදාළ සියලු records ලැබේ.")

        # ══════════════════════════════════════════════════════════════════════
        # 🚚 Load Status Update (compact)
        # ══════════════════════════════════════════════════════════════════════
        st.divider()
        with st.expander("🚚 Load Status Update", expanded=False):
            if hist_df.empty or 'Generated Load ID' not in hist_df.columns:
                st.info("Load records නොමැත.")
            else:
                STATUS_OPTIONS = ["Pending", "PL Pending", "Processing", "Completed", "Cancelled"]
                active_hist = hist_df[~hist_df['Pick Status'].astype(str).isin(['Cancelled', 'Completed'])] \
                    if 'Pick Status' in hist_df.columns else hist_df
                show_all_loads = st.checkbox("Completed / Cancelled ද පෙන්වන්න", value=False, key="ls_show_all")
                pool = hist_df if show_all_loads else active_hist
                lid_list = pool['Generated Load ID'].dropna().astype(str).unique().tolist()

                if not lid_list:
                    st.info("සියලු Loads Completed හෝ Cancelled වී ඇත.")
                else:
                    ls1, ls2, ls3 = st.columns([3, 2, 1])
                    sel_lid = ls1.selectbox("Load ID:", sorted(lid_list, reverse=True), key="ls_lid")
                    cur_rows = pool[pool['Generated Load ID'].astype(str) == str(sel_lid)]
                    cur_status = str(cur_rows.iloc[0].get('Pick Status', 'Pending')) if not cur_rows.empty else 'Pending'
                    safe_idx = STATUS_OPTIONS.index(cur_status) if cur_status in STATUS_OPTIONS else 0
                    new_status = ls2.selectbox("New Status:", STATUS_OPTIONS, index=safe_idx, key="ls_status")

                    if not cur_rows.empty:
                        r0 = cur_rows.iloc[0]
                        st.caption(f"**SO:** {r0.get('SO Number','-')} &nbsp;|&nbsp; "
                                   f"**Country:** {r0.get('Country Name','-')} &nbsp;|&nbsp; "
                                   f"**Ship:** {r0.get('SHIP MODE','-')} &nbsp;|&nbsp; "
                                   f"**Current:** `{cur_status}`")

                    if ls3.button("💾 Save", key="ls_save", use_container_width=True, type="primary"):
                        try:
                            ok = DBManager.update_cell("load_history", "Generated Load ID",
                                                       str(sel_lid), "Pick Status", new_status)
                            if ok and new_status == "Cancelled":
                                mpd = DBManager.read_table("master_pick_data")
                                lid_col = next((c for c in mpd.columns if str(c).strip().lower() == 'load id'), None)
                                # 🐛 FIX: overwrite එක fail වුනත් "deleted" කියලා
                                # පෙන්නුවා. දැන් ඇත්තටම මොකද වුනේ කියලා කියයි.
                                purged = True
                                if not mpd.empty and lid_col:
                                    filtered_mpd = mpd[mpd[lid_col].astype(str).str.strip() != str(sel_lid).strip()]
                                    if len(filtered_mpd) != len(mpd):
                                        purged = DBManager._overwrite_table("master_pick_data", filtered_mpd)
                                if purged:
                                    st.success(f"✅ {sel_lid} → Cancelled | Master_Pick_Data records deleted.")
                                else:
                                    st.error(f"⚠️ {sel_lid} → Cancelled, නමුත් Master_Pick_Data "
                                             "records delete වුනේ නෑ.")
                            elif ok:
                                st.success(f"✅ {sel_lid} → {new_status}")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Update error: {ex}")


    # ==========================================================================
    # TAB 3: INVENTORY DETAILS REPORT
    # ==========================================================================
    elif choice == "📋 Inventory Details Report":
        st.title("📋 Inventory Details Report")

        st.info(
            "**නව Logic Flow** — Inventory Report එකේ **Location Id** අනුව බෙදයි:\n\n"
            "• **SC20** = Pick කරපු Stock → **Logic 1** (License Plate Contents report එකෙන් Order NO)\n"
            "• **HKA** = Allocated කරපු Stock → **Logic 2** (master_pick_data) → **Logic 3** (master_partial_data)\n"
            "• **Damage** pallets → **Logic 4** (system එකේ logic එකම)\n"
            "• අනිත් ඔක්කොම → **Logic 5** (ATS)"
        )

        # ══════════════════════════════════════════════════════════════════════
        # UPLOADS — Inventory Report + License Plate Contents Report
        # ══════════════════════════════════════════════════════════════════════
        up_c1, up_c2 = st.columns(2)
        inv_report_file = up_c1.file_uploader(
            "1️⃣ Upload Inventory Report", type=['csv', 'xlsx'], key="inv_report_uploader")
        lpc_file = up_c2.file_uploader(
            "2️⃣ Upload License Plate Contents Report", type=['csv', 'xlsx'], key="lpc_uploader")

        if lpc_file is None:
            up_c2.caption("⚠️ SC20 (Logic 1) සඳහා මෙය අවශ්‍යයි — නැත්නම් SC20 rows unmatched වේ.")

        with st.expander("🗄️ inventory_status table setup / schema check", expanded=False):
            st.caption("Supabase එකේ මෙම table එක නැත්නම්, මෙම SQL එක එක් වරක් run කරන්න:")
            st.code(INVENTORY_STATUS_TABLE_SQL, language="sql")

            st.markdown("---")
            st.caption("දැනට DB එකේ මොන columns තියෙනවද කියලා check කරන්න:")
            if st.button("🔎 Check inventory_status schema", key="chk_invstatus_schema"):
                want = list(INVSTATUS_COL_MAP.values())
                valid, missing = DBManager._filter_valid_columns("inventory_status", want)
                if not missing:
                    st.success(f"✅ සියලු columns **{len(valid)}** ම තියෙනවා — schema OK.")
                else:
                    st.error(f"❌ නැති columns **{len(missing)}**: {', '.join(missing)}")
                    st.caption("Supabase → SQL Editor එකේ මේක run කරන්න:")
                    st.code(
                        "\n".join(
                            f"ALTER TABLE inventory_status ADD COLUMN IF NOT EXISTS "
                            f"{c} {MISSING_COL_TYPES.get(c, 'text')};" for c in missing
                        ) + "\n\n-- PostgREST schema cache එක refresh කරයි:\n"
                            "NOTIFY pgrst, 'reload schema';",
                        language="sql")

        with st.expander("📂 Master Data CSV Override (Supabase connect නැත්නම්)", expanded=False):
            st.caption("Supabase ඇත්නම් upload නොකළත් හරි. CSV upload කළොත් ඒ data priority ලැබේ.")
            col_csv1, col_csv2 = st.columns(2)
            col_csv3, col_csv4 = st.columns(2)
            csv_pick_file    = col_csv1.file_uploader("master_pick_data CSV",    type=['csv'], key="fmt_csv_pick")
            csv_partial_file = col_csv2.file_uploader("master_partial_data CSV", type=['csv'], key="fmt_csv_partial")
            csv_damage_file  = col_csv3.file_uploader("damage_items CSV",        type=['csv'], key="fmt_csv_damage")
            csv_vendor_file  = col_csv4.file_uploader("vendor_maintain CSV",     type=['csv'], key="fmt_csv_vendor")

        opt_c1, opt_c2 = st.columns(2)
        save_to_db = opt_c1.checkbox(
            "💾 inventory_status DB එකට save කරන්න (clear → insert)", value=True, key="fmt_save_db")
        loc_sc20 = opt_c2.text_input("SC20 / HKA Location codes", value="SC20,HKA",
                                     key="loc_codes",
                                     help="Pick location, Allocated location — comma වලින් වෙන් කරන්න").strip()
        _loc_parts = [p.strip().upper() for p in loc_sc20.split(',') if p.strip()]
        LOC_PICK  = _loc_parts[0] if len(_loc_parts) > 0 else 'SC20'
        LOC_ALLOC = _loc_parts[1] if len(_loc_parts) > 1 else 'HKA'

        if inv_report_file:
            if st.button("📊 Generate Inventory Details Report", type="primary",
                         use_container_width=True, key="gen_inv_rpt"):
                with st.spinner("Inventory process කරමින්..."):

                    # ════════════════════════════════════════════════════════════
                    # Helpers
                    # ════════════════════════════════════════════════════════════
                    def _is_blank(v):
                        return sv(v) == ''          # 🐛 FIX: '<NA>' / 'NaT' ද cover වේ

                    def _to_num(v):
                        n = pd.to_numeric(v, errors='coerce')
                        return 0.0 if pd.isna(n) else float(n)

                    def _qkey(v):
                        return round(_to_num(v), 6)

                    def _load_any(uploaded_file):
                        uploaded_file.seek(0)
                        if uploaded_file.name.lower().endswith('.csv'):
                            return pd.read_csv(uploaded_file, keep_default_na=False, na_values=[''])
                        return pd.read_excel(uploaded_file, keep_default_na=False, na_values=[''])

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

                    def _col(df, name, default=None):
                        lc = {str(c).strip().lower(): str(c) for c in df.columns}
                        return lc.get(name.strip().lower(), default)

                    # ════════════════════════════════════════════════════════════
                    # STEP 1 — Read uploaded Inventory Report
                    # ════════════════════════════════════════════════════════════
                    inv_data = _load_any(inv_report_file)
                    inv_data.columns = [str(c).strip() for c in inv_data.columns]
                    _inv_lc = {c.lower(): c for c in inv_data.columns}

                    def _icol(name):
                        return _inv_lc.get(name.strip().lower())

                    _C_PALLET = _icol('pallet')
                    _C_AQTY   = _icol('actual qty')
                    _C_LOC    = _icol('location id')

                    if _C_PALLET is None or _C_AQTY is None:
                        st.error("❌ Inventory file එකේ 'Pallet' සහ 'Actual Qty' columns තිබිය යුතුය.")
                        st.stop()
                    if _C_LOC is None:
                        st.error("❌ Inventory file එකේ 'Location Id' column එක තිබිය යුතුය "
                                 f"({LOC_PICK} / {LOC_ALLOC} වෙන් කරන්න බැහැ).")
                        st.stop()

                    # ════════════════════════════════════════════════════════════
                    # STEP 2 — Read License Plate Contents report  [Logic 1 source]
                    #   Allocation → 'HKEFL-' අයින් කළාම = Order NO
                    #   License Plate = Pallet
                    # ════════════════════════════════════════════════════════════
                    lp_by_plate = {}      # plate → [ {order, qty} ]  (LP file order)
                    lp_rows_n = 0
                    if lpc_file is not None:
                        try:
                            lp_data = _load_any(lpc_file)
                            lp_data.columns = [str(c).strip() for c in lp_data.columns]
                            _L_PLATE = _col(lp_data, 'license plate')
                            _L_ALLOC = _col(lp_data, 'allocation')
                            _L_QTY   = _col(lp_data, 'quantity')
                            if _L_PLATE is None or _L_ALLOC is None:
                                st.error("❌ License Plate Contents file එකේ 'License Plate' සහ "
                                         "'Allocation' columns තිබිය යුතුය.")
                                st.stop()
                            for _, lr in lp_data.iterrows():
                                plate = sv(lr.get(_L_PLATE, ''))
                                if not plate:
                                    continue
                                alloc = sv(lr.get(_L_ALLOC, ''))
                                # ── 'HKEFL-' කොටස අයින් කරලා Order NO ගන්නවා ──
                                order_no = re.sub(r'^\s*HKEFL\s*-\s*', '', alloc, flags=re.IGNORECASE).strip()
                                lp_by_plate.setdefault(plate, []).append({
                                    'order': order_no,
                                    'qty':   _to_num(lr.get(_L_QTY, 0)) if _L_QTY else 0.0,
                                })
                                lp_rows_n += 1
                            st.caption(f"✅ License Plate Contents: **{lp_rows_n}** rows | "
                                       f"unique plates: **{len(lp_by_plate)}**")
                        except Exception as _lpe:
                            st.warning(f"⚠️ License Plate Contents read error: {_lpe}")

                    # ════════════════════════════════════════════════════════════
                    # STEP 3 — Load master data (DB or CSV override)
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

                    try:    hist_lookup_df = DBManager.read_table("load_history")
                    except: hist_lookup_df = pd.DataFrame()

                    # ════════════════════════════════════════════════════════════
                    # STEP 4 — Build lookup maps
                    #   ⚠️ අලුත් data එකේ ඉදන් පරණ data එකට → .iloc[::-1] (reverse)
                    # ════════════════════════════════════════════════════════════

                    # ── [Logic 2] master_pick_data: (pallet, actual_qty) → queue of records ──
                    mpd_pool = {}
                    if not mpd_df.empty:
                        mp_p = _col(mpd_df, 'pallet', 'Pallet')
                        mp_q = _col(mpd_df, 'actual qty', 'Actual Qty')
                        mp_c = _col(mpd_df, 'country name', 'Country Name')
                        mp_o = _col(mpd_df, 'generated load id', 'Generated Load ID')
                        for _, r in mpd_df.iloc[::-1].iterrows():          # NEW → OLD
                            pal = sv(r.get(mp_p, ''))
                            if not pal:
                                continue
                            mpd_pool.setdefault((pal, _qkey(r.get(mp_q, 0))), []).append({
                                'country': sv(r.get(mp_c, '')),
                                'order':   sv(r.get(mp_o, '')),
                            })

                    # ── [Logic 3] master_partial_data: (gen_pallet_id, partial_qty) → queue ──
                    part_pool = {}
                    if not part_df.empty:
                        pg  = _col(part_df, 'gen pallet id', 'Gen Pallet ID')
                        pq  = _col(part_df, 'partial qty', 'Partial Qty')
                        pc  = _col(part_df, 'country name', 'Country Name')
                        po  = _col(part_df, 'load id', 'Load ID')
                        pi  = _col(part_df, 'invoice number', 'Invoice Number')
                        pgr = _col(part_df, 'grn number', 'Grn Number')
                        pv  = _col(part_df, 'vendor name', 'Vendor Name')
                        for _, r in part_df.iloc[::-1].iterrows():         # NEW → OLD
                            gpal = sv(r.get(pg, ''))
                            if not gpal:
                                continue
                            part_pool.setdefault((gpal, _qkey(r.get(pq, 0))), []).append({
                                'country': sv(r.get(pc, '')),
                                'order':   sv(r.get(po, '')),
                                'invoice': sv(r.get(pi, '')) if pi else '',
                                'grn':     sv(r.get(pgr, '')) if pgr else '',
                                'vendor':  sv(r.get(pv, '')) if pv else '',
                            })

                    # ── Order NO → Country lookup (Logic 1 Destination Country) ──
                    #    load_history → master_pick_data → master_partial_data
                    order_country = {}

                    def _reg_order_country(order_v, country_v):
                        o, c = sv(order_v), sv(country_v)
                        if o and c:
                            order_country.setdefault(o.upper(), c)

                    if not part_df.empty:
                        _po = _col(part_df, 'load id', 'Load ID')
                        _pc = _col(part_df, 'country name', 'Country Name')
                        for _, r in part_df.iterrows():
                            _reg_order_country(r.get(_po, ''), r.get(_pc, ''))
                    if not mpd_df.empty:
                        _mo = _col(mpd_df, 'generated load id', 'Generated Load ID')
                        _mc = _col(mpd_df, 'country name', 'Country Name')
                        for _, r in mpd_df.iterrows():
                            _reg_order_country(r.get(_mo, ''), r.get(_mc, ''))
                    if not hist_lookup_df.empty:
                        _ho = _col(hist_lookup_df, 'generated load id', 'Generated Load ID')
                        _hc = _col(hist_lookup_df, 'country name', 'Country Name')
                        for _, r in hist_lookup_df.iterrows():
                            _reg_order_country(r.get(_ho, ''), r.get(_hc, ''))

                    def lookup_country(order_no):
                        o = str(order_no).strip().upper()
                        if not o:
                            return ''
                        if o in order_country:
                            return order_country[o]
                        # trailing letter variant (e.g. SO-26372-S-007A → SO-26372-S-007)
                        o2 = re.sub(r'[A-Z]$', '', o)
                        return order_country.get(o2, '')

                    # ── [Logic 4] Damage maps — system එකේ logic එකම ──
                    damage_qty = {}
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
                    # 🐛 FIX: damage remark එකක් report column එකක නමට සමාන නම්
                    # (උදා: remark = "ATS" හෝ "Remark") report_cols එකේ duplicate
                    # column හැදිලා Excel export එක කැඩෙනවා. ඒවා rename කරයි.
                    _reserved = {h.lower() for h in ['Vendor Name', 'Invoice Number', 'Fifo Date',
                                 'Grn Number', 'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
                                 'Lot Number', 'Style', 'Color', 'Size', 'Client So 2',
                                 'Inventory Type', 'Location Id', 'Actual Qty', 'Pick Quantity',
                                 'Allocated', 'Destination Country', 'Order NO', 'ATS', 'COO',
                                 'Remark', 'Status', 'Row Order', 'Balance Qty']}
                    _seen_rmk, _fixed_remarks, _rmk_alias = set(), [], {}
                    for _rk in damage_remarks:
                        _new = _rk
                        while _new.lower() in _reserved or _new.lower() in _seen_rmk:
                            _new = f"{_new} (Damage)"
                        _seen_rmk.add(_new.lower())
                        _fixed_remarks.append(_new)
                        _rmk_alias[_rk] = _new
                    if _fixed_remarks != damage_remarks:
                        st.warning("⚠️ Damage remark නමක් report column එකකට ගැටෙනවා — "
                                   f"rename කළා: {[f'{k} → {v}' for k, v in _rmk_alias.items() if k != v]}")
                        damage_qty = {(p, _rmk_alias.get(rk, rk)): q for (p, rk), q in damage_qty.items()}
                        damage_remarks = _fixed_remarks

                    # ── Vendor → Country map (COO) ──
                    vendor_country = {}
                    if not vendor_df.empty and 'Vendor Name' in vendor_df.columns and 'Country' in vendor_df.columns:
                        for v, c in zip(vendor_df['Vendor Name'], vendor_df['Country']):
                            if sv(v) and sv(c):
                                vendor_country[sv(v).lower()] = sv(c)

                    # ════════════════════════════════════════════════════════════
                    # STEP 5 — Row builder
                    # ════════════════════════════════════════════════════════════
                    BASE_HEADERS = ['Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                                    'Client So', 'Pallet', 'Supplier Hu', 'Supplier', 'Lot Number',
                                    'Style', 'Color', 'Size', 'Client So 2', 'Inventory Type',
                                    'Location Id', 'Actual Qty']

                    def base_row(inv_row, pallet=None, actual_qty=None):
                        row = {}
                        for h in BASE_HEADERS:
                            # 🐛 FIX: sv() හරහා — '<NA>'/'NaT'/'nan' report එකට යන්නේ නෑ
                            if h == 'Pallet':
                                row[h] = pallet if pallet is not None else sv(inv_row.get(_C_PALLET, ''))
                            elif h == 'Actual Qty':
                                row[h] = actual_qty if actual_qty is not None else _to_num(inv_row.get(_C_AQTY, 0))
                            elif h == 'Client So 2':
                                cs = _icol('client so')
                                row[h] = sv(inv_row.get(cs, '')) if cs else ''
                            elif h == 'Location Id':
                                row[h] = sv(inv_row.get(_C_LOC, ''))
                            else:
                                src = _icol(h)
                                row[h] = sv(inv_row.get(src, '')) if src else ''
                        row['Pick Quantity'] = ''
                        row['Allocated'] = ''
                        row['Destination Country'] = ''
                        row['Order NO'] = ''
                        row['ATS'] = ''
                        row['COO'] = ''
                        row['Remark'] = ''
                        for rmk in damage_remarks:
                            row[rmk] = ''
                        row['Status'] = ''
                        row['_src_pallet'] = sv(inv_row.get(_C_PALLET, ''))
                        row['_logic'] = ''
                        return row

                    def set_coo(row):
                        vn = str(row.get('Vendor Name', '')).strip()
                        row['COO'] = vendor_country.get(vn.lower(), '') if vn else ''

                    out_rows = []
                    cnt = {'L1': 0, 'L1x': 0, 'L2': 0, 'L3': 0, 'L3x': 0, 'L4': 0, 'L5': 0}

                    # ── අලුත් data එකේ ඉදන් පරණ data එකට: inventory reverse order ──
                    inv_records = list(inv_data.iterrows())[::-1]

                    # ════════════════════════════════════════════════════════════
                    # STEP 6 — Run Logic 1 → 5
                    # ════════════════════════════════════════════════════════════
                    for _, inv_row in inv_records:
                        pallet = sv(inv_row.get(_C_PALLET, ''))
                        aqty   = _to_num(inv_row.get(_C_AQTY, 0))
                        loc    = sv(inv_row.get(_C_LOC, '')).upper()

                        # ── LOGIC 4: Damage — system එකේ logic එකම, අනිත් logic වලින් අයින් ──
                        if pallet in damage_pallets:
                            r = base_row(inv_row)
                            for rmk in damage_remarks:
                                q = damage_qty.get((pallet, rmk), '')
                                r[rmk] = q if q != '' else ''
                            if all(_is_blank(r[rk]) for rk in damage_remarks):
                                r[damage_remarks[0]] = aqty
                            r['Status'] = 'D'
                            r['_logic'] = 'L4-Damage'
                            set_coo(r)
                            out_rows.append(r)
                            cnt['L4'] += 1
                            continue

                        # ══════════════════════════════════════════════════════
                        # LOGIC 1 — SC20 = Pick කරපු Stock
                        #   Order NO       ← License Plate Contents (Allocation − 'HKEFL-')
                        #   Pick Quantity  ← Actual Qty
                        #   Dest. Country  ← system DB (Order NO lookup)
                        # ══════════════════════════════════════════════════════
                        if loc == LOC_PICK:
                            r = base_row(inv_row)
                            r['Pick Quantity'] = aqty
                            entries = lp_by_plate.get(pallet)
                            if entries:
                                # qty match එකක් තියෙනවා නම් ඒක, නැත්නම් පළමු එක
                                pick_entry = next(
                                    (e for e in entries if _qkey(e['qty'] / 1000.0) == _qkey(aqty)),
                                    None) or next(
                                    (e for e in entries if _qkey(e['qty']) == _qkey(aqty)),
                                    None) or entries[0]
                                order_no = pick_entry['order']
                                r['Order NO'] = order_no
                                r['Destination Country'] = lookup_country(order_no)
                                r['Status'] = 'D'
                                r['_logic'] = 'L1-SC20'
                                if not r['Destination Country']:
                                    r['Remark'] = 'Country not found in DB'
                                cnt['L1'] += 1
                            else:
                                r['Remark'] = 'License Plate Contents එකේ නෑ (Order NO හමු නොවීය)'
                                r['_logic'] = 'L1-SC20-Unmatched'
                                cnt['L1x'] += 1
                            set_coo(r)
                            out_rows.append(r)
                            continue

                        # ══════════════════════════════════════════════════════
                        # LOGIC 2 / 3 — HKA = Allocated කරපු Stock
                        # ══════════════════════════════════════════════════════
                        if loc == LOC_ALLOC:
                            r = base_row(inv_row)
                            r['Allocated'] = aqty

                            # ── LOGIC 2: master_pick_data (pallet + actual_qty), NEW → OLD ──
                            key = (pallet, _qkey(aqty))
                            queue = mpd_pool.get(key)
                            if queue:
                                hit = queue.pop(0)                 # consume (එක record එකක් එක වතාවයි)
                                r['Order NO'] = hit['order']
                                r['Destination Country'] = hit['country'] or lookup_country(hit['order'])
                                r['Status'] = 'D'
                                r['_logic'] = 'L2-MPD'
                                cnt['L2'] += 1
                                set_coo(r)
                                out_rows.append(r)
                                continue

                            # ── LOGIC 3: master_partial_data (gen_pallet_id + partial_qty), NEW → OLD ──
                            pqueue = part_pool.get(key)
                            if pqueue:
                                hit = pqueue.pop(0)
                                r['Order NO'] = hit['order']
                                r['Destination Country'] = hit['country'] or lookup_country(hit['order'])
                                if _is_blank(r['Invoice Number']) and not _is_blank(hit['invoice']):
                                    r['Invoice Number'] = hit['invoice']
                                if _is_blank(r['Grn Number']) and not _is_blank(hit['grn']):
                                    r['Grn Number'] = hit['grn']
                                if _is_blank(r['Vendor Name']) and not _is_blank(hit['vendor']):
                                    r['Vendor Name'] = hit['vendor']
                                r['Status'] = 'D'
                                r['_logic'] = 'L3-Partial'
                                cnt['L3'] += 1
                                set_coo(r)
                                out_rows.append(r)
                                continue

                            # ── LOGIC 3 Unmatched → report එකට add + Remark ──
                            r['Remark'] = ('Unmatched — master_pick_data / master_partial_data '
                                           'දෙකේම Pallet + Qty match වුනේ නෑ')
                            r['_logic'] = 'L3-Unmatched'
                            cnt['L3x'] += 1
                            set_coo(r)
                            out_rows.append(r)
                            continue

                        # ══════════════════════════════════════════════════════
                        # LOGIC 5 — අනිත් ඔක්කොම → Actual Qty + ATS
                        # ══════════════════════════════════════════════════════
                        r = base_row(inv_row)
                        r['ATS'] = aqty
                        r['_logic'] = 'L5-ATS'
                        cnt['L5'] += 1
                        set_coo(r)
                        out_rows.append(r)

                    # ════════════════════════════════════════════════════════════
                    # STEP 6.5 — BLANK FILL
                    #   Invoice Number / Grn Number / Supplier Hu — nan හෝ blank නම්:
                    #     (A) Pallet එකෙන් → System DB එකේ ඕනෑම table එකකින්
                    #     (B) හම්බුනේ නැත්නම් → master_partial_data එකේ gen_pallet_id
                    #         එකෙන් match කරලා, ඒකට අදාළ මුල් Pallet එක අරගෙන, ඒකෙන්
                    #     (C) Fallback → '{base}-P0001' pattern එකෙන් base pallet derive
                    # ════════════════════════════════════════════════════════════
                    FILL_FIELDS = ['Invoice Number', 'Grn Number', 'Supplier Hu']

                    pallet_ref = {}     # pallet → {field: (value, source_table)}
                    gen_to_base = {}    # gen_pallet_id → මුල් Pallet

                    def _clean(v):
                        return sv(v)

                    def _find_col(df, name):
                        """app header ('Invoice Number') හෝ raw db name ('invoice_number') දෙකම."""
                        lc = {str(c).strip().lower(): str(c) for c in df.columns}
                        n = name.strip().lower()
                        return lc.get(n) or lc.get(n.replace(' ', '_'))

                    def _absorb_ref(df, source_name, pallet_cols=('Pallet',)):
                        """df එකෙන් pallet → invoice/grn/supplier_hu values pallet_ref එකට එකතු
                        කරයි. කලින් source එකකින් හම්බුන value එක override වෙන්නේ නෑ (first-win)."""
                        if df is None or df.empty:
                            return 0
                        f_cols = {f: _find_col(df, f) for f in FILL_FIELDS}
                        f_cols = {f: c for f, c in f_cols.items() if c}
                        if not f_cols:
                            return 0
                        p_cols = [c for c in (_find_col(df, pc) for pc in pallet_cols) if c]
                        if not p_cols:
                            return 0
                        added = 0
                        for _, r in df.iterrows():
                            vals = {f: _clean(r.get(c, '')) for f, c in f_cols.items()}
                            vals = {f: v for f, v in vals.items() if v}
                            if not vals:
                                continue
                            for pc in p_cols:
                                key = _clean(r.get(pc, ''))
                                if not key:
                                    continue
                                slot = pallet_ref.setdefault(key, {})
                                for f, v in vals.items():
                                    if f not in slot:
                                        slot[f] = (v, source_name)
                                        added += 1
                        return added

                    def _absorb_gen(df, source_name):
                        """gen_pallet_id → මුල් Pallet map එක."""
                        if df is None or df.empty:
                            return 0
                        gc = _find_col(df, 'Gen Pallet ID')
                        pc = _find_col(df, 'Pallet')
                        if not gc or not pc:
                            return 0
                        n = 0
                        for _, r in df.iterrows():
                            g, p = _clean(r.get(gc, '')), _clean(r.get(pc, ''))
                            if g and p and g != p and g not in gen_to_base:
                                gen_to_base[g] = p
                                n += 1
                        return n

                    ref_stats = {}
                    with st.spinner("🔎 Blank fill sources load කරමින්..."):
                        # ── Priority 1: upload කළ inventory එකේම අනිත් rows ──
                        ref_stats['Uploaded Inventory'] = _absorb_ref(inv_data, 'Uploaded Inventory')
                        # ── Priority 2: master_pick_data ──
                        ref_stats['master_pick_data'] = _absorb_ref(mpd_df, 'master_pick_data')
                        # ── Priority 3: master_partial_data (Pallet + Gen Pallet ID දෙකටම) ──
                        ref_stats['master_partial_data'] = _absorb_ref(
                            part_df, 'master_partial_data', pallet_cols=('Pallet', 'Gen Pallet ID'))
                        _absorb_gen(part_df, 'master_partial_data')
                        # ── Priority 4/5/6: old_history_master, old_history, inventory_status ──
                        for _tbl, _label in [("old_history_master", "old_history_master"),
                                             ("old_history",        "old_history"),
                                             ("inventory_status",   "inventory_status (prev run)")]:
                            try:
                                _df = DBManager.read_table(_tbl)
                            except Exception:
                                _df = pd.DataFrame()
                            pcols = ('Pallet', 'Gen Pallet ID') if _tbl == "old_history" else ('Pallet',)
                            ref_stats[_label] = _absorb_ref(_df, _label, pallet_cols=pcols)
                            if _tbl == "old_history":
                                _absorb_gen(_df, 'old_history')

                    _gen_pat = re.compile(r'^(.+)-P\d+$', re.IGNORECASE)

                    def _lookup_fill(pallet_key, field):
                        """(value, source) හෝ (None, None)."""
                        if not pallet_key:
                            return None, None
                        slot = pallet_ref.get(pallet_key)
                        if slot and field in slot:
                            return slot[field]
                        return None, None

                    fill_log = []
                    fill_counts = {'A-Pallet': 0, 'B-GenPallet': 0, 'C-Pattern': 0}

                    for r in out_rows:
                        need = [f for f in FILL_FIELDS if _is_blank(r.get(f))]
                        if not need:
                            continue
                        rep_pal = _clean(r.get('Pallet', ''))
                        src_pal = _clean(r.get('_src_pallet', ''))

                        for f in need:
                            val = src = None
                            stage = None

                            # ── (A) Pallet එකෙන් කෙලින්ම ──
                            for k in (rep_pal, src_pal):
                                if k:
                                    val, src = _lookup_fill(k, f)
                                    if val:
                                        stage = 'A-Pallet'
                                        break

                            # ── (B) gen_pallet_id → මුල් Pallet ──
                            if not val:
                                for k in (rep_pal, src_pal):
                                    base = gen_to_base.get(k) if k else None
                                    if base:
                                        val, src = _lookup_fill(base, f)
                                        if val:
                                            stage = 'B-GenPallet'
                                            src = f"{src} ← gen:{k}"
                                            break

                            # ── (C) '{base}-P0001' pattern එකෙන් base pallet derive ──
                            if not val:
                                for k in (rep_pal, src_pal):
                                    m = _gen_pat.match(k) if k else None
                                    if m:
                                        val, src = _lookup_fill(m.group(1), f)
                                        if val:
                                            stage = 'C-Pattern'
                                            src = f"{src} ← base:{m.group(1)}"
                                            break

                            if val:
                                r[f] = val
                                fill_counts[stage] = fill_counts.get(stage, 0) + 1
                                fill_log.append({
                                    'Pallet': rep_pal, 'Location Id': r.get('Location Id', ''),
                                    'Field': f, 'Filled Value': val,
                                    'Stage': stage, 'Source': src,
                                })

                    fill_total = sum(fill_counts.values())
                    still_blank = sum(1 for r in out_rows for f in FILL_FIELDS if _is_blank(r.get(f)))

                    if fill_total:
                        st.success(
                            f"🔎 Blank Fill: **{fill_total}** cells පිරෙව්වා "
                            f"(A·Pallet={fill_counts.get('A-Pallet',0)} · "
                            f"B·GenPallet={fill_counts.get('B-GenPallet',0)} · "
                            f"C·Pattern={fill_counts.get('C-Pattern',0)}) | තව blank: **{still_blank}**"
                        )
                    else:
                        st.info(f"🔎 Blank Fill: පිරෙව්වා 0 | blank cells: **{still_blank}** "
                                "(reference tables වල data නෑ විය හැක)")

                    fill_log_df = pd.DataFrame(fill_log)
                    with st.expander(f"🔎 Blank Fill විස්තර ({len(fill_log_df)} cells)", expanded=False):
                        st.caption("Reference sources — first-win priority: Uploaded Inventory → "
                                   "master_pick_data → master_partial_data → old_history_master → "
                                   "old_history → inventory_status")
                        rs = pd.DataFrame([{'Source Table': k, 'Reference Values Loaded': v}
                                           for k, v in ref_stats.items()])
                        st.dataframe(rs.astype(str), use_container_width=True)
                        if not fill_log_df.empty:
                            st.dataframe(fill_log_df.astype(str), use_container_width=True, height=280)
                    # ════════════════════════════════════════════════════════════
                    # STEP 7 — Build report dataframe
                    # ════════════════════════════════════════════════════════════
                    # process කරන්න rows නැත්නම් (Pallet/Actual Qty හිස් file එකක්)
                    # කලින් හිස් report එකක් + "Qty Match ✅" වගේ නොගැලපෙන
                    # metrics ටිකක් generate වුනා. දැන් පැහැදිලි error එකක් දෙයි.
                    if not out_rows:
                        st.error("❌ Inventory file එකේ process කරන්න පුළුවන් rows නෑ "
                                 "(Pallet / Actual Qty හිස්ද කියලා බලන්න).")
                        st.stop()

                    for i, r in enumerate(out_rows):
                        r['Row Order'] = i + 1

                    report_cols = (BASE_HEADERS
                                   + ['Pick Quantity', 'Allocated', 'Destination Country', 'Order NO']
                                   + damage_remarks + ['ATS', 'COO', 'Remark'])

                    fmt_df = pd.DataFrame(out_rows)
                    for c in report_cols:
                        if c not in fmt_df.columns:
                            fmt_df[c] = ''
                    fmt_df = fmt_df[fmt_df['Pallet'].map(sv) != ''].reset_index(drop=True)
                    report_df = fmt_df[report_cols].copy()

                    # ════════════════════════════════════════════════════════════
                    # STEP 8 — Save to inventory_status DB
                    # ════════════════════════════════════════════════════════════
                    if save_to_db:
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
                                st.success(f"✅ inventory_status DB clear → **{len(db_out)}** rows save කළා.")
                            else:
                                # 🐛 FIX: කලින් replace_table() හැම විටම True return කළ නිසා
                                # save එක fail වුනත් "✅ save කළා" කියලා පෙන්නුවා.
                                st.error("❌ inventory_status save fail වුනා — උඩ error එක බලන්න. "
                                         "Report එක download කරගන්න පුළුවන්, නමුත් DB එක update වුනේ නෑ.")
                        except Exception as _dberr:
                            st.warning(f"⚠️ DB save skip කළා: {_dberr}")

                    # ════════════════════════════════════════════════════════════
                    # STEP 9 — Totals & metrics
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
                    sc2.metric(f"Pick Qty ({LOC_PICK})", int(total_pick))
                    sc3.metric(f"Allocated ({LOC_ALLOC})", int(total_alloc))
                    sc4.metric("ATS Qty", int(total_ats))
                    sc5.metric("Damage Qty", int(total_dmg))

                    lc1, lc2, lc3, lc4, lc5 = st.columns(5)
                    lc1.metric("L1 · SC20 Picked", cnt['L1'], delta=f"-{cnt['L1x']} unmatched" if cnt['L1x'] else None,
                               delta_color="inverse")
                    lc2.metric("L2 · MPD Match", cnt['L2'])
                    lc3.metric("L3 · Partial Match", cnt['L3'], delta=f"-{cnt['L3x']} unmatched" if cnt['L3x'] else None,
                               delta_color="inverse")
                    lc4.metric("L4 · Damage", cnt['L4'])
                    lc5.metric("L5 · ATS", cnt['L5'])

                    if qty_match:
                        st.success(f"✅ Qty Match: Inventory({int(inv_total_qty)}) = Report({int(rpt_total_qty)})")
                    else:
                        st.warning(f"⚠️ Qty Mismatch: Inventory({int(inv_total_qty)}) ≠ Report({int(rpt_total_qty)}) "
                                   f"| Diff={int(inv_total_qty - rpt_total_qty)}")

                    d_count = int((fmt_df['Status'].astype(str).str.upper() == 'D').sum()) if 'Status' in fmt_df.columns else 0
                    st.info(f"🔖 Status = D : **{d_count}** rows | Status ≠ D : **{len(fmt_df) - d_count}** rows | "
                            f"Total Accounted: **{int(accounted)}** / {int(inv_total_qty)}")

                    # ════════════════════════════════════════════════════════════
                    # STEP 10 — Unmatched sheet (Logic 1 & Logic 3)
                    # ════════════════════════════════════════════════════════════
                    unmatched_df = fmt_df[fmt_df['_logic'].isin(['L1-SC20-Unmatched', 'L3-Unmatched'])] \
                        [['Pallet', 'Location Id', 'Actual Qty', 'Lot Number', 'Invoice Number',
                          'Grn Number', 'Vendor Name', 'Remark']].reset_index(drop=True) \
                        if '_logic' in fmt_df.columns else pd.DataFrame()

                    if unmatched_df.empty:
                        st.success("✅ Unmatched rows නෑ — සියලු SC20 / HKA rows match වුනා.")
                    else:
                        st.warning(f"⚠️ Unmatched: **{len(unmatched_df)}** rows (Unmatched sheet බලන්න).")
                        st.dataframe(unmatched_df.astype(str), use_container_width=True, height=240)

                    st.divider()
                    st.dataframe(report_df.astype(str), use_container_width=True, height=440)

                    # ════════════════════════════════════════════════════════════
                    # STEP 11 — Excel export
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
                        report_with_total.to_excel(writer, sheet_name='Inventory_Details', index=False)
                        wb = writer.book
                        ws = writer.sheets['Inventory_Details']

                        hdr_xl   = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#fff', 'border': 1, 'font_size': 10})
                        pick_xl  = wb.add_format({'bg_color': '#E8F5E9', 'border': 1, 'font_size': 10})
                        alloc_xl = wb.add_format({'bg_color': '#E3F2FD', 'border': 1, 'font_size': 10, 'bold': True})
                        dmg_xl   = wb.add_format({'bg_color': '#FFE0E0', 'border': 1, 'font_size': 10})
                        ats_xl   = wb.add_format({'bg_color': '#FFF3CD', 'border': 1, 'font_size': 10, 'bold': True})
                        vnd_xl   = wb.add_format({'bg_color': '#FFF9C4', 'border': 1, 'font_size': 10})
                        rmk_xl   = wb.add_format({'bg_color': '#FCE4EC', 'border': 1, 'font_size': 10, 'italic': True})
                        norm_xl  = wb.add_format({'border': 1, 'font_size': 10})
                        # ⚡ SPEED FIX: කලින් cell එකකට report_with_total.iloc[ri-1][col]
                        # කියලා pandas scalar lookup එකක් කළා — rows 6000ක් × cols 25ක් =
                        # ~150k lookups → export එකට තත්පර 20+. දැන් column values එක වරක්
                        # list එකකට convert කරලා index කරයි (~10x වේගවත්).
                        #
                        # 🐛 FIX: ඒ speed fix එකේදී .astype(str) කළ නිසා Qty columns
                        # සියල්ලම Excel එකට **text** විදිහට ගියා ("100.0" වගේ) —
                        # SUM/filter වැඩ කළේ නෑ, "number stored as text" warning එකත් ආවා,
                        # තවද පහළ TOTAL row එක විතරක් number විදිහට තිබුනා. දැන් numeric
                        # columns write_number() එකෙන්, අනිත් ඒවා string විදිහට යවයි.
                        NUM_COLS = set(['Actual Qty', 'Pick Quantity', 'Allocated', 'ATS'] + list(damage_remarks))
                        num_xl_cache = {}

                        def _num_fmt(base_fmt_key, props):
                            if base_fmt_key not in num_xl_cache:
                                p2 = dict(props); p2['num_format'] = '#,##0.###'
                                num_xl_cache[base_fmt_key] = wb.add_format(p2)
                            return num_xl_cache[base_fmt_key]

                        _fmt_props = {
                            'pick':  {'bg_color': '#E8F5E9', 'border': 1, 'font_size': 10},
                            'alloc': {'bg_color': '#E3F2FD', 'border': 1, 'font_size': 10, 'bold': True},
                            'dmg':   {'bg_color': '#FFE0E0', 'border': 1, 'font_size': 10},
                            'ats':   {'bg_color': '#FFF3CD', 'border': 1, 'font_size': 10, 'bold': True},
                            'norm':  {'border': 1, 'font_size': 10},
                        }
                        _colvals = {c: report_with_total[c].tolist() for c in report_cols}
                        for ci, col_name in enumerate(report_cols):
                            ws.write(0, ci, col_name, hdr_xl)
                            ws.set_column(ci, ci, 15)
                            _vals = _colvals[col_name]
                            if   col_name in ['Pick Quantity', 'Destination Country', 'Order NO']: cell_fmt, fkey = pick_xl,  'pick'
                            elif col_name == 'Allocated':        cell_fmt, fkey = alloc_xl, 'alloc'
                            elif col_name in damage_remarks:     cell_fmt, fkey = dmg_xl,   'dmg'
                            elif col_name == 'ATS':              cell_fmt, fkey = ats_xl,   'ats'
                            elif col_name == 'Remark':           cell_fmt, fkey = rmk_xl,   None
                            elif col_name in ['Vendor Name', 'COO']: cell_fmt, fkey = vnd_xl, None
                            else:                                cell_fmt, fkey = norm_xl,  'norm'
                            is_num_col = col_name in NUM_COLS and fkey is not None
                            for ri in range(1, len(report_df) + 1):
                                val = _vals[ri - 1]
                                if is_num_col:
                                    n = pd.to_numeric(val, errors='coerce')
                                    if pd.notna(n):
                                        ws.write_number(ri, ci, float(n), _num_fmt(fkey, _fmt_props[fkey]))
                                        continue
                                ws.write(ri, ci, sv(val), cell_fmt)
                        tot_idx = len(report_df) + 1
                        tot_num = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#FFD700', 'border': 1, 'font_size': 10, 'num_format': '#,##0'})
                        tot_str = wb.add_format({'bold': True, 'bg_color': '#1a1a1a', 'font_color': '#FFD700', 'border': 1, 'font_size': 10})
                        _lastrow = report_with_total.iloc[-1]
                        for ci, col_name in enumerate(report_cols):
                            v = _lastrow[col_name]
                            try:    ws.write(tot_idx, ci, float(v) if v != '' else '', tot_num)
                            except: ws.write(tot_idx, ci, str(v), tot_str)
                        ws.freeze_panes(1, 0)

                        # ── Summary sheet ──
                        ws_s = wb.add_worksheet('Summary')
                        b_xl  = wb.add_format({'bold': True, 'font_size': 11})
                        v_xl  = wb.add_format({'font_size': 11, 'num_format': '#,##0'})
                        ok_xl = wb.add_format({'bold': True, 'font_color': '#27ae60', 'font_size': 11})
                        er_xl = wb.add_format({'bold': True, 'font_color': '#e74c3c', 'font_size': 11})
                        ws_s.set_column(0, 0, 34); ws_s.set_column(1, 1, 18)
                        summ = [
                            ('Inventory Total Actual Qty', int(inv_total_qty)),
                            ('Report Total Actual Qty', int(rpt_total_qty)),
                            ('Qty Match', 'YES' if qty_match else 'NO'), ('', ''),
                            (f'Pick Quantity ({LOC_PICK})', int(total_pick)),
                            (f'Allocated Qty ({LOC_ALLOC})', int(total_alloc)),
                            ('ATS Quantity', int(total_ats)),
                            ('Damage Quantity', int(total_dmg)),
                            ('Total Accounted', int(accounted)),
                            ('Unaccounted', int(inv_total_qty - accounted)), ('', ''),
                            ('Logic 1 — SC20 Picked (matched)', cnt['L1']),
                            ('Logic 1 — SC20 Unmatched', cnt['L1x']),
                            ('Logic 2 — master_pick_data match', cnt['L2']),
                            ('Logic 3 — master_partial_data match', cnt['L3']),
                            ('Logic 3 — Unmatched', cnt['L3x']),
                            ('Logic 4 — Damage', cnt['L4']),
                            ('Logic 5 — ATS (Others)', cnt['L5']), ('', ''),
                            ('Blank Fill — via Pallet', fill_counts.get('A-Pallet', 0)),
                            ('Blank Fill — via Gen Pallet ID', fill_counts.get('B-GenPallet', 0)),
                            ('Blank Fill — via Pattern', fill_counts.get('C-Pattern', 0)),
                            ('Blank Fill — Total Cells', fill_total),
                            ('Still Blank Cells', still_blank), ('', ''),
                            ('Total Report Lines', len(report_df)),
                            ('Status = D Lines', d_count),
                            ('Status <> D Lines', len(report_df) - d_count),
                        ]
                        for ri, (lab, val) in enumerate(summ):
                            ws_s.write(ri, 0, lab, b_xl)
                            if isinstance(val, int): ws_s.write(ri, 1, val, v_xl)
                            elif val == 'YES': ws_s.write(ri, 1, val, ok_xl)
                            elif val == 'NO':  ws_s.write(ri, 1, val, er_xl)
                            else: ws_s.write(ri, 1, val)

                        # ── Unmatched sheet ──
                        ws_u = wb.add_worksheet('Unmatched')
                        u_hdr = wb.add_format({'bold': True, 'bg_color': '#e74c3c', 'font_color': '#fff', 'border': 1, 'font_size': 10})
                        u_ok  = wb.add_format({'bold': True, 'bg_color': '#27ae60', 'font_color': '#fff', 'font_size': 11})
                        u_row = wb.add_format({'border': 1, 'font_size': 10})
                        if not unmatched_df.empty:
                            for ci, colu in enumerate(unmatched_df.columns):
                                ws_u.write(0, ci, colu, u_hdr); ws_u.set_column(ci, ci, 22)
                            for ri2, row2 in unmatched_df.iterrows():
                                for ci2, val2 in enumerate(row2):
                                    ws_u.write(ri2 + 1, ci2, str(val2), u_row)
                        else:
                            ws_u.write(0, 0, '✅ No Unmatched Rows', u_ok)
                            ws_u.set_column(0, 0, 40)

                        # ── Blank_Fill sheet (Invoice / GRN / Supplier Hu audit) ──
                        ws_f = wb.add_worksheet('Blank_Fill')
                        f_hdr = wb.add_format({'bold': True, 'bg_color': '#2980b9', 'font_color': '#fff', 'border': 1, 'font_size': 10})
                        f_row = wb.add_format({'border': 1, 'font_size': 10})
                        f_ok  = wb.add_format({'bold': True, 'bg_color': '#27ae60', 'font_color': '#fff', 'font_size': 11})
                        if not fill_log_df.empty:
                            for ci, colf in enumerate(fill_log_df.columns):
                                ws_f.write(0, ci, colf, f_hdr); ws_f.set_column(ci, ci, 24)
                            for ri3, row3 in fill_log_df.iterrows():
                                for ci3, val3 in enumerate(row3):
                                    ws_f.write(ri3 + 1, ci3, str(val3), f_row)
                        else:
                            ws_f.write(0, 0, 'No blank cells were filled', f_ok)
                            ws_f.set_column(0, 0, 44)

                    st.download_button("⬇️ Download Inventory Details Report", data=out_fmt.getvalue(),
                        file_name=f"Inventory_Details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.ms-excel", use_container_width=True, type="primary")
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
                                    # 🐛 FIX: හිස් cell එකක් str() කරද්දී 'nan' කියන
                                    # string එක DB එකට ගියා. sv() එකෙන් blank වේ.
                                    'Pallet':     sv(row.get(pallet_col, '')),
                                    'Actual Qty': sv(row.get(qty_col, '')),
                                    'Remark':     (sv(row.get(remark_col, '')) or 'Damage') if remark_col else 'Damage',
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
                            # 🐛 FIX: pandas nullable dtypes එකෙන් එන '<NA>' / 'NaT'
                            # tokens කලින් check එකේ තිබුනේ නෑ — ඒවා archive එකට
                            # real value විදිහට ගියා. sv() එකෙන් ඔක්කොම cover වේ.
                            val = r.get(app_col, None)
                            db_row[db_col] = None if sv(val) == '' else val
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
                                # 🐛 FIX: write එක fail වුනොත් delete වුනේ නෑ — success
                                # message එකක් පෙන්නන්නත්, ඒ rows archive කරන්නත් බෑ.
                                if DBManager._overwrite_table("master_pick_data", filtered_master):
                                    st.success(f"✅ {deleted_count} records deleted from Master_Pick_Data!")
                                else:
                                    deleted_pick_rows = pd.DataFrame()
                                    st.error("❌ Master_Pick_Data delete fail වුනා — කිසිවක් වෙනස් වුනේ නෑ.")
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
                                if DBManager._overwrite_table("master_partial_data", filtered_mpart):
                                    st.success(f"✅ {len(deleted_partial_rows)} records deleted from Master_Partial_Data!")
                                else:
                                    deleted_partial_rows = pd.DataFrame()
                                    st.error("❌ Master_Partial_Data delete fail වුනා — කිසිවක් වෙනස් වුනේ නෑ.")

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
                            if not DBManager._overwrite_table("master_pick_data", filtered):
                                deleted_pick_rows2, deleted_pick2 = pd.DataFrame(), 0
                                st.error("❌ Master_Pick_Data delete fail වුනා — කිසිවක් වෙනස් වුනේ නෑ.")

                        # ── Also delete from master_partial_data by Load ID ──
                        mpart_df2 = DBManager.read_table("master_partial_data", force=True)
                        deleted_partial_rows2 = pd.DataFrame()
                        if not mpart_df2.empty and 'Load ID' in mpart_df2.columns:
                            mpart_mask2           = mpart_df2['Load ID'].astype(str).str.strip().isin(load_ids_to_delete)
                            deleted_partial_rows2 = mpart_df2[mpart_mask2]
                            if not deleted_partial_rows2.empty:
                                if DBManager._overwrite_table("master_partial_data", mpart_df2[~mpart_mask2]):
                                    st.success(f"✅ {len(deleted_partial_rows2)} records deleted from Master_Partial_Data!")
                                else:
                                    deleted_partial_rows2 = pd.DataFrame()
                                    st.error("❌ Master_Partial_Data delete fail වුනා — කිසිවක් වෙනස් වුනේ නෑ.")

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
                                if not DBManager._overwrite_table("master_pick_data", filtered_batch):
                                    deleted_batch_pick_rows, deleted_batch = pd.DataFrame(), 0
                                    st.error("❌ Master_Pick_Data delete fail වුනා — කිසිවක් වෙනස් වුනේ නෑ.")

                            # ── Also delete from master_partial_data by Batch ID ──
                            mpart_df3 = DBManager.read_table("master_partial_data", force=True)
                            deleted_batch_partial_rows = pd.DataFrame()
                            if not mpart_df3.empty and 'Batch ID' in mpart_df3.columns:
                                mpart_mask3                = mpart_df3['Batch ID'].astype(str).str.strip() == str(del_batch_id).strip()
                                deleted_batch_partial_rows = mpart_df3[mpart_mask3]
                                if not deleted_batch_partial_rows.empty:
                                    if DBManager._overwrite_table("master_partial_data", mpart_df3[~mpart_mask3]):
                                        st.success(f"✅ {len(deleted_batch_partial_rows)} records deleted from Master_Partial_Data!")
                                    else:
                                        deleted_batch_partial_rows = pd.DataFrame()
                                        st.error("❌ Master_Partial_Data delete fail වුනා — කිසිවක් වෙනස් වුනේ නෑ.")

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

                            # 🐛 FIX: කලින් match වුනේ 0 rows වුනත් මුළු table එකම
                            # delete → re-insert වුනා (අනවශ්‍ය risk එකක්). දැන්
                            # ඇත්තටම rows match වුනොත් විතරයි rewrite කරන්නේ, තවද
                            # write එක fail වුනොත් ඒක report වේ.
                            def _apply_delete(table, filtered_df, deleted_n, label, matched_rows):
                                if deleted_n <= 0:
                                    results.append(f"{label}: 0 records")
                                    return pd.DataFrame()
                                if DBManager._overwrite_table(table, filtered_df):
                                    results.append(f"{label}: {deleted_n} records")
                                    return matched_rows
                                results.append(f"{label}: ❌ delete failed")
                                return pd.DataFrame()

                            mpd_fresh = DBManager.read_table("master_pick_data", force=True)
                            if not mpd_fresh.empty and 'Pallet' in mpd_fresh.columns:
                                mask_p4              = mpd_fresh['Pallet'].astype(str).str.strip() == str(del_pallet).strip()
                                filtered_mpd         = mpd_fresh[~mask_p4]
                                del_pallet_pick_rows = _apply_delete(
                                    "master_pick_data", filtered_mpd,
                                    len(mpd_fresh) - len(filtered_mpd),
                                    "Master_Pick_Data", mpd_fresh[mask_p4])

                            mpart_fresh = DBManager.read_table("master_partial_data", force=True)
                            if not mpart_fresh.empty:
                                # 🐛 FIX: pd.Series([True]*n) එකට default RangeIndex
                                # එකක් එනවා — df එකේ index එක වෙනස් නම් `&=` එකෙන්
                                # misalign වෙලා NaN/wrong mask එකක් හැදෙනවා.
                                mask = pd.Series(True, index=mpart_fresh.index)
                                if 'Pallet'        in mpart_fresh.columns: mask &= mpart_fresh['Pallet'].astype(str).str.strip()        != str(del_pallet).strip()
                                if 'Gen Pallet ID' in mpart_fresh.columns: mask &= mpart_fresh['Gen Pallet ID'].astype(str).str.strip() != str(del_pallet).strip()
                                filtered_mpart          = mpart_fresh[mask]
                                del_pallet_partial_rows = _apply_delete(
                                    "master_partial_data", filtered_mpart,
                                    len(mpart_fresh) - len(filtered_mpart),
                                    "Master_Partial_Data", mpart_fresh[~mask])

                            dmg_fresh = DBManager.read_table("damage_items", force=True)
                            if not dmg_fresh.empty and 'Pallet' in dmg_fresh.columns:
                                filtered_dmg = dmg_fresh[dmg_fresh['Pallet'].astype(str).str.strip() != str(del_pallet).strip()]
                                _apply_delete("damage_items", filtered_dmg,
                                              len(dmg_fresh) - len(filtered_dmg), "Damage_Items",
                                              pd.DataFrame())

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

        # ══════════════════════════════════════════════════════════════════════
        # 🔒 ADMIN PASSWORD GATE
        #   Password එක st.secrets["admin"]["password"] එකෙන් ගනී.
        #   Secrets එකේ නැත්නම් default එක use වේ.
        #   ⚠️ Code එකේ තියෙන password එකක් real security එකක් නෙවෙයි — repo එකට
        #   access තියෙන ඕනෑම කෙනෙකුට පේනවා. Streamlit Cloud → App settings →
        #   Secrets එකේ මේක දාන්න:
        #       [admin]
        #       password = "Isha@1996"
        # ══════════════════════════════════════════════════════════════════════
        try:
            ADMIN_PASSWORD = st.secrets["admin"]["password"]
        except Exception:
            ADMIN_PASSWORD = "Isha@1996"

        if not st.session_state.get('admin_unlocked', False):
            st.warning("🔒 මෙම කොටස protected — Admin password එක ඇතුලත් කරන්න.")
            _pw_c1, _pw_c2, _pw_c3 = st.columns([1, 1.4, 1])
            with _pw_c2:
                adm_pw = st.text_input("Admin Password", type="password", key="adm_pw_input")
                if st.button("🔓 Unlock", type="primary", use_container_width=True, key="adm_unlock_btn"):
                    if adm_pw == ADMIN_PASSWORD:
                        st.session_state['admin_unlocked'] = True
                        st.success("✅ Unlocked!")
                        st.rerun()
                    else:
                        st.error("❌ වැරදි password එකක්!")
            st.stop()

        _lk1, _lk2 = st.columns([5, 1])
        _lk1.caption("🔓 Admin section unlocked.")
        if _lk2.button("🔒 Lock", use_container_width=True, key="adm_lock_btn"):
            st.session_state['admin_unlocked'] = False
            st.rerun()

        # ── Login අයින් කළ නිසා "User Management" tab එක ඉවත් කළා ──
        adm_tab2, adm_tab3, adm_tab4 = st.tabs(
            ["🗄️ Database Management", "📜 Old History", "⬇️ Full Data Export"])

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
                                            db_row[db_col] = sv(val) or None
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

        # ══════════════════════════════════════════════════════════════════════
        # ⬇️ FULL DATA EXPORT — System DB එකේ සියලු tables එක Excel එකකට
        # ══════════════════════════════════════════════════════════════════════
        with adm_tab4:
            st.subheader("⬇️ Full Database Export")
            st.caption("System එකේ DB tables වල සම්පූර්ණ data එකක් download කරගන්න. "
                       "Excel එකේ table එකකට sheet එකක් බැගින් එනවා.")

            ALL_DB_TABLES = [
                "master_pick_data", "master_partial_data", "load_history", "summary_data",
                "damage_items", "vendor_maintain", "inventory_status",
                "old_history", "old_history_master",
            ]
            SHEET_NAME_MAP = {
                "master_pick_data":    "Master_Pick_Data",
                "master_partial_data": "Master_Partial_Data",
                "load_history":        "Load_History",
                "summary_data":        "Summary_Data",
                "damage_items":        "Damage_Items",
                "vendor_maintain":     "Vendor_Maintain",
                "inventory_status":    "Inventory_Status",
                "old_history":         "Old_History",
                "old_history_master":  "Old_History_Master",
            }
            EXCEL_ROW_LIMIT = 1_048_575        # header row එකට 1ක් අඩු කරලා

            exp_c1, exp_c2 = st.columns([3, 1])
            sel_tables = exp_c1.multiselect(
                "Export කරන Tables:", ALL_DB_TABLES, default=ALL_DB_TABLES,
                key="exp_tables")
            exp_fmt = exp_c2.radio("Format", ["Excel (.xlsx)", "CSV (.zip)"], key="exp_fmt")

            st.caption("⚡ Table ලොකු නම් Excel එක හදන්න වෙලාවක් යනවා — ඒ වගේ වෙලාවට "
                       "**CSV (.zip)** වේගවත් සහ row limit එකකුත් නෑ.")

            # ── Row counts (quick preview — මුළු table එක download කරන්නේ නෑ) ──
            if st.button("🔢 Row Counts බලන්න", key="exp_counts_btn"):
                with st.spinner("Counting..."):
                    cnt_rows = []
                    for t in sel_tables:
                        try:
                            cnt_rows.append({'Table': t,
                                             'Rows': DBManager.count_rows(t, force=True)})
                        except Exception as _ce:
                            cnt_rows.append({'Table': t, 'Rows': f'error: {_ce}'})
                if cnt_rows:
                    cdf = pd.DataFrame(cnt_rows)
                    st.dataframe(cdf.astype(str), use_container_width=True)
                    try:
                        st.metric("Total Rows", f"{int(pd.to_numeric(cdf['Rows'], errors='coerce').fillna(0).sum()):,}")
                    except Exception:
                        pass

            st.divider()

            if not sel_tables:
                st.info("Export කරන්න table එකක් හෝ තෝරන්න.")
            elif st.button("📦 Generate Full Export", type="primary",
                           use_container_width=True, key="exp_gen_btn"):

                exported, skipped, total_rows = {}, [], 0
                prog = st.progress(0.0, text="Starting...")

                for i, t in enumerate(sel_tables):
                    prog.progress(i / max(len(sel_tables), 1), text=f"Reading **{t}** ...")
                    try:
                        # force=True → cache නොව අලුත්ම data එක
                        df_t = DBManager.read_table(t, force=True)
                    except Exception as _re_:
                        skipped.append((t, str(_re_)))
                        continue
                    if df_t is None or df_t.empty:
                        skipped.append((t, "empty / no rows"))
                        continue
                    exported[t] = df_t
                    total_rows += len(df_t)

                prog.progress(1.0, text="Building file...")

                if not exported:
                    prog.empty()
                    st.warning("⚠️ Export කරන්න data නෑ.")
                    if skipped:
                        st.dataframe(pd.DataFrame(
                            [{'Table': a, 'Reason': b} for a, b in skipped]).astype(str),
                            use_container_width=True)
                else:
                    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                    if exp_fmt.startswith("CSV"):
                        # ── ZIP of CSVs ──
                        import zipfile
                        zbuf = io.BytesIO()
                        with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as zf:
                            for t, df_t in exported.items():
                                zf.writestr(f"{t}.csv", df_t.to_csv(index=False))
                            zf.writestr("_export_info.txt",
                                        f"Exported: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
                                        + "\n".join(f"{t}: {len(d)} rows"
                                                    for t, d in exported.items()))
                        prog.empty()
                        st.success(f"✅ Tables **{len(exported)}** · Rows **{total_rows:,}** ready.")
                        st.download_button(
                            "⬇️ Download Full DB Export (ZIP)", data=zbuf.getvalue(),
                            file_name=f"WMS_Full_DB_{stamp}.zip", mime="application/zip",
                            use_container_width=True, type="primary")
                    else:
                        # ── Single Excel workbook, sheet per table ──
                        xbuf = io.BytesIO()
                        truncated = []
                        with pd.ExcelWriter(xbuf, engine='xlsxwriter') as writer:
                            wb_e = writer.book
                            hdr_e = wb_e.add_format({'bold': True, 'bg_color': '#1a1a1a',
                                                     'font_color': '#fff', 'border': 1,
                                                     'font_size': 10})
                            # ── Index sheet (මුලින්ම) ──
                            ws_i = wb_e.add_worksheet('_Index')
                            b_e = wb_e.add_format({'bold': True, 'font_size': 11})
                            n_e = wb_e.add_format({'font_size': 11, 'num_format': '#,##0'})
                            ws_i.set_column(0, 0, 30); ws_i.set_column(1, 1, 26)
                            ws_i.set_column(2, 2, 14)
                            ws_i.write(0, 0, 'Table', hdr_e)
                            ws_i.write(0, 1, 'Sheet', hdr_e)
                            ws_i.write(0, 2, 'Rows', hdr_e)
                            for ri, (t, df_t) in enumerate(exported.items(), 1):
                                ws_i.write(ri, 0, t, b_e)
                                ws_i.write(ri, 1, SHEET_NAME_MAP.get(t, t)[:31])
                                ws_i.write(ri, 2, len(df_t), n_e)
                            _last = len(exported) + 2
                            ws_i.write(_last, 0, 'Exported At', b_e)
                            ws_i.write(_last, 1, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            ws_i.write(_last + 1, 0, 'Total Rows', b_e)
                            ws_i.write(_last + 1, 1, total_rows, n_e)

                            for t, df_t in exported.items():
                                sheet = re.sub(r'[\[\]\:\*\?\/\\]', '_',
                                               SHEET_NAME_MAP.get(t, t))[:31]
                                out_t = df_t
                                if len(df_t) > EXCEL_ROW_LIMIT:
                                    out_t = df_t.head(EXCEL_ROW_LIMIT)
                                    truncated.append((t, len(df_t)))
                                out_t.astype(str).to_excel(writer, sheet_name=sheet, index=False)
                                ws_t = writer.sheets[sheet]
                                for ci, cn in enumerate(out_t.columns):
                                    ws_t.write(0, ci, str(cn), hdr_e)
                                    ws_t.set_column(ci, ci, 16)
                                ws_t.freeze_panes(1, 0)

                        prog.empty()
                        st.success(f"✅ Tables **{len(exported)}** · Rows **{total_rows:,}** ready.")
                        if truncated:
                            st.warning("⚠️ Excel එකේ sheet එකකට rows 1,048,575ක සීමාවක් තියෙනවා. "
                                       "මේ tables truncate වුනා — සම්පූර්ණ data එකට **CSV (.zip)** "
                                       f"use කරන්න: {', '.join(f'{a} ({b:,})' for a, b in truncated)}")
                        st.download_button(
                            "⬇️ Download Full DB Export (Excel)", data=xbuf.getvalue(),
                            file_name=f"WMS_Full_DB_{stamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True, type="primary")

                    summ_c1, summ_c2 = st.columns(2)
                    summ_c1.dataframe(pd.DataFrame(
                        [{'Table': t, 'Rows': len(d)} for t, d in exported.items()]).astype(str),
                        use_container_width=True)
                    if skipped:
                        summ_c2.dataframe(pd.DataFrame(
                            [{'Skipped Table': a, 'Reason': b} for a, b in skipped]).astype(str),
                            use_container_width=True)
                    show_confetti()




footer_branding()
