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
    'Invoice Number1': 'invoice_number1', 'Transaction': 'transaction', 'Order Type': 'order_type',
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
    }
    _COL_MAP_REV = {
        "master_pick_data":    PICK_COL_MAP_REV,
        "master_partial_data": PARTIAL_COL_MAP_REV,
        "load_history":        HISTORY_COL_MAP_REV,
        "summary_data":        SUMMARY_COL_MAP_REV,
        "damage_items":        DAMAGE_COL_MAP_REV,
        "users":               USERS_COL_MAP_REV,
        "vendor_maintain":     VENDOR_COL_MAP_REV,
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
                        if not id_list:
                            return

                        STATUS_STYLES = {
                            'Pending':    ('var(--color-background-warning)', 'var(--color-text-warning)'),
                            'PL Pending': ('#fff0d6',                         '#7a4f00'),
                            'Processing': ('var(--color-background-info)',    'var(--color-text-info)'),
                            'Completed':  ('var(--color-background-success)', 'var(--color-text-success)'),
                            'Cancelled':  ('var(--color-background-danger)',  'var(--color-text-danger)'),
                        }

                        # ── Header row ────────────────────────────────────────────────────────
                        st.markdown(f'''
<div style="display:grid;grid-template-columns:2.5fr 1.8fr 0.8fr 1fr 0.7fr 1fr 0.6fr 0.7fr;
            background:{category_color}18;border:1px solid var(--color-border-tertiary);
            border-radius:8px 8px 0 0;border-bottom:2px solid {category_color};
            padding:6px 12px;font-size:11px;font-weight:600;
            color:var(--color-text-secondary);gap:6px;margin-bottom:0;">
  <div>Load ID</div><div>Update Status</div>
  <div>SO</div><div>Country</div><div>Ship</div><div>Date</div>
  <div style="text-align:right;">Lines</div>
  <div style="text-align:right;">Pick Qty</div>
</div>''', unsafe_allow_html=True)

                        # ── One row per load ID — pure Streamlit columns ──────────────────────
                        for i, lid in enumerate(id_list):
                            load_row     = active_loads[active_loads['Generated Load ID'] == lid].iloc[0]
                            status       = str(load_row.get('Pick Status', 'Pending'))
                            so_num       = str(load_row.get('SO Number', '-'))
                            country      = str(load_row.get('Country Name', '-'))
                            ship         = str(load_row.get('SHIP MODE', '-'))
                            date         = str(load_row.get('Date', '-'))[:10]
                            lid_key      = str(lid).strip()
                            pick_count   = pick_counts_by_lid.get(lid_key, 0)
                            pick_qty_val = pick_qty_by_lid.get(lid_key, 0)
                            s_data       = summ_by_load.get(str(lid), {})
                            variance     = s_data.get('variance', 0)
                            requested    = s_data.get('requested', 0)
                            picked_q     = s_data.get('picked', 0)
                            fill_pct     = min(int((picked_q / requested * 100)) if requested > 0 else 0, 100)

                            shortage_tag = (f'<span style="font-size:10px;background:#fde8e8;color:#c0392b;'
                                            f'padding:1px 5px;border-radius:4px;margin-left:5px;font-weight:600;">'
                                            f'-{int(variance)}</span>') if variance > 0 else ''
                            row_bg = '#f9f9f9' if i % 2 == 1 else '#ffffff'
                            border_bottom = '1px solid var(--color-border-tertiary)'

                            # c_lid | c_sel | c_save | c_so | c_country | c_ship | c_date | c_lines | c_qty
                            c_lid, c_sel, c_save, c_so, c_country, c_ship, c_date, c_lines, c_qty = st.columns(
                                [2.5, 1.4, 0.4, 0.8, 1.0, 0.7, 1.0, 0.6, 0.7]
                            )

                            with c_lid:
                                st.markdown(f'''
<div style="padding:4px 0 2px 0;">
  <div style="font-size:12px;font-weight:600;color:var(--color-text-primary);">{lid}{shortage_tag}</div>
  <div style="height:3px;background:#e0e0e0;border-radius:2px;margin-top:3px;margin-bottom:1px;">
    <div style="height:3px;width:{fill_pct}%;background:{category_color};border-radius:2px;"></div>
  </div>
  <div style="font-size:9px;color:var(--color-text-secondary);">{fill_pct}% picked</div>
</div>''', unsafe_allow_html=True)

                            with c_sel:
                                safe_idx = STATUS_OPTIONS.index(status) if status in STATUS_OPTIONS else 0
                                new_st = st.selectbox(
                                    f"_sel_{lid}", STATUS_OPTIONS, index=safe_idx,
                                    key=f"st_{lid}", label_visibility="collapsed"
                                )

                            with c_save:
                                st.markdown("<div style='margin-top:4px;'></div>", unsafe_allow_html=True)
                                if st.button("💾", key=f"upd_{lid}", use_container_width=True, help="Save"):
                                    try:
                                        ok = DBManager.update_cell("load_history", "Generated Load ID", str(lid), "Pick Status", new_st)
                                        if ok and new_st == "Cancelled":
                                            mpd = DBManager.read_table("master_pick_data")
                                            lid_col_c = next((c for c in mpd.columns if str(c).strip().lower() == 'load id'), None)
                                            if not mpd.empty and lid_col_c:
                                                filtered_mpd = mpd[mpd[lid_col_c].astype(str).str.strip() != str(lid).strip()]
                                                DBManager._overwrite_table("master_pick_data", filtered_mpd)
                                            st.success(f"✅ {lid} → Cancelled")
                                        elif ok:
                                            st.success(f"✅ {lid} → {new_st}")
                                        st.rerun()
                                    except Exception as ex:
                                        st.error(f"Error: {ex}")

                            with c_so:
                                st.markdown(f'<div style="font-size:12px;font-weight:500;padding-top:6px;">{so_num}</div>', unsafe_allow_html=True)
                            with c_country:
                                st.markdown(f'<div style="font-size:12px;padding-top:6px;">{country}</div>', unsafe_allow_html=True)
                            with c_ship:
                                st.markdown(f'<div style="font-size:12px;padding-top:6px;">{ship}</div>', unsafe_allow_html=True)
                            with c_date:
                                st.markdown(f'<div style="font-size:11px;color:var(--color-text-secondary);padding-top:6px;">{date}</div>', unsafe_allow_html=True)
                            with c_lines:
                                st.markdown(f'<div style="font-size:12px;font-weight:500;text-align:right;padding-top:6px;">{pick_count}</div>', unsafe_allow_html=True)
                            with c_qty:
                                st.markdown(f'<div style="font-size:12px;font-weight:500;text-align:right;padding-top:6px;">{int(pick_qty_val)}</div>', unsafe_allow_html=True)

                            st.markdown('<hr style="margin:0;border:none;border-top:1px solid var(--color-border-tertiary);">', unsafe_allow_html=True)

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
                st.caption("L1: Pick Id filled → MPD (pallet+qty) exact match → Pick Qty update | L2: L1 unmatch → Partial (gen_pallet_id+partial_qty) exact match → Pick Qty update → delete from partial → archive | L3: Pick Id=0 → MPD exact match → Allocated update [remark=Partial → route to L4] | L4: L3 unmatch → Partial pallet match (excl. L1/L2/L3 used gen_pallets) → Allocated + ATS balance | L5: Remaining → ATS | Damage preserved")

                # ── CSV Fallback Upload Section ──────────────────────────────────────────
                with st.expander("📂 CSV Data Override (Optional — Supabase connect නොවේ නම් CSV upload කරන්න)", expanded=False):
                    st.caption("Supabase connection ඇත්නම් CSV upload නොකළත් හරි. CSV upload කළහොත් ඒ data priority ලැබේ.")
                    col_csv1, col_csv2 = st.columns(2)
                    col_csv3, col_csv4 = st.columns(2)
                    csv_pick_file    = col_csv1.file_uploader("master_pick_data CSV",    type=['csv'], key="fmt_csv_pick")
                    csv_partial_file = col_csv2.file_uploader("master_partial_data CSV", type=['csv'], key="fmt_csv_partial")
                    csv_damage_file  = col_csv3.file_uploader("damage_items CSV",        type=['csv'], key="fmt_csv_damage")
                    csv_vendor_file  = col_csv4.file_uploader("vendor_maintain CSV",     type=['csv'], key="fmt_csv_vendor")

                if st.button("📊 Generate Formatted Pick Report", type="primary", use_container_width=True, key="gen_fmt"):
                    with st.spinner("Generating Formatted Report..."):
                        inv_report_file.seek(0)
                        inv_data = pd.read_csv(inv_report_file, keep_default_na=False, na_values=['']) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file, keep_default_na=False, na_values=[''])
                        inv_data.columns = [str(c).strip() for c in inv_data.columns]
                        inv_col_map_r = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}

                        CANONICAL = ['Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                                     'Client So', 'Pallet', 'Supplier Hu', 'Supplier', 'Lot Number',
                                     'Style', 'Color', 'Size', 'Inventory Type', 'Actual Qty', 'Pick Id']
                        rename_map = {}
                        for canon in CANONICAL:
                            matched = inv_col_map_r.get(canon.strip().lower())
                            if matched and matched != canon:
                                rename_map[matched] = canon
                        if rename_map:
                            inv_data = inv_data.rename(columns=rename_map)
                        inv_col_map_r = {str(c).strip().lower(): str(c).strip() for c in inv_data.columns}
                        _inv_aq_col  = inv_col_map_r.get('actual qty',  'Actual Qty')
                        _inv_pal_col = inv_col_map_r.get('pallet',      'Pallet')
                        _inv_pid_col = inv_col_map_r.get('pick id',     'Pick Id')

                        REPORT_HEADERS = ['Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                                          'Client So', 'Pallet', 'Supplier Hu', 'Supplier',
                                          'Lot Number', 'Style', 'Color', 'Size', 'Client So 2',
                                          'Inventory Type', 'Actual Qty']

                        def _load_csv_with_rev_map(uploaded_file, rev_col_map):
                            try:
                                df = pd.read_csv(uploaded_file)
                                df.drop(columns=[c for c in ['id', 'created_at'] if c in df.columns], inplace=True, errors='ignore')
                                df.rename(columns=rev_col_map, inplace=True)
                                return df
                            except Exception as _e:
                                st.warning(f"CSV read error: {_e}")
                                return pd.DataFrame()

                        # ── Load DB / CSV data ─────────────────────────────────────────────
                        if csv_pick_file:
                            csv_pick_file.seek(0)
                            mpd_df = _load_csv_with_rev_map(csv_pick_file, PICK_COL_MAP_REV)
                            st.info("ℹ️ master_pick_data: CSV data use කරයි")
                        else:
                            try:    mpd_df = DBManager.read_table("master_pick_data")
                            except: mpd_df = pd.DataFrame()

                        if csv_partial_file:
                            csv_partial_file.seek(0)
                            _partial_df_fmt = _load_csv_with_rev_map(csv_partial_file, PARTIAL_COL_MAP_REV)
                            st.info("ℹ️ master_partial_data: CSV data use කරයි")
                        else:
                            try:    _partial_df_fmt = DBManager.read_table("master_partial_data")
                            except: _partial_df_fmt = pd.DataFrame()

                        if csv_damage_file:
                            csv_damage_file.seek(0)
                            _damage_df_fmt = _load_csv_with_rev_map(csv_damage_file, DAMAGE_COL_MAP_REV)
                            st.info("ℹ️ damage_items: CSV data use කරයි")
                        else:
                            try:    _damage_df_fmt = DBManager.read_table("damage_items")
                            except: _damage_df_fmt = pd.DataFrame()

                        if csv_vendor_file:
                            csv_vendor_file.seek(0)
                            _vendor_df_fmt = _load_csv_with_rev_map(csv_vendor_file, VENDOR_COL_MAP_REV)
                            st.info("ℹ️ vendor_maintain: CSV data use කරයි")
                        else:
                            try:    _vendor_df_fmt = DBManager.read_table("vendor_maintain")
                            except: _vendor_df_fmt = pd.DataFrame()

                        mpd_col = {str(c).strip().lower(): str(c).strip() for c in (mpd_df.columns if not mpd_df.empty else [])}

                        # ── old_history_master / old_history / master_partial_data lookup ───
                        # Priority: master_pick_data → old_history_master → old_history → master_partial_data
                        # (Logic.txt Additional: blank fill sequence)
                        OH_FILL_COLS = ['Vendor Name', 'Invoice Number', 'Fifo Date', 'Grn Number',
                                        'Client So', 'Supplier Hu', 'Supplier', 'Lot Number',
                                        'Style', 'Color', 'Size', 'Client So 2']
                        OH_COL_DB_MAP = {
                            'Vendor Name': 'vendor_name', 'Invoice Number': 'invoice_number',
                            'Fifo Date': 'fifo_date', 'Grn Number': 'grn_number',
                            'Client So': 'client_so', 'Supplier Hu': 'supplier_hu',
                            'Supplier': 'supplier', 'Lot Number': 'lot_number',
                            'Style': 'style', 'Color': 'color', 'Size': 'size',
                            'Client So 2': 'client_so_2',
                        }

                        # ── Source 1: old_history_master (primary pallet reference) ──────────
                        oh_master_lookup = {}
                        try:
                            _oh_master_df = DBManager.read_table("old_history_master")
                            if not _oh_master_df.empty:
                                # Rename db columns back to app names
                                _ohm_col_map_rev = {v: k for k, v in OH_COL_DB_MAP.items()}
                                _oh_master_df_r = _oh_master_df.rename(columns=_ohm_col_map_rev)
                                for _, _oh_r in _oh_master_df_r.iterrows():
                                    _oh_pal = str(_oh_r.get('Pallet', _oh_r.get('pallet', ''))).strip().lower()
                                    if _oh_pal:
                                        _oh_entry = {}
                                        for _col_d in OH_FILL_COLS:
                                            _v = str(_oh_r.get(_col_d, '')).strip()
                                            if _v and _v not in ('nan', 'None'):
                                                _oh_entry[_col_d] = _v
                                        if _oh_entry:
                                            oh_master_lookup[_oh_pal] = _oh_entry
                        except Exception:
                            pass

                        # ── Source 2: old_history table (fallback) ───────────────────────────
                        oh_history_lookup = {}
                        try:
                            _oh_hist_df = DBManager.read_table("old_history")
                            if not _oh_hist_df.empty:
                                _ohhc = {str(c).strip().lower(): str(c).strip() for c in _oh_hist_df.columns}
                                for _, _oh_r in _oh_hist_df.iterrows():
                                    _oh_pal = str(_oh_r.get(_ohhc.get('pallet','pallet'), '')).strip().lower()
                                    if _oh_pal:
                                        _oh_entry = {}
                                        for _col_d in OH_FILL_COLS:
                                            _db_k = OH_COL_DB_MAP.get(_col_d, _col_d.lower().replace(' ','_'))
                                            _v = str(_oh_r.get(_ohhc.get(_db_k, _col_d), '')).strip()
                                            if _v and _v not in ('nan', 'None'):
                                                _oh_entry[_col_d] = _v
                                        _ord = str(_oh_r.get(_ohhc.get('generated_load_id', 'generated_load_id'), '')).strip()
                                        if _ord and _ord not in ('nan', 'None'):
                                            _oh_entry['Order NO'] = _ord
                                        if _oh_entry and _oh_pal not in oh_history_lookup:
                                            oh_history_lookup[_oh_pal] = _oh_entry
                        except Exception:
                            pass

                        # ── Country lookup maps: pallet/gen_pallet_id → country_name ─────────
                        # Built from every available DB so Destination Country can be filled later
                        country_lookup = {}   # key: pallet or gen_pallet_id (lower) → country string

                        # From master_pick_data (pallet → country)
                        if not mpd_df.empty:
                            _mpd_cn = mpd_col.get('country name', 'Country Name')
                            _mpd_pc = mpd_col.get('pallet', 'Pallet')
                            for _, _mr in mpd_df.iterrows():
                                _mp2  = str(_mr.get(_mpd_pc, '')).strip().lower()
                                _mcn2 = str(_mr.get(_mpd_cn, '')).strip()
                                if _mp2 and _mcn2 and _mcn2 not in ('nan','None',''):
                                    if _mp2 not in country_lookup:
                                        country_lookup[_mp2] = _mcn2

                        # From old_history (pallet + gen_pallet_id → country_name)
                        try:
                            _oh2_df = DBManager.read_table("old_history")
                            if not _oh2_df.empty:
                                _oh2c = {str(c).strip().lower(): str(c).strip() for c in _oh2_df.columns}
                                for _, _ohr2 in _oh2_df.iterrows():
                                    for _ck in ['pallet', 'gen_pallet_id']:
                                        _kv = str(_ohr2.get(_oh2c.get(_ck, _ck), '')).strip().lower()
                                        if _kv and _kv not in ('nan','none',''):
                                            _cv = str(_ohr2.get(_oh2c.get('country_name','country_name'), '')).strip()
                                            if _cv and _cv not in ('nan','None','') and _kv not in country_lookup:
                                                country_lookup[_kv] = _cv
                        except Exception:
                            pass

                        # From load_history (generated_load_id → country — fallback via Order NO)
                        load_id_country_map = {}
                        try:
                            _lh_df = DBManager.read_table("load_history")
                            if not _lh_df.empty:
                                _lhc = {str(c).strip().lower(): str(c).strip() for c in _lh_df.columns}
                                _lh_lid_col = _lhc.get('generated load id', 'Generated Load ID')
                                _lh_cnt_col = _lhc.get('country name', 'Country Name')
                                for _, _lhr in _lh_df.iterrows():
                                    _lid2 = str(_lhr.get(_lh_lid_col, '')).strip()
                                    _lc2  = str(_lhr.get(_lh_cnt_col, '')).strip()
                                    if _lid2 and _lc2 and _lc2 not in ('nan','None',''):
                                        load_id_country_map[_lid2] = _lc2
                        except Exception:
                            pass

                        # ── Source 3: logic2_matched_partial_history (pallet or gen_pallet_id) ──
                        # Per Logic.txt Additional: blank fill sequence step 6
                        l2_matched_hist_lookup = {}   # keyed by pallet (lower) or gen_pallet_id (lower)
                        try:
                            _l2mph_df = DBManager.read_table("logic2_matched_partial_history")
                            if not _l2mph_df.empty:
                                _l2mph_c = {str(c).strip().lower(): str(c).strip() for c in _l2mph_df.columns}
                                _FILL_DB_KEYS = {
                                    'Vendor Name':    'vendor_name',    'Invoice Number': 'invoice_number',
                                    'Fifo Date':      'fifo_date',       'Grn Number':     'grn_number',
                                    'Client So':      'client_so',       'Supplier Hu':    'supplier_hu',
                                    'Supplier':       'supplier',        'Lot Number':     'lot_number',
                                    'Style':          'style',           'Color':          'color',
                                    'Size':           'size',            'Client So 2':    'client_so_2',
                                }
                                for _, _r in _l2mph_df.iterrows():
                                    _pal_keys = []
                                    _raw_p = str(_r.get(_l2mph_c.get('pallet','pallet'), '')).strip().lower()
                                    _raw_gp = str(_r.get(_l2mph_c.get('gen_pallet_id','gen_pallet_id'), '')).strip().lower()
                                    if _raw_p and _raw_p not in ('nan','none',''): _pal_keys.append(_raw_p)
                                    if _raw_gp and _raw_gp not in ('nan','none',''): _pal_keys.append(_raw_gp)
                                    if not _pal_keys: continue
                                    _entry = {}
                                    for _col_d, _db_k in _FILL_DB_KEYS.items():
                                        _v = str(_r.get(_l2mph_c.get(_db_k, _db_k), '')).strip()
                                        if _v and _v not in ('nan', 'None'): _entry[_col_d] = _v
                                    _ord2 = str(_r.get(_l2mph_c.get('load_id','load_id'), '')).strip()
                                    if _ord2 and _ord2 not in ('nan','None',''): _entry['Order NO'] = _ord2
                                    for _pk in _pal_keys:
                                        if _pk not in l2_matched_hist_lookup:
                                            l2_matched_hist_lookup[_pk] = _entry
                                        # Also populate country_lookup
                                        _cn_l2m = str(_r.get(_l2mph_c.get('country_name','country_name'), '')).strip()
                                        if _cn_l2m and _cn_l2m not in ('nan','None','') and _pk not in country_lookup:
                                            country_lookup[_pk] = _cn_l2m
                        except Exception:
                            pass

                        # ── Source 4: logic2_history (pallet or gen_pallet_id) ──────────────
                        # Per Logic.txt Additional: blank fill sequence step 7
                        l2_history_lookup = {}   # keyed by pallet (lower) or gen_pallet_id (lower)
                        try:
                            _l2h_df = DBManager.read_table("logic2_history")
                            if not _l2h_df.empty:
                                _l2h_c = {str(c).strip().lower(): str(c).strip() for c in _l2h_df.columns}
                                for _, _r in _l2h_df.iterrows():
                                    _pal_keys2 = []
                                    _raw_p2  = str(_r.get(_l2h_c.get('pallet','pallet'), '')).strip().lower()
                                    _raw_gp2 = str(_r.get(_l2h_c.get('gen_pallet_id','gen_pallet_id'), '')).strip().lower()
                                    if _raw_p2  and _raw_p2  not in ('nan','none',''): _pal_keys2.append(_raw_p2)
                                    if _raw_gp2 and _raw_gp2 not in ('nan','none',''): _pal_keys2.append(_raw_gp2)
                                    if not _pal_keys2: continue
                                    _entry2 = {}
                                    for _col_d, _db_k in _FILL_DB_KEYS.items():
                                        _v2 = str(_r.get(_l2h_c.get(_db_k, _db_k), '')).strip()
                                        if _v2 and _v2 not in ('nan', 'None'): _entry2[_col_d] = _v2
                                    _ord3 = str(_r.get(_l2h_c.get('load_id','load_id'), '')).strip()
                                    if _ord3 and _ord3 not in ('nan','None',''): _entry2['Order NO'] = _ord3
                                    for _pk2 in _pal_keys2:
                                        if _pk2 not in l2_history_lookup:
                                            l2_history_lookup[_pk2] = _entry2
                                        # Also populate country_lookup
                                        _cn_l2h = str(_r.get(_l2h_c.get('country_name','country_name'), '')).strip()
                                        if _cn_l2h and _cn_l2h not in ('nan','None','') and _pk2 not in country_lookup:
                                            country_lookup[_pk2] = _cn_l2h
                        except Exception:
                            pass

                        # ── Source 5: master_partial_data (last fallback by pallet or gen_pallet_id) ──
                        # Built later after partial_map is constructed (see fill_row_from_oh_master helper)

                        # ── vendor_country_map ──────────────────────────────────────────────
                        def _get_vendor_country_map_from_df(vdf):
                            if vdf is None or vdf.empty: return {}
                            if 'Vendor Name' in vdf.columns and 'Country' in vdf.columns:
                                return {
                                    str(v).strip().lower(): str(c).strip()
                                    for v, c in zip(vdf['Vendor Name'], vdf['Country'])
                                    if str(v).strip() and str(c).strip() not in ('', 'nan', 'None')
                                }
                            return {}

                        vendor_country_map = _get_vendor_country_map_from_df(_vendor_df_fmt)
                        if not vendor_country_map:
                            vendor_country_map = get_vendor_country_map()

                        # ── partial lookup maps ────────────────────────────────────────────
                        def _get_partial_lookup_maps_from_df(part_df):
                            invoice_map = {}; grn_map = {}; vendor_map = {}
                            pallet_invoice_map = {}; pallet_grn_map = {}
                            if part_df is None or part_df.empty:
                                return invoice_map, grn_map, vendor_map, pallet_invoice_map, pallet_grn_map
                            pc2 = {str(c).strip().lower(): str(c).strip() for c in part_df.columns}
                            gp_col2 = pc2.get('gen pallet id', 'Gen Pallet ID')
                            iv_col2 = pc2.get('invoice number', 'Invoice Number')
                            gr_col2 = pc2.get('grn number', 'Grn Number')
                            vn_col2 = pc2.get('vendor name', 'Vendor Name')
                            pp_col2 = pc2.get('pallet', 'Pallet')
                            for _, r2 in part_df.iterrows():
                                gp_key   = str(r2.get(gp_col2, '')).strip().lower()
                                pp_key   = str(r2.get(pp_col2, '')).strip()
                                inv_num  = str(r2.get(iv_col2, '')).strip()
                                grn_num  = str(r2.get(gr_col2, '')).strip()
                                vnd_name = str(r2.get(vn_col2, '')).strip()
                                if inv_num and inv_num not in ('nan','None'):
                                    if gp_key: invoice_map[gp_key] = inv_num
                                if grn_num and grn_num not in ('nan','None'):
                                    if gp_key: grn_map[gp_key] = grn_num
                                if vnd_name and vnd_name not in ('nan','None'):
                                    if gp_key: vendor_map[gp_key] = vnd_name
                                if pp_key:
                                    if inv_num and inv_num not in ('nan','None'):
                                        pallet_invoice_map[pp_key] = inv_num
                                    if grn_num and grn_num not in ('nan','None'):
                                        pallet_grn_map[pp_key] = grn_num
                                    if vnd_name and vnd_name not in ('nan','None'):
                                        vendor_map[pp_key.lower()] = vnd_name
                            return invoice_map, grn_map, vendor_map, pallet_invoice_map, pallet_grn_map

                        inv_invoice_map_fmt, inv_grn_map_fmt, partial_vendor_map_fmt, pallet_invoice_map_fmt, pallet_grn_map_fmt = \
                            _get_partial_lookup_maps_from_df(_partial_df_fmt)
                        if not any([inv_invoice_map_fmt, inv_grn_map_fmt, partial_vendor_map_fmt]):
                            inv_invoice_map_fmt, inv_grn_map_fmt, partial_vendor_map_fmt, pallet_invoice_map_fmt, pallet_grn_map_fmt = get_partial_lookup_maps()

                        # ── MPD exact map: {(pallet, actual_qty): [entries]} ───────────────
                        mpd_exact_map        = {}   # (pallet, qty) → [entries]
                        mpd_pallet_rows      = {}   # pallet → [entries]
                        mpd_partial_pallets  = set() # pallets where remark='Partial' in MPD (→ route to Logic 4)
                        if not mpd_df.empty:
                            p_col  = mpd_col.get('pallet',             'Pallet')
                            aq_col = mpd_col.get('actual qty',         'Actual Qty')
                            cn_col = mpd_col.get('country name',       'Country Name')
                            gl_col = mpd_col.get('generated load id',  'Generated Load ID')
                            rk_col = mpd_col.get('remark',             'Remark')
                            for _, _mrow in mpd_df.iterrows():
                                _mp  = str(_mrow.get(p_col,  '')).strip()
                                _mq  = pd.to_numeric(_mrow.get(aq_col, 0), errors='coerce')
                                if pd.isna(_mq): _mq = 0.0
                                _mq  = float(_mq)
                                _mcn = str(_mrow.get(cn_col, '')).strip()
                                _mgl = str(_mrow.get(gl_col, '')).strip()
                                _mrk = str(_mrow.get(rk_col, '')).strip().lower()
                                if _mp:
                                    _key = (_mp, round(_mq, 6))
                                    if _key not in mpd_exact_map:
                                        mpd_exact_map[_key] = []
                                    mpd_exact_map[_key].append({'country': _mcn, 'load_id': _mgl, 'actual_qty': _mq, 'remark': _mrk})
                                    if _mp not in mpd_pallet_rows:
                                        mpd_pallet_rows[_mp] = []
                                    mpd_pallet_rows[_mp].append({'country': _mcn, 'load_id': _mgl, 'actual_qty': _mq, 'remark': _mrk})
                                    # Logic 3 → Logic 4 routing: if remark='partial', mark this pallet
                                    if _mrk == 'partial':
                                        mpd_partial_pallets.add(_mp)

                        # ── Damage setup ─────────────────────────────────────────────────
                        dmg_df = _damage_df_fmt
                        damage_remarks = []
                        dmg_pallet_remark_qty = {}
                        damage_pallets = set()
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns and 'Remark' in dmg_df.columns:
                            _dmg = dmg_df.copy()
                            _dmg['_pkey'] = _dmg['Pallet'].astype(str).str.strip()
                            _dmg['_rmk']  = _dmg['Remark'].astype(str).str.strip()
                            _dmg['_dqty'] = pd.to_numeric(_dmg.get('Actual Qty', 0), errors='coerce').fillna(0)
                            damage_remarks = list(_dmg['_rmk'].unique())
                            _dmg_grp = _dmg.groupby(['_pkey', '_rmk'])['_dqty'].sum()
                            dmg_pallet_remark_qty = {(p, r): v for (p, r), v in _dmg_grp.items()}
                        if not dmg_df.empty and 'Pallet' in dmg_df.columns:
                            damage_pallets = set(str(p).strip() for p in dmg_df['Pallet'].dropna())

                        # ── Partial map ──────────────────────────────────────────────────
                        partial_df = _partial_df_fmt
                        partial_map  = {}   # orig_pallet → [entries]
                        gen_to_orig  = {}   # gen_pallet  → orig_pallet
                        gen_pallet_exact_map = {}  # (gen_pallet, partial_qty) → entry
                        if not partial_df.empty:
                            pc     = {str(c).strip().lower(): str(c).strip() for c in partial_df.columns}
                            pp_col  = pc.get('pallet',        'Pallet')
                            pq_col  = pc.get('partial qty',   'Partial Qty')
                            pg_col  = pc.get('gen pallet id', 'Gen Pallet ID')
                            pl_col  = pc.get('load id',       'Load ID')
                            pa_col  = pc.get('actual qty',    'Actual Qty')
                            pi_col  = pc.get('invoice number','Invoice Number')
                            pg2_col = pc.get('grn number',    'Grn Number')
                            pcn_col = pc.get('country name',  'Country Name')
                            pb_col  = pc.get('balance qty',   'Balance Qty')
                            _sel_cols = [c for c in [pp_col, pq_col, pg_col, pl_col, pa_col,
                                                      pi_col, pg2_col, pb_col, pcn_col]
                                         if c in partial_df.columns]
                            _pdf = partial_df[_sel_cols].copy()
                            _pdf[pq_col] = pd.to_numeric(_pdf[pq_col], errors='coerce').fillna(0)
                            _pdf[pa_col] = pd.to_numeric(_pdf[pa_col], errors='coerce').fillna(0)
                            if pb_col in _pdf.columns:
                                _pdf[pb_col] = pd.to_numeric(_pdf[pb_col], errors='coerce').fillna(0)
                            for _, _pr in _pdf.iterrows():
                                opallet  = str(_pr.get(pp_col,  '')).strip()
                                gpallet  = str(_pr.get(pg_col,  '')).strip()
                                pqty     = float(_pr.get(pq_col, 0))
                                loadid_p = str(_pr.get(pl_col,  '')).strip()
                                aqty     = float(_pr.get(pa_col, 0))
                                bqty     = float(_pr.get(pb_col, 0)) if pb_col in _pr.index else 0.0
                                inv_num  = str(_pr.get(pi_col,  '')).strip()
                                grn_num  = str(_pr.get(pg2_col, '')).strip()
                                cntry_p  = str(_pr.get(pcn_col, '')).strip() if pcn_col in _pr.index else ''
                                if inv_num  in ('nan','None'): inv_num  = ''
                                if grn_num  in ('nan','None'): grn_num  = ''
                                if cntry_p  in ('nan','None'): cntry_p  = ''
                                entry = {
                                    'gen_pallet': gpallet, 'partial_qty': pqty,
                                    'load_id': loadid_p,   'mpd_actual': aqty,
                                    'balance_qty': bqty,   'invoice_number': inv_num,
                                    'grn_number': grn_num, 'orig_pallet': opallet,
                                    'country': cntry_p,
                                }
                                if opallet:
                                    if opallet not in partial_map: partial_map[opallet] = []
                                    partial_map[opallet].append(entry)
                                if gpallet and opallet:
                                    gen_to_orig[gpallet] = opallet
                                if gpallet:
                                    _l2key = (gpallet, round(pqty, 6))
                                    gen_pallet_exact_map[_l2key] = entry
                                # Populate country_lookup from partial data
                                if cntry_p and cntry_p not in ('nan','None',''):
                                    if opallet and opallet.lower() not in country_lookup:
                                        country_lookup[opallet.lower()] = cntry_p
                                    if gpallet and gpallet.lower() not in country_lookup:
                                        country_lookup[gpallet.lower()] = cntry_p

                        import re as _re
                        _gen_pat = _re.compile(r'^(.+)-P(\d+)$')
                        def _base_pallet(p):
                            m = _gen_pat.match(str(p).strip())
                            return m.group(1) if m else str(p).strip()

                        def _is_blank(val):
                            return not val or str(val).strip() in ('', 'nan', 'None')

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
                                elif h in ('Invoice Number', 'Grn Number'):
                                    row[h] = ''
                                elif h in inv_row.index:
                                    row[h] = inv_row[h]
                                else:
                                    fb = inv_col_map_r.get(h.strip().lower())
                                    row[h] = inv_row[fb] if fb and fb in inv_row.index else ''
                            return row

                        def fill_row_from_partial(row, pallet_key=None, gen_pallet_key=None, partial_entry=None):
                            if _is_blank(row.get('Invoice Number')):
                                if partial_entry and not _is_blank(partial_entry.get('invoice_number','')):
                                    row['Invoice Number'] = partial_entry['invoice_number']
                                elif gen_pallet_key and not _is_blank(inv_invoice_map_fmt.get(gen_pallet_key,'')):
                                    row['Invoice Number'] = inv_invoice_map_fmt[gen_pallet_key]
                                elif pallet_key and not _is_blank(pallet_invoice_map_fmt.get(pallet_key,'')):
                                    row['Invoice Number'] = pallet_invoice_map_fmt[pallet_key]
                            if _is_blank(row.get('Grn Number')):
                                if partial_entry and not _is_blank(partial_entry.get('grn_number','')):
                                    row['Grn Number'] = partial_entry['grn_number']
                                elif gen_pallet_key and not _is_blank(inv_grn_map_fmt.get(gen_pallet_key,'')):
                                    row['Grn Number'] = inv_grn_map_fmt[gen_pallet_key]
                                elif pallet_key and not _is_blank(pallet_grn_map_fmt.get(pallet_key,'')):
                                    row['Grn Number'] = pallet_grn_map_fmt[pallet_key]
                            return row

                        def fill_row_from_oh_master(row, pallet_key, is_pick_or_alloc=False):
                            """Blank fill sequence per Logic.txt Additional:
                            1. master_pick_data (already done before calling this)
                            2. old_history_master → oh_master_lookup
                            3. old_history       → oh_history_lookup
                            4. old_history_master (2nd pass) → oh_master_lookup
                            5. old_history (2nd pass)       → oh_history_lookup
                            6. logic2_matched_partial_history (pallet or gen_pallet_id) → l2_matched_hist_lookup
                            7. logic2_history (pallet or gen_pallet_id) → l2_history_lookup
                            8. master_partial_data (pallet or gen_pallet_id) → partial_map / gen_to_orig
                            NOTE: Order NO only filled if is_pick_or_alloc=True (Pick Qty or Allocated > 0)
                            """
                            _pkey_low = str(pallet_key).lower()
                            _base_low = _base_pallet(pallet_key).lower()

                            def _fill_from_src(src_data):
                                for col in OH_FILL_COLS:
                                    if _is_blank(row.get(col)) and not _is_blank(src_data.get(col, '')):
                                        row[col] = src_data[col]
                                # Order NO only for Pick / Allocated rows
                                if is_pick_or_alloc:
                                    if _is_blank(row.get('Order NO')) and not _is_blank(src_data.get('Order NO', '')):
                                        row['Order NO'] = src_data['Order NO']

                            # Source 2: old_history_master (1st pass)
                            oh_data = oh_master_lookup.get(_pkey_low) or oh_master_lookup.get(_base_low, {})
                            _fill_from_src(oh_data)

                            # Source 3: old_history (1st pass)
                            oh_hist = oh_history_lookup.get(_pkey_low) or oh_history_lookup.get(_base_low, {})
                            _fill_from_src(oh_hist)

                            # Source 4: old_history_master (2nd pass — Logic.txt repeats this)
                            oh_data2 = oh_master_lookup.get(_pkey_low) or oh_master_lookup.get(_base_low, {})
                            _fill_from_src(oh_data2)

                            # Source 5: old_history (2nd pass — Logic.txt repeats this)
                            oh_hist2 = oh_history_lookup.get(_pkey_low) or oh_history_lookup.get(_base_low, {})
                            _fill_from_src(oh_hist2)

                            # Source 6: logic2_matched_partial_history (pallet or gen_pallet_id)
                            l2m_data = l2_matched_hist_lookup.get(_pkey_low) or l2_matched_hist_lookup.get(_base_low, {})
                            _fill_from_src(l2m_data)

                            # Source 7: logic2_history (pallet or gen_pallet_id)
                            l2h_data = l2_history_lookup.get(_pkey_low) or l2_history_lookup.get(_base_low, {})
                            _fill_from_src(l2h_data)

                            # Source 8: master_partial_data (pallet or gen_pallet_id)
                            _part_src = pallet_key  # could be orig pallet or gen_pallet_id
                            _part_entries = partial_map.get(str(_part_src).strip()) or \
                                            partial_map.get(_base_pallet(str(_part_src).strip()), [])
                            for pe in _part_entries:
                                if not _part_entries:
                                    break
                                if _is_blank(row.get('Invoice Number')) and not _is_blank(pe.get('invoice_number', '')):
                                    row['Invoice Number'] = pe['invoice_number']
                                if _is_blank(row.get('Grn Number')) and not _is_blank(pe.get('grn_number', '')):
                                    row['Grn Number'] = pe['grn_number']
                            return row

                        def _get_vendor_info(inv_row, pallet_key):
                            vn = str(inv_row.get('Vendor Name', '')).strip() if 'Vendor Name' in inv_row.index else ''
                            if not vn: vn = partial_vendor_map_fmt.get(pallet_key, '')
                            vc = vendor_country_map.get(vn.lower(), '') if vn else ''
                            inv_n = str(inv_row.get('Invoice Number', '')).strip() if 'Invoice Number' in inv_row.index else ''
                            if not inv_n or inv_n in ('nan','None'): inv_n = pallet_invoice_map_fmt.get(pallet_key, '')
                            grn_n = str(inv_row.get('Grn Number', '')).strip() if 'Grn Number' in inv_row.index else ''
                            if not grn_n or grn_n in ('nan','None'): grn_n = pallet_grn_map_fmt.get(pallet_key, '')
                            return vn, vc, inv_n, grn_n

                        # ══════════════════════════════════════════════════════════════════
                        # Split inventory: Pick Id filled = picked, 0/blank = not picked
                        # ══════════════════════════════════════════════════════════════════
                        def _pick_id_filled(val):
                            return str(val).strip() not in ('', '0', 'nan', 'None', '0.0')

                        if _inv_pid_col in inv_data.columns:
                            inv_picked   = inv_data[inv_data[_inv_pid_col].apply(_pick_id_filled)].copy()
                            inv_unpicked = inv_data[~inv_data[_inv_pid_col].apply(_pick_id_filled)].copy()
                        else:
                            inv_picked   = pd.DataFrame(columns=inv_data.columns)
                            inv_unpicked = inv_data.copy()
                            st.warning("⚠️ 'Pick Id' column inventory file හි නෑ — all rows unpicked ලෙස process කරයි")

                        fmt_rows = []
                        l1_unmatched = []    # Logic 1/2 unmatched report
                        l3_unmatched = []    # Logic 3/4/5 unmatched (ATS) report

                        # Set to track matched Pallet strings from Logic 1 & 2
                        # Logic 4 calculation ලදී L1/L2 matched gen_pallet_ids exclude කරන්න
                        l1_l2_matched_pallets = set()  # orig pallet strings matched by L1
                        l1_l2_matched_gen_pallets = set()  # gen_pallet_ids matched by L2

                        logic3_matched_pallets = set()
                        logic4_matched_pallets = set()

                        # ════════════════════════════════════════════════════════════════
                        # LOGIC 1: Pick Id column check → value filled (e.g. 73500258) = picked,
                        #          value 0 = not picked.
                        #          Picked rows: Pallet + Actual Qty → master_pick_data exact match
                        #          → Pick_Report: other data update + Actual Qty & Pick Quantity update
                        #          → Exact matched Pallets → NOT passed to next logics
                        # ════════════════════════════════════════════════════════════════
                        l1_matched_idx = set()
                        for _inv_idx, inv_row in inv_picked.iterrows():
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = float(pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)
                            _l1key = (orig_pallet, round(inv_actual_qty, 6))
                            if _l1key not in mpd_exact_map:
                                continue
                            l1_matched_idx.add(_inv_idx)
                            l1_l2_matched_pallets.add(orig_pallet)
                            vn, vc, inv_n, grn_n = _get_vendor_info(inv_row, orig_pallet)
                            _mpd_e = mpd_exact_map[_l1key][0]
                            row = build_row(inv_row)
                            row['Pick Quantity']       = inv_actual_qty
                            row['Allocated']           = ''
                            row['Destination Country'] = _mpd_e.get('country', '')
                            row['Order NO']            = _mpd_e.get('load_id', '')
                            for rmk in damage_remarks: row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                            row['ATS']            = ''
                            row['Vendor Name']    = vn
                            row['COO']            = vc
                            row['Invoice Number'] = inv_n
                            row['Grn Number']     = grn_n
                            row = fill_row_from_partial(row, pallet_key=orig_pallet)
                            row = fill_row_from_oh_master(row, orig_pallet, is_pick_or_alloc=True)
                            fmt_rows.append(row)

                        # ════════════════════════════════════════════════════════════════
                        # LOGIC 2: Logic 1 unmatch →
                        #          master_partial_data gen_pallet_id + partial_qty exact match
                        #          → Pick_Report: data update + Actual Qty & Pick Quantity update
                        #          → After update: matched line deleted from master_partial_data
                        #            and moved to old_history DB (per Logic.txt Logic 2)
                        #          → Exact matched Pallets → NOT passed to next logics
                        #          → L1 + L2 both unmatched Pallets → Unmatched Report generated
                        # ════════════════════════════════════════════════════════════════
                        l2_matched_idx = set()
                        l2_gen_pallets_to_archive = []   # (gen_pallet_id, partial_qty) entries matched by L2
                        for _inv_idx, inv_row in inv_picked.iterrows():
                            if _inv_idx in l1_matched_idx:
                                continue
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = float(pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)
                            _l2key = (orig_pallet, round(inv_actual_qty, 6))
                            if _l2key in gen_pallet_exact_map:
                                l2_matched_idx.add(_inv_idx)
                                l1_l2_matched_gen_pallets.add(orig_pallet)  # gen_pallet_id track
                                par_entry  = gen_pallet_exact_map[_l2key]
                                real_orig  = par_entry.get('orig_pallet', orig_pallet)
                                l1_l2_matched_pallets.add(real_orig)
                                # Track for post-loop delete + archive
                                l2_gen_pallets_to_archive.append({
                                    'gen_pallet_id': orig_pallet,
                                    'partial_qty':   inv_actual_qty,
                                    'par_entry':     par_entry,
                                })
                                vn, vc, inv_n, grn_n = _get_vendor_info(inv_row, real_orig)
                                if not vn: vn = partial_vendor_map_fmt.get(orig_pallet, '')
                                if not inv_n: inv_n = par_entry.get('invoice_number','') or inv_invoice_map_fmt.get(orig_pallet,'')
                                if not grn_n: grn_n = par_entry.get('grn_number','') or inv_grn_map_fmt.get(orig_pallet,'')
                                _mpd_m2 = mpd_pallet_rows.get(real_orig, [])
                                _cntry2 = (_mpd_m2[0].get('country','') if _mpd_m2 else '') or par_entry.get('country','')
                                row = build_row(inv_row)
                                row['Pick Quantity']       = inv_actual_qty
                                row['Allocated']           = ''
                                row['Destination Country'] = _cntry2
                                row['Order NO']            = par_entry.get('load_id', '')
                                for rmk in damage_remarks: row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                row['ATS']            = ''
                                row['Vendor Name']    = vn
                                row['COO']            = vc
                                row['Invoice Number'] = inv_n
                                row['Grn Number']     = grn_n
                                row = fill_row_from_partial(row, pallet_key=real_orig, gen_pallet_key=orig_pallet, partial_entry=par_entry)
                                row = fill_row_from_oh_master(row, real_orig, is_pick_or_alloc=True)
                                fmt_rows.append(row)
                            else:
                                # L1 + L2 both unmatched → L2B will attempt logic2_matched_partial_history match
                                pass  # L2B loop below handles remaining inv_picked rows not in l1/l2_matched_idx

                        # ════════════════════════════════════════════════════════════════
                        # LOGIC 2B: L1 + L2 both unmatched rows →
                        #           logic2_matched_partial_history exact match:
                        #           Inventory Pallet (= gen_pallet_id) + Actual Qty (= partial_qty)
                        #           → Pick_Report: data update + Actual Qty & Pick Quantity update
                        #           → Matched rows removed from l1_unmatched (not reported as unmatched)
                        # ════════════════════════════════════════════════════════════════

                        # Build exact map from logic2_matched_partial_history:
                        # (gen_pallet_id_lower, partial_qty_rounded) → entry
                        l2b_exact_map = {}
                        try:
                            _l2b_df = DBManager.read_table("logic2_matched_partial_history")
                            if not _l2b_df.empty:
                                _l2b_c = {str(c).strip().lower(): str(c).strip() for c in _l2b_df.columns}
                                _l2b_gp  = _l2b_c.get('gen_pallet_id', 'gen_pallet_id')
                                _l2b_pq  = _l2b_c.get('partial_qty',   'partial_qty')
                                _l2b_lid = _l2b_c.get('load_id',        'load_id')
                                _l2b_cn  = _l2b_c.get('country_name',   'country_name')
                                _l2b_pal = _l2b_c.get('pallet',         'pallet')
                                _l2b_vn  = _l2b_c.get('vendor_name',    'vendor_name')
                                _l2b_inv = _l2b_c.get('invoice_number',  'invoice_number')
                                _l2b_grn = _l2b_c.get('grn_number',     'grn_number')
                                for _, _l2b_row in _l2b_df.iterrows():
                                    _gp_val  = str(_l2b_row.get(_l2b_gp,  '')).strip()
                                    _pq_val  = pd.to_numeric(_l2b_row.get(_l2b_pq, 0), errors='coerce')
                                    if pd.isna(_pq_val): _pq_val = 0.0
                                    _pq_val  = float(_pq_val)
                                    if _gp_val and _gp_val not in ('nan','None',''):
                                        _l2b_key = (_gp_val.lower(), round(_pq_val, 6))
                                        if _l2b_key not in l2b_exact_map:
                                            _l2b_aq  = _l2b_c.get('actual_qty',   'actual_qty')
                                            _l2b_bq  = _l2b_c.get('balance_qty',  'balance_qty')
                                            _l2b_aq_val = pd.to_numeric(_l2b_row.get(_l2b_aq, 0), errors='coerce')
                                            _l2b_bq_val = pd.to_numeric(_l2b_row.get(_l2b_bq, 0), errors='coerce')
                                            l2b_exact_map[_l2b_key] = {
                                                'gen_pallet_id':  _gp_val,
                                                'partial_qty':    _pq_val,
                                                'actual_qty':     float(_l2b_aq_val) if pd.notna(_l2b_aq_val) else 0.0,
                                                'balance_qty':    float(_l2b_bq_val) if pd.notna(_l2b_bq_val) else 0.0,
                                                'orig_pallet':    str(_l2b_row.get(_l2b_pal, '')).strip(),
                                                'load_id':        str(_l2b_row.get(_l2b_lid, '')).strip(),
                                                'country':        str(_l2b_row.get(_l2b_cn,  '')).strip(),
                                                'vendor_name':    str(_l2b_row.get(_l2b_vn,  '')).strip(),
                                                'invoice_number': str(_l2b_row.get(_l2b_inv, '')).strip(),
                                                'grn_number':     str(_l2b_row.get(_l2b_grn, '')).strip(),
                                            }
                        except Exception as _l2b_build_err:
                            st.warning(f"⚠️ Logic 2B map build error: {_l2b_build_err}")

                        l2b_still_unmatched = []
                        l2b_matched_count   = 0

                        for _inv_idx, inv_row in inv_picked.iterrows():
                            if _inv_idx in l1_matched_idx or _inv_idx in l2_matched_idx:
                                continue  # already matched by L1 or L2
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = float(pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)

                            # Try gen_pallet_id (inventory Pallet) + partial_qty (inventory Actual Qty) exact match
                            _l2b_key = (orig_pallet.lower(), round(inv_actual_qty, 6))
                            if _l2b_key in l2b_exact_map:
                                _l2b_e    = l2b_exact_map[_l2b_key]
                                real_orig = _l2b_e.get('orig_pallet', orig_pallet) or orig_pallet
                                l1_l2_matched_gen_pallets.add(orig_pallet)
                                l1_l2_matched_pallets.add(real_orig)

                                vn, vc, inv_n, grn_n = _get_vendor_info(inv_row, real_orig)
                                if not vn:    vn    = _l2b_e.get('vendor_name', '')    or partial_vendor_map_fmt.get(orig_pallet, '')
                                if not inv_n: inv_n = _l2b_e.get('invoice_number', '') or inv_invoice_map_fmt.get(orig_pallet, '')
                                if not grn_n: grn_n = _l2b_e.get('grn_number', '')    or inv_grn_map_fmt.get(orig_pallet, '')
                                _cntry_l2b = _l2b_e.get('country', '') or \
                                             country_lookup.get(orig_pallet.lower(), '') or \
                                             country_lookup.get(real_orig.lower(), '')

                                row = build_row(inv_row)
                                row['Pick Quantity']       = inv_actual_qty
                                row['Allocated']           = ''
                                row['Destination Country'] = _cntry_l2b
                                row['Order NO']            = _l2b_e.get('load_id', '')
                                for rmk in damage_remarks: row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                row['ATS']            = ''
                                row['Vendor Name']    = vn
                                row['COO']            = vc
                                row['Invoice Number'] = inv_n
                                row['Grn Number']     = grn_n
                                row = fill_row_from_partial(row, pallet_key=real_orig, gen_pallet_key=orig_pallet)
                                row = fill_row_from_oh_master(row, real_orig, is_pick_or_alloc=True)
                                fmt_rows.append(row)
                                l2b_matched_count += 1

                                # ── L2B Balance row: if balance_qty > 0 generate ATS row for orig_pallet ──
                                # Find orig_pallet in inv_unpicked (Pick Id = 0) to get inventory row data
                                _l2b_bal_qty = _l2b_e.get('balance_qty', 0) or 0
                                if _l2b_bal_qty > 0.01:
                                    # Look for orig_pallet row in inv_unpicked
                                    _bal_inv_rows = inv_unpicked[
                                        inv_unpicked[_inv_pal_col].astype(str).str.strip() == real_orig
                                    ]
                                    if not _bal_inv_rows.empty:
                                        _bal_inv_row = _bal_inv_rows.iloc[0]
                                        _bal_act_qty = float(pd.to_numeric(_bal_inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)
                                        # Use inventory Actual Qty if > 0, else fall back to balance_qty
                                        _bal_use_qty = _bal_act_qty if _bal_act_qty > 0 else _l2b_bal_qty
                                        if _bal_use_qty > 0.01 and real_orig not in logic3_matched_pallets \
                                                and real_orig not in logic4_matched_pallets \
                                                and real_orig not in l1_l2_matched_pallets:
                                            bal_vn, bal_vc, bal_inv_n, bal_grn_n = _get_vendor_info(_bal_inv_row, real_orig)
                                            if not bal_vn: bal_vn = vn
                                            if not bal_vc: bal_vc = vc
                                            if not bal_inv_n: bal_inv_n = inv_n
                                            if not bal_grn_n: bal_grn_n = grn_n
                                            bal_row = build_row(_bal_inv_row, override_pallet=real_orig, override_actual_qty=round(_bal_use_qty, 3))
                                            bal_row['Pick Quantity']       = ''
                                            bal_row['Allocated']           = ''
                                            bal_row['Destination Country'] = ''
                                            bal_row['Order NO']            = ''
                                            for rmk in damage_remarks: bal_row[rmk] = dmg_pallet_remark_qty.get((real_orig, rmk), '')
                                            bal_row['ATS']            = round(_bal_use_qty, 3)
                                            bal_row['Vendor Name']    = bal_vn
                                            bal_row['COO']            = bal_vc
                                            bal_row['Invoice Number'] = bal_inv_n
                                            bal_row['Grn Number']     = bal_grn_n
                                            bal_row = fill_row_from_partial(bal_row, pallet_key=real_orig)
                                            bal_row = fill_row_from_oh_master(bal_row, real_orig)
                                            fmt_rows.append(bal_row)
                                            # Mark orig_pallet as handled so Logic 5 doesn't duplicate
                                            logic4_matched_pallets.add(real_orig)
                            else:
                                # Still unmatched after L1 + L2 + L2B — keep in unmatched report
                                l2b_still_unmatched.append({
                                    'Pallet':     orig_pallet,
                                    'Actual Qty': inv_actual_qty,
                                    'Pick Id':    str(inv_row.get(_inv_pid_col, '')).strip(),
                                    'Style':      str(inv_row.get('Style', '')).strip(),
                                    'Color':      str(inv_row.get('Color', '')).strip(),
                                    'Size':       str(inv_row.get('Size',  '')).strip(),
                                    'Reason':     '❌ No MPD / Partial / L2History match',
                                })

                        # Replace l1_unmatched with only the truly-still-unmatched after L2B
                        l1_unmatched = l2b_still_unmatched
                        if l2b_matched_count > 0:
                            st.info(f"📋 Logic 2B: **{l2b_matched_count}** L1/L2-unmatched rows matched via logic2_matched_partial_history")

                        # ── Logic 2 Post-loop: Delete matched lines from master_partial_data
                        #    and archive to logic2_matched_partial_history (new DB per Logic.txt) ──
                        if l2_gen_pallets_to_archive:
                            try:
                                sb_l2 = get_supabase_client()
                                _l2_deleted_count  = 0
                                _l2_archived_count = 0
                                _report_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                                for _l2_item in l2_gen_pallets_to_archive:
                                    _gp_id  = _l2_item['gen_pallet_id']
                                    _pe     = _l2_item['par_entry']
                                    _orig_p = _pe.get('orig_pallet', '')

                                    # 1. Delete from master_partial_data by gen_pallet_id
                                    try:
                                        _del_res = sb_l2.table('master_partial_data').delete()\
                                            .eq('gen_pallet_id', _gp_id).execute()
                                        _l2_deleted_count += len(_del_res.data) if _del_res.data else 1
                                    except Exception as _del_err:
                                        st.warning(f"⚠️ L2 delete error (gen_pallet_id={_gp_id}): {_del_err}")

                                    # 2. Archive to logic2_matched_partial_history (new dedicated table)
                                    try:
                                        _arch_row = {
                                            'archived_by':         current_user,
                                            'archive_reason':      'Logic2_Matched',
                                            'report_generated_at': _report_ts,
                                            # Identity
                                            'batch_id':            _pe.get('batch_id',    None) or None,
                                            'so_number':           _pe.get('so_number',   None) or None,
                                            'load_id':             _pe.get('load_id',     None) or None,
                                            'country_name':        _pe.get('country',     None) or None,
                                            # Pallet
                                            'pallet':              _orig_p or None,
                                            'gen_pallet_id':       _gp_id  or None,
                                            'supplier':            _pe.get('supplier',    None) or None,
                                            # Qty
                                            'actual_qty':          _pe.get('mpd_actual',  None),
                                            'partial_qty':         _l2_item['partial_qty'],
                                            'balance_qty':         _pe.get('balance_qty', None),
                                            # Item details
                                            'location_id':         _pe.get('location_id', None) or None,
                                            'lot_number':          _pe.get('lot_number',  None) or None,
                                            'color':               _pe.get('color',       None) or None,
                                            'size':                _pe.get('size',        None) or None,
                                            'style':               _pe.get('style',       None) or None,
                                            'customer_po_number':  _pe.get('customer_po_number', None) or None,
                                            # Vendor/docs
                                            'vendor_name':         _pe.get('vendor_name',     None) or None,
                                            'invoice_number':      _pe.get('invoice_number',   None) or None,
                                            'grn_number':          _pe.get('grn_number',       None) or None,
                                            # Inventory snapshot
                                            'inv_pallet':          _gp_id,   # gen_pallet_id = inventory pallet in L2
                                            'inv_actual_qty':      _l2_item['partial_qty'],
                                        }
                                        # Remove None string values
                                        _arch_row = {k: (None if str(v).strip() in ('nan','None','') else v)
                                                     for k, v in _arch_row.items()}
                                        sb_l2.table('logic2_matched_partial_history').insert(_arch_row).execute()
                                        _l2_archived_count += 1
                                    except Exception as _arch_err:
                                        st.warning(f"⚠️ L2 archive error (gen_pallet_id={_gp_id}): {_arch_err}")

                                DBManager.invalidate('master_partial_data')
                                if _l2_deleted_count > 0 or _l2_archived_count > 0:
                                    st.info(f"🗂️ Logic 2: **{_l2_deleted_count}** partial lines deleted from master_partial_data → **{_l2_archived_count}** archived to logic2_matched_partial_history")
                            except Exception as _l2_post_err:
                                st.warning(f"⚠️ Logic 2 post-process error: {_l2_post_err}")

                        # ════════════════════════════════════════════════════════════════
                        # LOGIC 3: Pick Id = 0 rows →
                        #          Inventory Pallet + Actual Qty → master_pick_data exact match
                        #          → Pick_Report: other data update + Actual Qty & Allocated (new col) update
                        #          → IMPORTANT: if master_pick_data remark = 'Partial' → route to Logic 4
                        #          → Exact matched Pallets (non-Partial) → NOT passed to next logics
                        # ════════════════════════════════════════════════════════════════
                        for _inv_idx, inv_row in inv_unpicked.iterrows():
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = float(pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)
                            # ── Skip if this pallet already matched by L1/L2/L2B (Pick Id filled rows) ──
                            if orig_pallet in l1_l2_matched_pallets or orig_pallet in l1_l2_matched_gen_pallets:
                                continue
                            _l3key = (orig_pallet, round(inv_actual_qty, 6))
                            if _l3key not in mpd_exact_map:
                                continue
                            # ── Logic 3 → Logic 4 routing: remark='Partial' check ──
                            _mpd_e3 = mpd_exact_map[_l3key][0]
                            if _mpd_e3.get('remark', '').lower() == 'partial' or orig_pallet in mpd_partial_pallets:
                                # Remark = Partial → send to Logic 4 (do NOT add to logic3_matched_pallets)
                                continue
                            logic3_matched_pallets.add(orig_pallet)
                            vn, vc, inv_n, grn_n = _get_vendor_info(inv_row, orig_pallet)
                            row = build_row(inv_row)
                            row['Pick Quantity']       = ''
                            row['Allocated']           = inv_actual_qty
                            row['Destination Country'] = _mpd_e3.get('country', '')
                            row['Order NO']            = _mpd_e3.get('load_id', '')
                            for rmk in damage_remarks: row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                            row['ATS']            = ''
                            row['Vendor Name']    = vn
                            row['COO']            = vc
                            row['Invoice Number'] = inv_n
                            row['Grn Number']     = grn_n
                            row = fill_row_from_partial(row, pallet_key=orig_pallet)
                            row = fill_row_from_oh_master(row, orig_pallet, is_pick_or_alloc=True)
                            fmt_rows.append(row)

                        # ════════════════════════════════════════════════════════════════
                        # LOGIC 4: Logic 3 unmatch →
                        #          master_partial_data pallet exact match
                        #          → Exclude gen_pallet_ids already matched by Logic 1/2/3 (already ok)
                        #          → Remaining gen_pallet_ids: count = lines to create
                        #            Each line: Pallet = gen_pallet_id, Actual Qty = partial_qty
                        #            (EX: pallet has 2 gen_pallet_ids, 1 captured by L1/L2/L3
                        #             → use only remaining 1 gen_pallet_id)
                        #          → sum(partial_qty of remaining gen_pallet_ids):
                        #            If sum < inventory Actual Qty:
                        #              balance = Actual Qty - sum → update Actual Qty & ATS
                        #            If sum == inventory Actual Qty:
                        #              Pallet = gen_pallet_id, Actual Qty = partial_qty (no balance row)
                        #          → Unmatch remaining Pallets → Unmatched Report generated
                        #          → Other remaining → Actual Qty & ATS update
                        # ════════════════════════════════════════════════════════════════
                        for _inv_idx, inv_row in inv_unpicked.iterrows():
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = float(pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)
                            is_damaged     = orig_pallet in damage_pallets

                            if orig_pallet in logic3_matched_pallets:
                                continue
                            # ── Skip if this pallet already matched by L1/L2/L2B ──
                            if orig_pallet in l1_l2_matched_pallets or orig_pallet in l1_l2_matched_gen_pallets:
                                continue

                            partials_all = partial_map.get(orig_pallet, [])
                            if not partials_all:
                                continue

                            # ── Logic 4 key rule: L1/L2 match කල gen_pallet_ids exclude ──
                            partials = [
                                p for p in partials_all
                                if p.get('gen_pallet', '') not in l1_l2_matched_gen_pallets
                            ]
                            if not partials:
                                continue  # all gen_pallets already used by L1/L2

                            logic4_matched_pallets.add(orig_pallet)
                            vn, vc, inv_n, grn_n = _get_vendor_info(inv_row, orig_pallet)
                            total_partial_qty = sum(p['partial_qty'] for p in partials)
                            _mpd_m4  = mpd_pallet_rows.get(orig_pallet, [])
                            _cntry4_fb = _mpd_m4[0].get('country', '') if _mpd_m4 else ''

                            for par_entry in partials:
                                gp   = par_entry['gen_pallet']
                                pqt  = par_entry['partial_qty']
                                _cntry4 = par_entry.get('country', '') or _cntry4_fb
                                row = build_row(inv_row, override_pallet=gp, override_actual_qty=pqt)
                                row['Pick Quantity']       = ''
                                row['Allocated']           = pqt
                                row['Destination Country'] = _cntry4
                                row['Order NO']            = par_entry.get('load_id', '')
                                for rmk in damage_remarks: row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                row['ATS']            = ''
                                row['Vendor Name']    = vn
                                row['COO']            = vc
                                row['Invoice Number'] = par_entry.get('invoice_number','') or inv_invoice_map_fmt.get(gp,'') or inv_n
                                row['Grn Number']     = par_entry.get('grn_number','') or inv_grn_map_fmt.get(gp,'') or grn_n
                                row = fill_row_from_partial(row, pallet_key=orig_pallet, gen_pallet_key=gp, partial_entry=par_entry)
                                row = fill_row_from_oh_master(row, orig_pallet, is_pick_or_alloc=True)
                                fmt_rows.append(row)

                            # Balance row → ATS
                            balance_qty = round(inv_actual_qty - total_partial_qty, 6)
                            if balance_qty > 0.01 and not is_damaged:
                                bal_row = build_row(inv_row, override_pallet=orig_pallet, override_actual_qty=round(balance_qty, 3))
                                bal_row['Pick Quantity']       = ''
                                bal_row['Allocated']           = ''
                                bal_row['Destination Country'] = ''
                                bal_row['Order NO']            = ''
                                for rmk in damage_remarks: bal_row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                                bal_row['ATS']            = round(balance_qty, 3)
                                bal_row['Vendor Name']    = vn
                                bal_row['COO']            = vc
                                bal_row['Invoice Number'] = inv_n
                                bal_row['Grn Number']     = grn_n
                                bal_row = fill_row_from_partial(bal_row, pallet_key=orig_pallet)
                                bal_row = fill_row_from_oh_master(bal_row, orig_pallet)
                                fmt_rows.append(bal_row)

                        # ════════════════════════════════════════════════════════════════
                        # LOGIC 5: Remaining unpicked (L3/L4 unmatch) → ATS
                        #          Damage details: code එකේ තියෙන විදිහටම preserve කෙරේ
                        # ════════════════════════════════════════════════════════════════
                        for _inv_idx, inv_row in inv_unpicked.iterrows():
                            orig_pallet    = str(inv_row.get(_inv_pal_col, '')).strip()
                            inv_actual_qty = float(pd.to_numeric(inv_row.get(_inv_aq_col, 0), errors='coerce') or 0)
                            is_damaged     = orig_pallet in damage_pallets

                            if orig_pallet in logic3_matched_pallets or orig_pallet in logic4_matched_pallets:
                                continue
                            # ── Skip if this pallet already matched by L1/L2/L2B (Pick Id filled rows) ──
                            if orig_pallet in l1_l2_matched_pallets or orig_pallet in l1_l2_matched_gen_pallets:
                                continue

                            vn, vc, inv_n, grn_n = _get_vendor_info(inv_row, orig_pallet)
                            row = build_row(inv_row)
                            row['Pick Quantity']       = ''
                            row['Allocated']           = ''
                            row['Destination Country'] = ''
                            row['Order NO']            = ''
                            for rmk in damage_remarks: row[rmk] = dmg_pallet_remark_qty.get((orig_pallet, rmk), '')
                            row['ATS']            = round(inv_actual_qty, 3) if (not is_damaged and inv_actual_qty > 0) else ''
                            row['Vendor Name']    = vn
                            row['COO']            = vc
                            row['Invoice Number'] = inv_n
                            row['Grn Number']     = grn_n
                            row = fill_row_from_partial(row, pallet_key=orig_pallet)
                            row = fill_row_from_oh_master(row, orig_pallet)
                            fmt_rows.append(row)

                            l3_unmatched.append({
                                'Pallet':     orig_pallet,
                                'Actual Qty': inv_actual_qty,
                                'Style':      str(inv_row.get('Style', '')).strip(),
                                'Color':      str(inv_row.get('Color', '')).strip(),
                                'Size':       str(inv_row.get('Size',  '')).strip(),
                                'Reason':     'ATS — No MPD / Partial match',
                            })

                        # ── Build fmt_df ──────────────────────────────────────────────────
                        _extra_cols = ['Pick Quantity', 'Allocated', 'Destination Country', 'Order NO'] \
                                      + damage_remarks + ['ATS', 'Vendor Name', 'COO']
                        final_cols  = REPORT_HEADERS + [c for c in _extra_cols if c not in REPORT_HEADERS]
                        for _fc in ['Invoice Number', 'Grn Number']:
                            if _fc not in final_cols:
                                final_cols.append(_fc)

                        fmt_df = pd.DataFrame(fmt_rows, columns=final_cols)
                        fmt_df = fmt_df[
                            fmt_df['Pallet'].astype(str).str.strip().replace({'nan':'','None':''}) != ''
                        ].reset_index(drop=True)

                        # ── Additional: Blank fill from OH master + old_history + L2 histories + partial + COO re-lookup ──
                        # Per Logic.txt Additional: fill order = master_pick_data (done) →
                        #   old_history_master → old_history → old_history_master (2nd) → old_history (2nd) →
                        #   logic2_matched_partial_history (pallet or gen_pallet_id) →
                        #   logic2_history (pallet or gen_pallet_id) →
                        #   master_partial_data (pallet or gen_pallet_id)
                        for idx, r_row in fmt_df.iterrows():
                            pkey = str(r_row.get('Pallet', '')).strip()
                            base = _base_pallet(pkey)
                            part_entries = partial_map.get(base, []) or partial_map.get(pkey, [])

                            vn = str(fmt_df.at[idx, 'Vendor Name']).strip()
                            if not vn or vn in ('nan','None',''):
                                fb_vn = partial_vendor_map_fmt.get(pkey,'') or partial_vendor_map_fmt.get(base,'')
                                if not fb_vn:
                                    for pe in part_entries:
                                        fb_vn = partial_vendor_map_fmt.get(pe.get('gen_pallet',''), '')
                                        if fb_vn: break
                                if not fb_vn:
                                    # old_history_master (1st pass)
                                    fb_vn = oh_master_lookup.get(pkey.lower(),{}).get('Vendor Name','') or \
                                            oh_master_lookup.get(base.lower(),{}).get('Vendor Name','')
                                if not fb_vn:
                                    # old_history (1st pass)
                                    fb_vn = oh_history_lookup.get(pkey.lower(),{}).get('Vendor Name','') or \
                                            oh_history_lookup.get(base.lower(),{}).get('Vendor Name','')
                                if not fb_vn:
                                    # old_history_master (2nd pass)
                                    fb_vn = oh_master_lookup.get(pkey.lower(),{}).get('Vendor Name','') or \
                                            oh_master_lookup.get(base.lower(),{}).get('Vendor Name','')
                                if not fb_vn:
                                    # old_history (2nd pass)
                                    fb_vn = oh_history_lookup.get(pkey.lower(),{}).get('Vendor Name','') or \
                                            oh_history_lookup.get(base.lower(),{}).get('Vendor Name','')
                                if not fb_vn:
                                    # logic2_matched_partial_history (pallet or gen_pallet_id)
                                    fb_vn = l2_matched_hist_lookup.get(pkey.lower(),{}).get('Vendor Name','') or \
                                            l2_matched_hist_lookup.get(base.lower(),{}).get('Vendor Name','')
                                if not fb_vn:
                                    # logic2_history (pallet or gen_pallet_id)
                                    fb_vn = l2_history_lookup.get(pkey.lower(),{}).get('Vendor Name','') or \
                                            l2_history_lookup.get(base.lower(),{}).get('Vendor Name','')
                                if fb_vn:
                                    fmt_df.at[idx, 'Vendor Name'] = fb_vn
                                    fmt_df.at[idx, 'COO'] = vendor_country_map.get(fb_vn.lower(), '')
                            else:
                                if not str(fmt_df.at[idx,'COO']).strip() or str(fmt_df.at[idx,'COO']).strip() in ('nan','None',''):
                                    fmt_df.at[idx, 'COO'] = vendor_country_map.get(vn.lower(), '')

                            for _fc2 in ['Invoice Number', 'Grn Number']:
                                _cur = str(fmt_df.at[idx, _fc2]).strip()
                                if not _cur or _cur in ('nan','None',''):
                                    fb = (pallet_invoice_map_fmt if _fc2 == 'Invoice Number' else pallet_grn_map_fmt).get(pkey,'') or \
                                         (pallet_invoice_map_fmt if _fc2 == 'Invoice Number' else pallet_grn_map_fmt).get(base,'')
                                    if not fb:
                                        _map2 = inv_invoice_map_fmt if _fc2 == 'Invoice Number' else inv_grn_map_fmt
                                        _ek2  = 'invoice_number'    if _fc2 == 'Invoice Number' else 'grn_number'
                                        for pe in part_entries:
                                            fb = pe.get(_ek2,'') or _map2.get(pe.get('gen_pallet',''),'')
                                            if fb: break
                                    if not fb:
                                        # old_history_master (1st pass)
                                        fb = oh_master_lookup.get(pkey.lower(),{}).get(_fc2,'') or \
                                             oh_master_lookup.get(base.lower(),{}).get(_fc2,'')
                                    if not fb:
                                        # old_history (1st pass)
                                        fb = oh_history_lookup.get(pkey.lower(),{}).get(_fc2,'') or \
                                             oh_history_lookup.get(base.lower(),{}).get(_fc2,'')
                                    if not fb:
                                        # old_history_master (2nd pass)
                                        fb = oh_master_lookup.get(pkey.lower(),{}).get(_fc2,'') or \
                                             oh_master_lookup.get(base.lower(),{}).get(_fc2,'')
                                    if not fb:
                                        # old_history (2nd pass)
                                        fb = oh_history_lookup.get(pkey.lower(),{}).get(_fc2,'') or \
                                             oh_history_lookup.get(base.lower(),{}).get(_fc2,'')
                                    if not fb:
                                        # logic2_matched_partial_history (pallet or gen_pallet_id)
                                        fb = l2_matched_hist_lookup.get(pkey.lower(),{}).get(_fc2,'') or \
                                             l2_matched_hist_lookup.get(base.lower(),{}).get(_fc2,'')
                                    if not fb:
                                        # logic2_history (pallet or gen_pallet_id)
                                        fb = l2_history_lookup.get(pkey.lower(),{}).get(_fc2,'') or \
                                             l2_history_lookup.get(base.lower(),{}).get(_fc2,'')
                                    if fb: fmt_df.at[idx, _fc2] = fb

                            # Order NO from old_history_master → old_history → l2 histories if blank
                            # RULE: Only fill if Pick Quantity or Allocated is filled
                            _ord_pick  = float(pd.to_numeric(fmt_df.at[idx, 'Pick Quantity'] if 'Pick Quantity' in fmt_df.columns else 0, errors='coerce') or 0)
                            _ord_alloc = float(pd.to_numeric(fmt_df.at[idx, 'Allocated']     if 'Allocated'     in fmt_df.columns else 0, errors='coerce') or 0)
                            _ord_ats   = float(pd.to_numeric(fmt_df.at[idx, 'ATS']           if 'ATS'           in fmt_df.columns else 0, errors='coerce') or 0)
                            ord_n = str(fmt_df.at[idx, 'Order NO']).strip() if 'Order NO' in fmt_df.columns else ''
                            # Only fill Order NO if row has Pick or Allocated — never for ATS-only rows
                            if (_ord_pick > 0 or _ord_alloc > 0) and (not ord_n or ord_n in ('nan','None','')):
                                # old_history_master (1st pass)
                                fb_ord = oh_master_lookup.get(pkey.lower(),{}).get('Order NO','') or \
                                         oh_master_lookup.get(base.lower(),{}).get('Order NO','')
                                if not fb_ord:
                                    # old_history (1st pass)
                                    fb_ord = oh_history_lookup.get(pkey.lower(),{}).get('Order NO','') or \
                                             oh_history_lookup.get(base.lower(),{}).get('Order NO','')
                                if not fb_ord:
                                    # old_history_master (2nd pass)
                                    fb_ord = oh_master_lookup.get(pkey.lower(),{}).get('Order NO','') or \
                                             oh_master_lookup.get(base.lower(),{}).get('Order NO','')
                                if not fb_ord:
                                    # old_history (2nd pass)
                                    fb_ord = oh_history_lookup.get(pkey.lower(),{}).get('Order NO','') or \
                                             oh_history_lookup.get(base.lower(),{}).get('Order NO','')
                                if not fb_ord:
                                    # logic2_matched_partial_history
                                    fb_ord = l2_matched_hist_lookup.get(pkey.lower(),{}).get('Order NO','') or \
                                             l2_matched_hist_lookup.get(base.lower(),{}).get('Order NO','')
                                if not fb_ord:
                                    # logic2_history
                                    fb_ord = l2_history_lookup.get(pkey.lower(),{}).get('Order NO','') or \
                                             l2_history_lookup.get(base.lower(),{}).get('Order NO','')
                                if fb_ord: fmt_df.at[idx, 'Order NO'] = fb_ord

                            for _oh_col in OH_FILL_COLS:
                                if _oh_col in ('Vendor Name','Invoice Number','Grn Number'): continue
                                _cur2 = str(fmt_df.at[idx, _oh_col]).strip() if _oh_col in fmt_df.columns else ''
                                if not _cur2 or _cur2 in ('nan','None',''):
                                    # old_history_master (1st pass)
                                    _oh_v = oh_master_lookup.get(pkey.lower(),{}).get(_oh_col,'') or \
                                            oh_master_lookup.get(base.lower(),{}).get(_oh_col,'')
                                    if not _oh_v:
                                        # old_history (1st pass)
                                        _oh_v = oh_history_lookup.get(pkey.lower(),{}).get(_oh_col,'') or \
                                                oh_history_lookup.get(base.lower(),{}).get(_oh_col,'')
                                    if not _oh_v:
                                        # old_history_master (2nd pass)
                                        _oh_v = oh_master_lookup.get(pkey.lower(),{}).get(_oh_col,'') or \
                                                oh_master_lookup.get(base.lower(),{}).get(_oh_col,'')
                                    if not _oh_v:
                                        # old_history (2nd pass)
                                        _oh_v = oh_history_lookup.get(pkey.lower(),{}).get(_oh_col,'') or \
                                                oh_history_lookup.get(base.lower(),{}).get(_oh_col,'')
                                    if not _oh_v:
                                        # logic2_matched_partial_history (pallet or gen_pallet_id)
                                        _oh_v = l2_matched_hist_lookup.get(pkey.lower(),{}).get(_oh_col,'') or \
                                                l2_matched_hist_lookup.get(base.lower(),{}).get(_oh_col,'')
                                    if not _oh_v:
                                        # logic2_history (pallet or gen_pallet_id)
                                        _oh_v = l2_history_lookup.get(pkey.lower(),{}).get(_oh_col,'') or \
                                                l2_history_lookup.get(base.lower(),{}).get(_oh_col,'')
                                    if not _oh_v:
                                        # master_partial_data third (pallet or gen_pallet_id)
                                        for pe in part_entries:
                                            _pe_v = str(pe.get(_oh_col.lower().replace(' ','_'), '')).strip()
                                            if _pe_v and _pe_v not in ('nan','None',''):
                                                _oh_v = _pe_v; break
                                    if _oh_v and _oh_col in fmt_df.columns:
                                        fmt_df.at[idx, _oh_col] = _oh_v

                        # ── Destination Country blank fill (any DB pallet/gen_pallet_id match) ──
                        # RULE: Only update if Pick Quantity OR Allocated has a value (quantity > 0)
                        # Destination Country and Order NO must NEVER be filled for ATS/Damage rows
                        _dc_fixed = 0
                        _pick_col_dc  = 'Pick Quantity' if 'Pick Quantity' in fmt_df.columns else None
                        _alloc_col_dc = 'Allocated'     if 'Allocated'     in fmt_df.columns else None
                        for _dc_idx, _dc_row in fmt_df.iterrows():
                            _dc_val = str(fmt_df.at[_dc_idx, 'Destination Country']).strip()
                            if _dc_val and _dc_val not in ('nan', 'None', ''):
                                continue  # already filled
                            # ── Gate: only fill if Pick Quantity OR Allocated > 0 ──
                            _dc_pick  = float(pd.to_numeric(fmt_df.at[_dc_idx, _pick_col_dc]  if _pick_col_dc  else 0, errors='coerce') or 0)
                            _dc_alloc = float(pd.to_numeric(fmt_df.at[_dc_idx, _alloc_col_dc] if _alloc_col_dc else 0, errors='coerce') or 0)
                            if _dc_pick <= 0 and _dc_alloc <= 0:
                                continue  # ATS / damage / balance row — do NOT fill Destination Country
                            _dc_pkey = str(fmt_df.at[_dc_idx, 'Pallet']).strip()
                            _dc_base = _base_pallet(_dc_pkey)
                            fb_dc = country_lookup.get(_dc_pkey.lower(), '') or \
                                    country_lookup.get(_dc_base.lower(), '')
                            if not fb_dc:
                                _dc_parts = partial_map.get(_dc_base, []) or partial_map.get(_dc_pkey, [])
                                for _dpe in _dc_parts:
                                    fb_dc = country_lookup.get(_dpe.get('gen_pallet','').lower(), '') or \
                                            _dpe.get('country', '')
                                    if fb_dc: break
                            if not fb_dc:
                                _dc_ord = str(fmt_df.at[_dc_idx, 'Order NO']).strip()
                                if _dc_ord and _dc_ord not in ('nan', 'None', ''):
                                    fb_dc = load_id_country_map.get(_dc_ord, '')
                            if fb_dc and fb_dc not in ('nan', 'None', ''):
                                fmt_df.at[_dc_idx, 'Destination Country'] = fb_dc
                                _dc_fixed += 1
                        if _dc_fixed > 0:
                            st.info(f"🌍 Destination Country: **{_dc_fixed}** blank rows filled from DB lookup")

                        # ── FINAL SAFETY: Force-blank Destination Country + Order NO for ATS/Damage rows ──
                        # Any row where Pick Quantity=0 AND Allocated=0 must have DC and Order NO blank
                        if 'Pick Quantity' in fmt_df.columns and 'Allocated' in fmt_df.columns:
                            _pick_s  = pd.to_numeric(fmt_df['Pick Quantity'], errors='coerce').fillna(0)
                            _alloc_s = pd.to_numeric(fmt_df['Allocated'],     errors='coerce').fillna(0)
                            _ats_mask = (_pick_s <= 0) & (_alloc_s <= 0)
                            if 'Destination Country' in fmt_df.columns:
                                fmt_df.loc[_ats_mask, 'Destination Country'] = ''
                            if 'Order NO' in fmt_df.columns:
                                fmt_df.loc[_ats_mask, 'Order NO'] = ''

                        # ── Auto-fix ATS: Actual Qty = Pick + Allocated + Damage + ATS ──────
                        # NOTE: Only apply to rows where Pick Quantity AND Allocated are both blank/zero
                        #       (i.e. pure ATS rows). Never overwrite Pick/Allocated rows with ATS.
                        _autofix_count = 0
                        for _af_idx, _af_row in fmt_df.iterrows():
                            _af_act   = float(pd.to_numeric(_af_row.get('Actual Qty',    0), errors='coerce') or 0)
                            _af_pick  = float(pd.to_numeric(_af_row.get('Pick Quantity', 0), errors='coerce') or 0)
                            _af_alloc = float(pd.to_numeric(_af_row.get('Allocated',     0), errors='coerce') or 0)
                            _af_dmg   = sum(
                                (lambda v: float(v) if pd.notna(v) else 0.0)(pd.to_numeric(_af_row.get(_rmk, 0), errors='coerce'))
                                for _rmk in damage_remarks
                            )
                            _af_ats  = float(pd.to_numeric(_af_row.get('ATS', 0), errors='coerce') or 0)
                            _af_acc  = round(_af_pick + _af_alloc + _af_dmg + _af_ats, 3)
                            _af_diff = round(_af_act - _af_acc, 3)
                            if abs(_af_diff) > 0.01:
                                # ── Do NOT touch rows that have Pick Quantity or Allocated filled ──
                                if _af_pick > 0 or _af_alloc > 0:
                                    continue
                                fmt_df.at[_af_idx, 'ATS'] = max(0.0, round(_af_act - _af_pick - _af_alloc - _af_dmg, 3))
                                _autofix_count += 1
                        if _autofix_count > 0:
                            st.info(f"🔧 Auto-Fix: **{_autofix_count}** rows — ATS adjusted (Actual = Pick + Alloc + Dmg + ATS)")

                        # ── Inventory vs Report qty check ─────────────────────────────────
                        inv_total_qty = pd.to_numeric(inv_data[_inv_aq_col], errors='coerce').fillna(0).sum()
                        rpt_total_qty = pd.to_numeric(fmt_df['Actual Qty'],   errors='coerce').fillna(0).sum()
                        qty_match     = abs(inv_total_qty - rpt_total_qty) < 0.01

                        total_pick_qty  = pd.to_numeric(fmt_df['Pick Quantity'], errors='coerce').fillna(0).sum()
                        total_alloc_qty = pd.to_numeric(fmt_df['Allocated'],     errors='coerce').fillna(0).sum()
                        total_ats_qty   = pd.to_numeric(fmt_df['ATS'],           errors='coerce').fillna(0).sum()
                        total_dmg_qty   = sum(pd.to_numeric(fmt_df[r], errors='coerce').fillna(0).sum() for r in damage_remarks)
                        accounted       = total_pick_qty + total_alloc_qty + total_dmg_qty + total_ats_qty
                        total_lines     = len(fmt_df)
                        partial_lines   = len([r for r in fmt_df['Pallet'].astype(str) if '-P' in r])

                        sc1,sc2,sc3,sc4,sc5 = st.columns(5)
                        sc1.metric("Total Lines",   total_lines)
                        sc2.metric("Pick Qty",      int(total_pick_qty))
                        sc3.metric("Allocated Qty", int(total_alloc_qty))
                        sc4.metric("ATS Qty",       int(total_ats_qty))
                        sc5.metric("Damage Qty",    int(total_dmg_qty))

                        if qty_match:
                            st.success(f"✅ Qty Match: Inv({int(inv_total_qty)}) = Report({int(rpt_total_qty)})")
                        else:
                            st.warning(f"⚠️ Qty Mismatch: Inv({int(inv_total_qty)}) ≠ Report({int(rpt_total_qty)}) | Diff={int(inv_total_qty-rpt_total_qty)}")

                        if abs(inv_total_qty - accounted) < 0.01:
                            st.success(f"✅ Reconciled: Pick({int(total_pick_qty)}) + Alloc({int(total_alloc_qty)}) + ATS({int(total_ats_qty)}) + Dmg({int(total_dmg_qty)}) = {int(accounted)}")
                        else:
                            st.warning(f"⚠️ Unaccounted: {int(inv_total_qty-accounted)} | Accounted={int(accounted)} vs Inv={int(inv_total_qty)}")

                        # ── Qty_Mismatch Fix: old_history + logic2_matched_partial_history ──────
                        # Sources: (1) old_history  (2) logic2_matched_partial_history
                        # Per source: pallet (+ gen_pallet_id for L2mph) → collect ALL balance_qty values
                        # inventory Actual Qty == any balance_qty → exact match → update Actual Qty & ATS only
                        _qm_fixed_count = 0
                        try:
                            # ── Helper: add rows from a df into the combined bal-sets ──────────
                            # _qm_bal_sets:  {pallet_lower → set(bal_rounded)}
                            # _qm_bal_vals:  {(pallet_lower, bal_rounded) → float value}
                            _qm_bal_sets = {}
                            _qm_bal_vals = {}

                            def _qm_add_rows(df, pal_db_key, bal_db_key, extra_pal_db_key=None):
                                """Feed rows from df into _qm_bal_sets / _qm_bal_vals."""
                                if df is None or df.empty:
                                    return
                                _dc = {str(c).strip().lower(): str(c).strip() for c in df.columns}
                                _pc  = _dc.get(pal_db_key, pal_db_key)
                                _bc  = _dc.get(bal_db_key, bal_db_key)
                                _gpc = _dc.get(extra_pal_db_key, None) if extra_pal_db_key else None
                                if _bc not in df.columns:
                                    return
                                df[_bc] = pd.to_numeric(df[_bc], errors='coerce').fillna(0)
                                for _, _r in df.iterrows():
                                    _b = float(_r.get(_bc, 0) or 0)
                                    if _b <= 0:
                                        continue
                                    _brnd = round(_b, 6)
                                    # collect all pallet keys for this row
                                    _pkeys = []
                                    if _pc in df.columns:
                                        _pv = str(_r.get(_pc, '')).strip().lower()
                                        if _pv and _pv not in ('nan', 'none', ''):
                                            _pkeys.append(_pv)
                                    if _gpc and _gpc in df.columns:
                                        _gpv = str(_r.get(_gpc, '')).strip().lower()
                                        if _gpv and _gpv not in ('nan', 'none', ''):
                                            _pkeys.append(_gpv)
                                    for _pk in _pkeys:
                                        if _pk not in _qm_bal_sets:
                                            _qm_bal_sets[_pk] = set()
                                        _qm_bal_sets[_pk].add(_brnd)
                                        _qm_bal_vals[(_pk, _brnd)] = _b

                            # Source 1: old_history — pallet column
                            _oh_fix_df = DBManager.read_table("old_history")
                            _qm_add_rows(_oh_fix_df, pal_db_key='pallet', bal_db_key='balance_qty')

                            # Source 2: logic2_matched_partial_history — pallet column only
                            try:
                                _l2mph_fix_df = DBManager.read_table("logic2_matched_partial_history")
                                _qm_add_rows(
                                    _l2mph_fix_df,
                                    pal_db_key='pallet',
                                    bal_db_key='balance_qty',
                                )
                            except Exception:
                                pass

                            # ── Apply fixes to fmt_df ─────────────────────────────────────────
                            if _qm_bal_sets:
                                _inv_pal_raw2 = inv_data[_inv_pal_col].astype(str).str.strip()
                                _inv_qty_raw2 = pd.to_numeric(inv_data[_inv_aq_col], errors='coerce').fillna(0)
                                _inv_pal_qty2 = {}
                                for _p2, _q2 in zip(_inv_pal_raw2, _inv_qty_raw2):
                                    _inv_pal_qty2[_p2] = _inv_pal_qty2.get(_p2, 0) + _q2

                                for _fmt_idx, _fmt_row in fmt_df.iterrows():
                                    _fmt_p  = str(_fmt_row.get('Pallet', '')).strip()
                                    _fmt_aq = float(pd.to_numeric(_fmt_row.get('Actual Qty', 0), errors='coerce') or 0)
                                    _inv_aq2 = _inv_pal_qty2.get(_fmt_p, None)
                                    if _inv_aq2 is None:
                                        continue
                                    if abs(_inv_aq2 - _fmt_aq) < 0.01:
                                        continue  # no mismatch
                                    _pkey_low    = _fmt_p.lower()
                                    _inv_rounded = round(_inv_aq2, 6)
                                    # Try exact pallet key first, then base pallet (strip -Pxxxx suffix)
                                    _base_low2   = _base_pallet(_fmt_p).lower()
                                    _bal_set = _qm_bal_sets.get(_pkey_low) or _qm_bal_sets.get(_base_low2)
                                    if not _bal_set:
                                        continue
                                    # Scan ALL balance_qty values for this pallet
                                    _matched_bal = None
                                    for _bc in _bal_set:
                                        if abs(_bc - _inv_rounded) < 0.01:
                                            _matched_bal = (_qm_bal_vals.get((_pkey_low, _bc))
                                                            or _qm_bal_vals.get((_base_low2, _bc)))
                                            break
                                    if _matched_bal is not None:
                                        fmt_df.at[_fmt_idx, 'Actual Qty'] = _matched_bal
                                        fmt_df.at[_fmt_idx, 'ATS']        = _matched_bal
                                        _qm_fixed_count += 1

                        except Exception as _qm_err:
                            st.warning(f"⚠️ Qty_Mismatch fix error: {_qm_err}")

                        if _qm_fixed_count > 0:
                            st.info(f"🔧 Qty_Mismatch Fix: **{_qm_fixed_count}** rows — Actual Qty & ATS updated (old_history + logic2_matched_partial_history balance_qty match)")

                        # ── Mismatch pallets report ────────────────────────────────────────
                        mismatch_pallets = []
                        _inv_pals_raw = inv_data[_inv_pal_col].astype(str).str.strip()
                        _inv_qtys     = pd.to_numeric(inv_data[_inv_aq_col], errors='coerce').fillna(0)
                        inv_pal_qty_map = {}
                        for _p, _q in zip(_inv_pals_raw, _inv_qtys):
                            inv_pal_qty_map[_p] = inv_pal_qty_map.get(_p, 0) + _q
                        rpt_pal_qty_map = {}
                        for _, _rr in fmt_df.iterrows():
                            _rp = str(_rr.get('Pallet','')).strip()
                            _rq = float(pd.to_numeric(_rr.get('Actual Qty',0), errors='coerce') or 0)
                            rpt_pal_qty_map[_rp] = rpt_pal_qty_map.get(_rp, 0) + _rq
                        for _ip, _iq in inv_pal_qty_map.items():
                            _rq2  = rpt_pal_qty_map.get(_ip, 0)
                            _diff = round(_iq - _rq2, 2)
                            if abs(_diff) > 0.01:
                                mismatch_pallets.append({
                                    'Pallet': _ip, 'Raw Pallet': _ip,
                                    'Inventory Actual Qty': _iq,
                                    'Report Actual Qty':    _rq2,
                                    'Difference':           _diff,
                                })

                        # ── Reconciliation report (Actual ≠ Pick+Alloc+Dmg+ATS) ───────────
                        recon_rows = []
                        tally_fail_rows = []
                        for idx, r_row in fmt_df.iterrows():
                            act_q   = float(pd.to_numeric(r_row.get('Actual Qty',    0), errors='coerce') or 0)
                            pick_q  = float(pd.to_numeric(r_row.get('Pick Quantity', 0), errors='coerce') or 0)
                            alloc_q = float(pd.to_numeric(r_row.get('Allocated',     0), errors='coerce') or 0)
                            ats_q   = float(pd.to_numeric(r_row.get('ATS',           0), errors='coerce') or 0)
                            dmg_q   = sum(
                                (lambda v: float(v) if pd.notna(v) else 0.0)(pd.to_numeric(r_row.get(rmk,0), errors='coerce'))
                                for rmk in damage_remarks
                            )
                            total_acc = round(pick_q + alloc_q + dmg_q + ats_q, 3)
                            diff      = round(act_q - total_acc, 2)
                            if abs(diff) > 0.01:
                                _rec = {
                                    'Pallet':          str(r_row.get('Pallet','')),
                                    'Actual Qty':      act_q,
                                    'Pick Quantity':   pick_q,
                                    'Allocated':       alloc_q,
                                    'Damage Qty':      dmg_q,
                                    'ATS':             ats_q,
                                    'Total Accounted': total_acc,
                                    'Difference':      diff,
                                    'Vendor Name':     str(r_row.get('Vendor Name','')),
                                    'Invoice Number':  str(r_row.get('Invoice Number','')),
                                    'Status':          '⚠️ Unaccounted',
                                }
                                recon_rows.append(_rec)
                                tally_fail_rows.append(_rec)

                        recon_df = pd.DataFrame(recon_rows)
                        tally_df = pd.DataFrame(tally_fail_rows)
                        if not recon_df.empty:
                            st.warning(f"⚠️ Reconciliation Issues: **{len(recon_df)}** rows")
                            st.dataframe(recon_df.astype(str), use_container_width=True)

                        # ── Unmatched detail reports ───────────────────────────────────────
                        if l1_unmatched:
                            with st.expander(f"⚠️ Logic 1/2 Unmatched (Picked, no match): {len(l1_unmatched)} rows", expanded=False):
                                st.dataframe(pd.DataFrame(l1_unmatched).astype(str), use_container_width=True)
                        if l3_unmatched:
                            with st.expander(f"ℹ️ Logic 3/4 Unmatched (ATS): {len(l3_unmatched)} rows", expanded=False):
                                st.dataframe(pd.DataFrame(l3_unmatched).astype(str), use_container_width=True)

                        # ── Preview ────────────────────────────────────────────────────────
                        st.dataframe(fmt_df.astype(str), use_container_width=True)

                        # ── Total row ──────────────────────────────────────────────────────
                        total_row = {}
                        for _col in final_cols:
                            if _col in ['Actual Qty','Pick Quantity','Allocated','ATS'] + damage_remarks:
                                total_row[_col] = pd.to_numeric(fmt_df[_col], errors='coerce').fillna(0).sum()
                            elif _col == 'Vendor Name':
                                total_row[_col] = 'TOTAL'
                            else:
                                total_row[_col] = ''
                        fmt_df_with_total = pd.concat([fmt_df, pd.DataFrame([total_row])], ignore_index=True)

                        # ── Excel export ───────────────────────────────────────────────────
                        out_fmt = io.BytesIO()
                        with pd.ExcelWriter(out_fmt, engine='xlsxwriter') as writer:
                            fmt_df_with_total.to_excel(writer, sheet_name='Pick_Report', index=False)
                            wb     = writer.book
                            ws_fmt = writer.sheets['Pick_Report']

                            # ── Summary sheet ──────────────────────────────────────────────
                            ws_summ = wb.add_worksheet('Summary')
                            bold_xl    = wb.add_format({'bold': True, 'font_size': 11})
                            val_xl     = wb.add_format({'font_size': 11, 'num_format': '#,##0'})
                            ok_xl      = wb.add_format({'bold': True, 'font_color': '#27ae60', 'font_size': 11})
                            err_xl     = wb.add_format({'bold': True, 'font_color': '#e74c3c', 'font_size': 11})
                            summ_data = [
                                ('Inventory Total Actual Qty', int(inv_total_qty)),
                                ('Report Total Actual Qty',    int(rpt_total_qty)),
                                ('Qty Match', 'YES ✅' if qty_match else 'NO ⚠️'), ('',''),
                                ('Pick Quantity',   int(total_pick_qty)),
                                ('Allocated Qty',   int(total_alloc_qty)),
                                ('ATS Quantity',    int(total_ats_qty)),
                                ('Damage Quantity', int(total_dmg_qty)),
                                ('Total Accounted', int(accounted)),
                                ('Unaccounted',     int(inv_total_qty - accounted)), ('',''),
                                ('Total Report Lines',    total_lines),
                                ('Partial Lines',         partial_lines), ('',''),
                                ('Qty Mismatch Pallets',  len(mismatch_pallets)),
                                ('Tally Fail Lines',      len(tally_fail_rows)),
                                ('L1/L2 Unmatched',       len(l1_unmatched)),
                                ('L3/L4 ATS Lines',       len(l3_unmatched)),
                            ]
                            ws_summ.set_column(0, 0, 28); ws_summ.set_column(1, 1, 18)
                            for ri, (label, value) in enumerate(summ_data):
                                ws_summ.write(ri, 0, label, bold_xl)
                                if label in ('Qty Mismatch Pallets','Tally Fail Lines','L1/L2 Unmatched'):
                                    ws_summ.write(ri, 1, value, err_xl if (isinstance(value,int) and value>0) else ok_xl)
                                elif isinstance(value, int): ws_summ.write(ri, 1, value, val_xl)
                                elif 'YES' in str(value):    ws_summ.write(ri, 1, value, ok_xl)
                                elif 'NO'  in str(value):    ws_summ.write(ri, 1, value, err_xl)
                                else: ws_summ.write(ri, 1, value)

                            # ── Qty_Mismatch sheet ─────────────────────────────────────────
                            mm_sheet   = wb.add_worksheet('Qty_Mismatch')
                            mm_hdr_xl  = wb.add_format({'bold':True,'bg_color':'#e74c3c','font_color':'#fff','border':1,'font_size':10})
                            mm_ok_xl   = wb.add_format({'bold':True,'bg_color':'#27ae60','font_color':'#fff','font_size':11})
                            mm_row_xl  = wb.add_format({'border':1,'font_size':10})
                            mm_neg_xl  = wb.add_format({'border':1,'font_size':10,'font_color':'#e74c3c','bold':True})
                            if mismatch_pallets:
                                mm_df2 = pd.DataFrame(mismatch_pallets)
                                for ci, col in enumerate(mm_df2.columns):
                                    mm_sheet.write(0, ci, col, mm_hdr_xl); mm_sheet.set_column(ci, ci, 22)
                                for ri2, row2 in mm_df2.iterrows():
                                    for ci2, val2 in enumerate(row2):
                                        use_xl = mm_neg_xl if (mm_df2.columns[ci2]=='Difference' and float(val2 or 0)!=0) else mm_row_xl
                                        mm_sheet.write(ri2+1, ci2, val2, use_xl)
                            else:
                                mm_sheet.write(0, 0, '✅ No Qty Mismatches Found', mm_ok_xl); mm_sheet.set_column(0,0,35)

                            # ── Reconciliation_Report sheet ────────────────────────────────
                            if not recon_df.empty:
                                rs = wb.add_worksheet('Reconciliation_Report')
                                r_hdr = wb.add_format({'bold':True,'bg_color':'#f39c12','font_color':'#fff','border':1,'font_size':10})
                                r_row = wb.add_format({'border':1,'font_size':10})
                                r_bad = wb.add_format({'border':1,'font_size':10,'font_color':'#e74c3c','bold':True})
                                for ci, col in enumerate(recon_df.columns):
                                    rs.write(0, ci, col, r_hdr); rs.set_column(ci, ci, 20)
                                for ri2, row2 in recon_df.iterrows():
                                    for ci2, val2 in enumerate(row2):
                                        rs.write(ri2+1, ci2, str(val2), r_bad if recon_df.columns[ci2]=='Difference' else r_row)

                            # ── Tally_Fail_Report sheet ────────────────────────────────────
                            ts = wb.add_worksheet('Tally_Fail_Report')
                            t_hdr = wb.add_format({'bold':True,'bg_color':'#8e44ad','font_color':'#fff','border':1,'font_size':10})
                            t_ok  = wb.add_format({'bold':True,'bg_color':'#27ae60','font_color':'#fff','font_size':11})
                            t_row = wb.add_format({'border':1,'font_size':10})
                            t_bad = wb.add_format({'border':1,'font_size':10,'font_color':'#e74c3c','bold':True,'bg_color':'#fff0f0'})
                            if not tally_df.empty:
                                for ci, col in enumerate(tally_df.columns):
                                    ts.write(0, ci, col, t_hdr); ts.set_column(ci, ci, 18)
                                for ri2, row2 in tally_df.iterrows():
                                    for ci2, val2 in enumerate(row2):
                                        ts.write(ri2+1, ci2, str(val2), t_bad if tally_df.columns[ci2]=='Difference' else t_row)
                                st.warning(f"⚠️ Tally Fail: **{len(tally_df)}** rows — Actual ≠ Pick + Alloc + Dmg + ATS")
                            else:
                                ts.write(0, 0, '✅ All Lines Tallied — Actual Qty = Pick + Allocated + Damage + ATS', t_ok)
                                ts.set_column(0, 0, 55)

                            # ── Pick_Report sheet formatting ───────────────────────────────
                            hdr_xl   = wb.add_format({'bold':True,'bg_color':'#1a1a1a','font_color':'#ffffff','border':1,'font_size':10})
                            pick_xl  = wb.add_format({'bg_color':'#E8F5E9','border':1,'font_size':10})
                            alloc_xl = wb.add_format({'bg_color':'#E3F2FD','border':1,'font_size':10,'bold':True})
                            dmg_xl   = wb.add_format({'bg_color':'#FFE0E0','border':1,'font_size':10})
                            ats_xl   = wb.add_format({'bg_color':'#E3F2FD','border':1,'font_size':10,'bold':True})
                            vnd_xl   = wb.add_format({'bg_color':'#FFF9C4','border':1,'font_size':10})
                            norm_xl  = wb.add_format({'border':1,'font_size':10})
                            for ci, col_name in enumerate(final_cols):
                                ws_fmt.write(0, ci, col_name, hdr_xl); ws_fmt.set_column(ci, ci, 15)
                                for ri in range(1, len(fmt_df)+1):
                                    val = str(fmt_df_with_total.iloc[ri-1][col_name])
                                    if   col_name in ['Pick Quantity','Destination Country','Order NO']: ws_fmt.write(ri,ci,val,pick_xl)
                                    elif col_name == 'Allocated':    ws_fmt.write(ri,ci,val,alloc_xl)
                                    elif col_name in damage_remarks: ws_fmt.write(ri,ci,val,dmg_xl)
                                    elif col_name == 'ATS':          ws_fmt.write(ri,ci,val,ats_xl)
                                    elif col_name in ['Vendor Name','COO']: ws_fmt.write(ri,ci,val,vnd_xl)
                                    else:                            ws_fmt.write(ri,ci,val,norm_xl)
                            total_row_idx = len(fmt_df) + 1
                            tot_num_xl = wb.add_format({'bold':True,'bg_color':'#1a1a1a','font_color':'#FFD700','border':1,'font_size':10,'num_format':'#,##0'})
                            tot_str_xl = wb.add_format({'bold':True,'bg_color':'#1a1a1a','font_color':'#FFD700','border':1,'font_size':10})
                            for ci, col_name in enumerate(final_cols):
                                val = fmt_df_with_total.iloc[-1][col_name]
                                try:    ws_fmt.write(total_row_idx, ci, float(val) if val!='' else '', tot_num_xl)
                                except: ws_fmt.write(total_row_idx, ci, str(val), tot_str_xl)
                            ws_fmt.freeze_panes(1, 0)

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
                'Item Id': 'item_id', 'Invoice Number1': 'invoice_number1',
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
