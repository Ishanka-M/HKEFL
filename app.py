import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import io
import time

# --- 1. System Config & Auth ---
st.set_page_config(page_title="Advanced WMS Picking System", layout="wide", page_icon="📦")

# --- Master_Pick_Data CORRECT Headers ---
MASTER_PICK_HEADERS = [
    'Wh Id', 'Client Code', 'Pallet', 'Invoice Number', 'Location Id', 'Item Number',
    'Description', 'Lot Number', 'Actual Qty', 'Unavailable Qty', 'Uom', 'Status',
    'Mlp', 'Stored Attribute Id', 'Fifo Date', 'Expiration Date', 'Grn Number',
    'Gate Pass Id', 'Cust Dec No', 'Color', 'Size', 'Style', 'Supplier', 'Plant',
    'Client So', 'Client So Line', 'Po', 'Cust Dec', 'Customer Ref Number', 'Item Id',
    'Invoice Number1', 'Transaction', 'Order Type', 'Order Number', 'Store Order Number',
    'Customer Po Number', 'Partial Order Flag', 'Order Date', 'Load Id', 'Asn Number',
    'Po Number', 'Supplier Hu', 'New Item Number', 'Asn Line Number',
    'Received Gross Weight', 'Current Gross Weight', 'Received Net Weight',
    'Current Net Weight', 'Supplier Desc', 'Cbm', 'Container Type', 'Display Item Number',
    'Old Item Number', 'Inventory Type', 'Type', 'Qc', 'Vendor Name', 'Manufacture Date',
    'Suom', 'S Qty', 'Pick Id', 'Downloaded Date',
    'Batch ID', 'SO Number', 'Generated Load ID', 'Country Name', 'Pick Quantity', 'Remark'
]

SHEET_HEADERS = {
    "Load_History": ['Batch ID', 'Generated Load ID', 'SO Number', 'Country Name', 'SHIP MODE', 'Date', 'Pick Status'],
    "Summary_Data": ['Batch ID', 'SO Number', 'Load ID', 'UPC', 'Country', 'Ship Mode', 'Requested', 'Picked', 'Variance', 'Status'],
    "Master_Partial_Data": ['Batch ID', 'SO Number', 'Pallet', 'Supplier', 'Load ID', 'Country Name', 'Actual Qty', 'Partial Qty', 'Gen Pallet ID'],
    "Master_Pick_Data": MASTER_PICK_HEADERS,
    "Damage_Items": ['Pallet', 'Actual Qty', 'Remark', 'Date Added', 'Added By']
}

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

def get_master_workbook():
    client = get_gsheet_client()
    return client.open_by_url(st.secrets["general"]["spreadsheet_url"])

def get_or_create_sheet(sh, name, headers):
    try:
        ws = sh.worksheet(name)
        if not ws.get_all_values():
            ws.append_row(headers)
    except:
        ws = sh.add_worksheet(title=name, rows="5000", cols=str(max(20, len(headers) + 5)))
        ws.append_row(headers)
    return ws

def get_safe_dataframe(sh, sheet_name):
    try:
        ws = sh.worksheet(sheet_name)
        data = ws.get_all_values()
        if not data:
            return pd.DataFrame()
        raw_headers = [str(h).strip() for h in data[0]]
        headers = []
        seen = {}
        for h in raw_headers:
            if h in seen:
                seen[h] += 1
                headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                headers.append(h)
        if len(data) > 1:
            return pd.DataFrame(data[1:], columns=headers)
        else:
            return pd.DataFrame(columns=headers)
    except Exception as e:
        return pd.DataFrame()

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

HK_LOGO_URL = st.secrets.get("general", {}).get("logo_url", "")

def login_section():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if 'role' not in st.session_state:
        st.session_state['role'] = 'user'
    if 'username' not in st.session_state:
        st.session_state['username'] = 'Unknown'

    if not st.session_state['logged_in']:
        # Show logo on main page login screen
        st.markdown(f"""
        <div style="display:flex; justify-content:center; align-items:center; padding: 40px 0 10px 0;">
            <img src="data:image/jpeg;base64,{HK_LOGO_B64}" style="max-width:380px; width:100%;" />
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<h3 style='text-align:center; color:#555; margin-bottom:30px;'>Warehouse Management System</h3>", unsafe_allow_html=True)

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

    # Show logo in sidebar when logged in
    st.sidebar.markdown(f"""
    <div style="text-align:center; padding: 8px 0 12px 0;">
        <img src="data:image/jpeg;base64,{HK_LOGO_B64}" style="max-width:200px; width:100%;" />
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
    Subtract all previously picked quantities from inventory.
    Also subtract damage quantities.
    After subtraction, strictly remove any pallet rows where Actual Qty <= 0.
    """
    try:
        pick_history = get_safe_dataframe(sh, "Master_Pick_Data")
        if not pick_history.empty and 'Actual Qty' in pick_history.columns and 'Pallet' in pick_history.columns:
            pick_history['Actual Qty'] = pd.to_numeric(pick_history['Actual Qty'], errors='coerce').fillna(0)
            pick_summary = pick_history.groupby('Pallet')['Actual Qty'].sum().reset_index()
            pick_summary.columns = ['Pallet', 'Total_Picked']

            inv_df = pd.merge(inv_df, pick_summary, on='Pallet', how='left')
            inv_df['Total_Picked'] = inv_df['Total_Picked'].fillna(0)
            inv_df['Actual Qty'] = pd.to_numeric(inv_df['Actual Qty'], errors='coerce').fillna(0)
            inv_df['Actual Qty'] = inv_df['Actual Qty'] - inv_df['Total_Picked']
            if 'Total_Picked' in inv_df.columns:
                inv_df = inv_df.drop(columns=['Total_Picked'])
    except Exception as e:
        st.warning(f"Inventory Reconcile Error: {e}")

    # Subtract damage quantities
    try:
        dmg_summary = get_damage_pallets(sh)
        if not dmg_summary.empty:
            inv_df = pd.merge(inv_df, dmg_summary, on='Pallet', how='left')
            inv_df['Damage_Qty'] = inv_df['Damage_Qty'].fillna(0)
            inv_df['Actual Qty'] = pd.to_numeric(inv_df['Actual Qty'], errors='coerce').fillna(0)
            inv_df['Actual Qty'] = inv_df['Actual Qty'] - inv_df['Damage_Qty']
            inv_df = inv_df.drop(columns=['Damage_Qty'])
    except Exception as e:
        st.warning(f"Damage Reconcile Error: {e}")

    inv_df['Actual Qty'] = pd.to_numeric(inv_df['Actual Qty'], errors='coerce').fillna(0)
    inv_df = inv_df[inv_df['Actual Qty'] > 0].reset_index(drop=True)
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

def process_picking(inv_df, req_df, batch_id):
    pick_rows, partial_rows, summary = [], [], []

    supplier_col = next((c for c in inv_df.columns if 'supplier' in str(c).lower()), None)
    pick_id_col = next((c for c in inv_df.columns if 'pick id' in str(c).lower() or 'pickid' in str(c).lower()), None)

    temp_inv = inv_df.copy()
    temp_inv['Actual Qty'] = pd.to_numeric(temp_inv['Actual Qty'], errors='coerce').fillna(0)
    temp_inv = temp_inv[temp_inv['Actual Qty'] > 0].reset_index(drop=True)

    for lid in req_df['Generated Load ID'].unique():
        current_reqs = req_df[req_df['Generated Load ID'] == lid]
        so_num = str(current_reqs['SO Number'].iloc[0])
        ship_mode = str(current_reqs['SHIP MODE: (SEA/AIR)'].iloc[0]) if 'SHIP MODE: (SEA/AIR)' in current_reqs.columns else ""

        for _, req in current_reqs.iterrows():
            upc = str(req['Product UPC'])
            needed = float(req['PICK QTY'])
            country = req['Country Name']

            if supplier_col:
                stock = temp_inv[
                    (temp_inv[supplier_col].astype(str) == upc) &
                    (temp_inv['Actual Qty'] > 0)
                ].sort_values(by='Actual Qty', ascending=False)
            else:
                stock = temp_inv[
                    (temp_inv['Supplier'].astype(str) == upc) &
                    (temp_inv['Actual Qty'] > 0)
                ].sort_values(by='Actual Qty', ascending=False)

            picked_qty = 0

            for idx, item in stock.iterrows():
                if needed <= 0:
                    break

                current_avail = float(temp_inv.at[idx, 'Actual Qty'])
                if current_avail <= 0:
                    continue

                take = min(current_avail, needed)

                if take > 0:
                    p_row = item.copy()

                    p_row['Pick Id'] = str(item[pick_id_col]) if pick_id_col else ""
                    p_row['Supplier'] = str(item[supplier_col]) if supplier_col else upc

                    p_row['Batch ID'] = batch_id
                    p_row['SO Number'] = so_num
                    p_row['Generated Load ID'] = lid
                    p_row['Country Name'] = country
                    p_row['Pick Quantity'] = take
                    p_row['Remark'] = ""
                    p_row['Actual Qty'] = take
                    p_row['Order Type'] = "Sample Orders"
                    p_row['Order Number'] = lid
                    p_row['Store Order Number'] = lid
                    p_row['Customer Po Number'] = f"{country}-{lid}"
                    p_row['Load Id'] = lid

                    pick_rows.append(p_row)

                    if take < current_avail:
                        partial_rows.append({
                            'Batch ID': batch_id, 'SO Number': so_num, 'Pallet': item['Pallet'],
                            'Supplier': p_row['Supplier'], 'Load ID': lid,
                            'Country Name': country, 'Actual Qty': current_avail,
                            'Partial Qty': take, 'Gen Pallet ID': f"{item['Pallet']}-P{len(partial_rows)+1:04d}"
                        })

                    temp_inv.at[idx, 'Actual Qty'] -= take
                    needed -= take
                    picked_qty += take

            variance = float(req['PICK QTY']) - picked_qty
            summary.append({
                'Batch ID': batch_id, 'SO Number': so_num, 'Load ID': lid, 'UPC': upc, 'Country': country,
                'Ship Mode': ship_mode,
                'Requested': req['PICK QTY'], 'Picked': picked_qty, 'Variance': variance,
                'Status': 'Fully Picked' if variance == 0 else 'Shortage'
            })

    pick_df = pd.DataFrame(pick_rows)
    if not pick_df.empty:
        pick_df['Actual Qty'] = pd.to_numeric(pick_df['Actual Qty'])
        pick_df = pick_df[pick_df['Actual Qty'] > 0]
        if 'Pick Id' in pick_df.columns:
            pick_df['Pick Id'] = pick_df['Pick Id'].astype(str)
        if 'Supplier' in pick_df.columns:
            pick_df['Supplier'] = pick_df['Supplier'].astype(str)

    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)

def generate_inventory_details_report(inv_df, sh):
    """
    Generate inventory details report showing allocation status.
    Each pallet is checked against Master_Pick_Data and Load_History.
    If one pallet is picked in multiple loads, create one line per load.
    Adds: Batch ID, SO Number, Generated Load ID, Country Name, Pick Quantity, Remark columns.
    """
    try:
        pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
        hist_df = get_safe_dataframe(sh, "Load_History")

        # Start with the base inventory
        report_rows = []

        for _, inv_row in inv_df.iterrows():
            pallet = str(inv_row.get('Pallet', ''))
            actual_qty = pd.to_numeric(inv_row.get('Actual Qty', 0), errors='coerce')

            # Check if this pallet has been picked
            if not pick_df.empty and 'Pallet' in pick_df.columns:
                pallet_picks = pick_df[pick_df['Pallet'].astype(str) == pallet]

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
                    # Not picked
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

    menu = []
    if current_role == 'ADMIN':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report", "🔄 Revert/Delete Picks", "🩹 Damage Items", "⚙️ Admin Settings"]
    elif current_role == 'SYSUSER':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "📋 Inventory Details Report", "🔄 Revert/Delete Picks", "🩹 Damage Items"]
    else:
        menu = ["📊 Dashboard & Tracking", "📋 Inventory Details Report", "🔄 Revert/Delete Picks"]

    choice = st.sidebar.radio("Navigation Menu", menu)
    sh = get_master_workbook()

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

                    # Check inventory Pallet + Actual Qty against Master_Pick_Data
                    # Rule: Only pick if inventory Actual Qty > Master_Pick_Data total Actual Qty for same Pallet
                    # Take only the EXCESS (inventory qty - master picked qty)
                    # If inventory qty <= master picked qty → skip entirely (already fully picked or over-picked)
                    master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
                    if not master_pick_df.empty and 'Pallet' in master_pick_df.columns and 'Actual Qty' in master_pick_df.columns:
                        master_pick_df['Actual Qty'] = pd.to_numeric(master_pick_df['Actual Qty'], errors='coerce').fillna(0)
                        # Sum ALL Actual Qty per Pallet in Master_Pick_Data
                        master_check = master_pick_df.groupby('Pallet')['Actual Qty'].sum().reset_index()
                        master_check.columns = ['Pallet', 'Master_Total_Picked']

                        inv['Actual Qty'] = pd.to_numeric(inv['Actual Qty'], errors='coerce').fillna(0)
                        inv = pd.merge(inv, master_check, on='Pallet', how='left')
                        inv['Master_Total_Picked'] = inv['Master_Total_Picked'].fillna(0)

                        # Available = inventory Actual Qty - total already picked in Master_Pick_Data
                        # Only if inventory qty > master total picked → excess is available
                        # If inventory qty <= master total picked → 0 available (skip)
                        inv['Actual Qty'] = (inv['Actual Qty'] - inv['Master_Total_Picked']).clip(lower=0)
                        inv = inv.drop(columns=['Master_Total_Picked'], errors='ignore')
                        inv = inv[inv['Actual Qty'] > 0].reset_index(drop=True)

                    pick_df, part_df, summ_df = process_picking(inv, req, batch_id)

                    if not pick_df.empty:
                        ws_pick = get_or_create_sheet(sh, "Master_Pick_Data", MASTER_PICK_HEADERS)
                        # Align columns to MASTER_PICK_HEADERS
                        for col in MASTER_PICK_HEADERS:
                            if col not in pick_df.columns:
                                pick_df[col] = ''
                        pick_df_to_save = pick_df[MASTER_PICK_HEADERS]
                        ws_pick.append_rows(pick_df_to_save.astype(str).replace('nan', '').values.tolist())

                    if not part_df.empty:
                        ws_part = get_or_create_sheet(sh, "Master_Partial_Data", SHEET_HEADERS["Master_Partial_Data"])
                        ws_part.append_rows(part_df.astype(str).replace('nan', '').values.tolist())

                    ws_summ = get_or_create_sheet(sh, "Summary_Data", SHEET_HEADERS["Summary_Data"])
                    ws_summ.append_rows(summ_df.astype(str).replace('nan', '').values.tolist())

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
                st.balloons()

    # ==========================================
    # TAB 2: DASHBOARD & TRACKING
    # ==========================================
    elif choice == "📊 Dashboard & Tracking":
        col_t1, col_t2 = st.columns([4, 1])
        col_t1.title("📊 Load Tracking & Dashboard")
        if col_t2.button("🔄 Refresh Data", use_container_width=True):
            st.rerun()

        hist_df = get_safe_dataframe(sh, "Load_History")
        summ_df = get_safe_dataframe(sh, "Summary_Data")
        pick_df = get_safe_dataframe(sh, "Master_Pick_Data")

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
            st.subheader("📦 Active Load ID Cards")
            st.caption("Cancel සහ Completed Load IDs මෙහි නොපෙන්වයි.")

            if not hist_df.empty and 'Generated Load ID' in hist_df.columns and 'Pick Status' in hist_df.columns:
                # Show only active loads (exclude Cancelled and Completed)
                active_loads = hist_df[
                    ~hist_df['Pick Status'].astype(str).isin(['Cancelled', 'Completed'])
                ].copy()

                if active_loads.empty:
                    st.info("සියලු Loads Completed හෝ Cancelled වී ඇත.")
                else:
                    # Display in card grid: 4 per row (compact)
                    load_ids = active_loads['Generated Load ID'].dropna().unique().tolist()

                    for i in range(0, len(load_ids), 4):
                        cols = st.columns(4)
                        for j, lid in enumerate(load_ids[i:i+4]):
                            with cols[j]:
                                load_row = active_loads[active_loads['Generated Load ID'] == lid].iloc[0]
                                status = load_row.get('Pick Status', 'Pending')
                                so_num = load_row.get('SO Number', '-')
                                country = load_row.get('Country Name', '-')
                                ship_mode = load_row.get('SHIP MODE', '-')
                                date = str(load_row.get('Date', '-'))[:10]

                                # Pick count for this load
                                if not pick_df.empty and 'Load Id' in pick_df.columns:
                                    load_picks = pick_df[pick_df['Load Id'].astype(str) == str(lid)]
                                    pick_count = len(load_picks)
                                    pick_qty = pd.to_numeric(load_picks.get('Actual Qty', pd.Series()), errors='coerce').sum()
                                else:
                                    pick_count = 0
                                    pick_qty = 0

                                status_bg = {'Pending': '#fff3cd', 'Processing': '#cce5ff'}.get(status, '#f8f9fa')
                                status_color = {'Pending': '#856404', 'Processing': '#004085'}.get(status, '#333')
                                status_dot = {'Pending': '🟡', 'Processing': '🔵'}.get(status, '⚪')

                                st.markdown(f"""
                                <div style="border:1px solid #ddd; border-radius:8px; padding:10px 12px; margin-bottom:8px; background:#fff; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
                                    <div style="font-weight:700; font-size:12px; color:#222; margin-bottom:5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="{lid}">📦 {lid}</div>
                                    <div style="display:inline-block; background:{status_bg}; color:{status_color}; font-size:10px; font-weight:600; padding:2px 7px; border-radius:10px; margin-bottom:5px;">{status_dot} {status}</div>
                                    <div style="font-size:11px; color:#555; line-height:1.6;">
                                        <div>📋 <b>SO:</b> {so_num}</div>
                                        <div>🌍 {country} &nbsp;|&nbsp; 🚢 {ship_mode}</div>
                                        <div>📅 {date}</div>
                                        <div>🧾 <b>{pick_count}</b> lines &nbsp;|&nbsp; Qty: <b>{int(pick_qty)}</b></div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)

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

            # --- Advanced Search & Status Update ---
            st.subheader("🔍 Advanced Search & Status Update")
            col_s1, col_s2, col_s3 = st.columns([2, 2, 1])

            search_by = col_s1.selectbox("🔎 Search By:", ["Load Id", "Pallet", "Supplier (Product UPC)", "SO Number"])

            search_term = None
            if search_by == "Load Id":
                if 'Generated Load ID' in hist_df.columns:
                    search_term = col_s2.selectbox("Select Load ID:", hist_df['Generated Load ID'].dropna().unique())

                    if search_term:
                        current_status = hist_df[hist_df['Generated Load ID'] == search_term]['Pick Status'].iloc[0] if 'Pick Status' in hist_df.columns else "Pending"
                        status_options = ["Pending", "Processing", "Completed", "Cancelled"]
                        safe_index = status_options.index(current_status) if current_status in status_options else 0
                        new_status = col_s3.selectbox("📝 Update Pick Status:", status_options, index=safe_index)

                        if col_s3.button("Update Status"):
                            ws_hist = sh.worksheet("Load_History")
                            cell = ws_hist.find(search_term)
                            if cell:
                                ws_hist.update_cell(cell.row, 7, new_status)
                                st.success(f"Status updated to {new_status}!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Error: Load ID cell not found in Google Sheet.")
                else:
                    st.warning("Generated Load ID column not found.")
            else:
                search_term = col_s2.text_input(f"Enter {search_by}:")
                col_s3.write("")

            if search_term:
                st.markdown(f"### Results for {search_by}: `{search_term}`")
                tab_v, tab_p = st.tabs(["📉 Summary / Variance", "📦 Picked Items Detail"])

                col_map_pick = {"Load Id": "Load Id", "Pallet": "Pallet", "Supplier (Product UPC)": "Supplier", "SO Number": "SO Number"}
                col_map_summ = {"Load Id": "Load ID", "Pallet": None, "Supplier (Product UPC)": "UPC", "SO Number": "SO Number"}

                with tab_p:
                    if not pick_df.empty and col_map_pick[search_by] in pick_df.columns:
                        search_col = col_map_pick[search_by]
                        if search_by == "Load Id":
                            filtered_picks = pick_df[pick_df[search_col].astype(str) == str(search_term)]
                        else:
                            filtered_picks = pick_df[pick_df[search_col].astype(str).str.contains(str(search_term), case=False, na=False)]
                        st.dataframe(filtered_picks.astype(str), use_container_width=True)
                    else:
                        st.write("No pick data found for this search.")

                with tab_v:
                    if not summ_df.empty and col_map_summ[search_by]:
                        search_col_s = col_map_summ[search_by]
                        if search_col_s in summ_df.columns:
                            if search_by == "Load Id":
                                filtered_summ = summ_df[summ_df[search_col_s].astype(str) == str(search_term)]
                            else:
                                filtered_summ = summ_df[summ_df[search_col_s].astype(str).str.contains(str(search_term), case=False, na=False)]

                            st.dataframe(filtered_summ.astype(str), use_container_width=True)

                            if 'Variance' in filtered_summ.columns:
                                filtered_summ['Variance'] = pd.to_numeric(filtered_summ['Variance'], errors='coerce')
                                shortages = filtered_summ[filtered_summ['Variance'] > 0]
                                if not shortages.empty:
                                    st.warning("⚠️ Warning: Shortages detected!")
                                    cols_to_show = [c for c in ['UPC', 'Requested', 'Picked', 'Variance'] if c in shortages.columns]
                                    st.table(shortages[cols_to_show])
                        else:
                            st.write(f"Column '{search_col_s}' not found in Summary Data.")
                    else:
                        if not col_map_summ[search_by]:
                            st.info("Summary view is not available for Pallet search.")
                        else:
                            st.write("No summary data found.")

    # ==========================================
    # TAB 3: INVENTORY DETAILS REPORT
    # ==========================================
    elif choice == "📋 Inventory Details Report":
        st.title("📋 Inventory Details Report")
        st.info("""
        Inventory file upload කරහම එය Master_Pick_Data සහ Load_History සමඟ compare කර allocation status සහිත report එකක් generate වේ.
        - **Picked**: Pallet pick allocate වී ඇත
        - **Available**: Pallet pick allocate වී නොමැත
        - එකම Pallet pick ගොඩකට allocate වී ඇත්නම් pick ගානට lines update වේ
        """)

        inv_report_file = st.file_uploader("Upload Inventory File", type=['csv', 'xlsx'], key="inv_report_uploader")

        if inv_report_file:
            if st.button("🔍 Generate Inventory Details Report", type="primary", use_container_width=True):
                with st.spinner("Generating Report..."):
                    inv_data = pd.read_csv(inv_report_file) if inv_report_file.name.endswith('.csv') else pd.read_excel(inv_report_file)

                    report_df = generate_inventory_details_report(inv_data, sh)

                    if not report_df.empty:
                        st.success(f"✅ Report generated! Total rows: {len(report_df)}")

                        # Show summary
                        col_r1, col_r2, col_r3 = st.columns(3)
                        if 'Allocation Status' in report_df.columns:
                            picked_count = len(report_df[report_df['Allocation Status'] == 'Picked'])
                            avail_count = len(report_df[report_df['Allocation Status'] == 'Available'])
                            col_r1.metric("Total Pallets (Lines)", len(report_df))
                            col_r2.metric("Picked Lines", picked_count)
                            col_r3.metric("Available Pallets", avail_count)

                        st.dataframe(report_df.astype(str), use_container_width=True)

                        # Download
                        out_rpt = io.BytesIO()
                        with pd.ExcelWriter(out_rpt, engine='xlsxwriter') as writer:
                            report_df.to_excel(writer, sheet_name='Inventory_Details', index=False)

                        st.download_button(
                            "⬇️ Download Inventory Details Report",
                            data=out_rpt.getvalue(),
                            file_name=f"Inventory_Details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel",
                            use_container_width=True
                        )
                    else:
                        st.warning("Report data generate කිරීම අසාර්ථක විය.")

    # ==========================================
    # TAB 4: REVERT / DELETE PICKS
    # ==========================================
    elif choice == "🔄 Revert/Delete Picks":
        st.title("🔄 Revert / Delete Picked Data")

        del_tab1, del_tab2 = st.tabs(["📁 Upload File to Delete", "🆔 Delete by Load ID Only"])

        # --- Option 1: Upload file ---
        with del_tab1:
            st.info("Load ID, Pallet සහ Actual Qty අඩංගු Excel හෝ CSV ගොනුවක් Upload කිරීමෙන් එම දත්ත Master_Pick_Data සහ Load_History එකෙන් මකා දැමිය හැක.")

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
                            load_ids_to_delete = del_df['LOAD ID'].astype(str).str.strip().unique().tolist()

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

                                # Delete from Load_History as well
                                hist_df = get_safe_dataframe(sh, "Load_History")
                                if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
                                    filtered_hist = hist_df[~hist_df['Generated Load ID'].astype(str).isin(load_ids_to_delete)]
                                    if len(filtered_hist) < len(hist_df):
                                        ws_hist = sh.worksheet("Load_History")
                                        ws_hist.clear()
                                        ws_hist.append_row(SHEET_HEADERS["Load_History"])
                                        if not filtered_hist.empty:
                                            ws_hist.append_rows(filtered_hist.astype(str).replace('nan', '').values.tolist())

                                st.success(f"✅ සාර්ථකව Master_Pick_Data සහ Load_History වලින් records {deleted_count} ක් මකා දමන ලදී!")
                                st.balloons()
                            else:
                                st.warning("⚠️ Upload කල දත්ත හා ගැලපෙන වාර්තා Master_Pick_Data හි හමු නොවීය.")
                        else:
                            st.error("දැනට Master_Pick_Data හි දත්ත නොමැත.")

        # --- Option 2: Delete by Load ID only ---
        with del_tab2:
            st.info("Load ID එකක් ටයිප් කිරීමෙන් ඒ Load ID එකට අදාල සියලු data Master_Pick_Data සහ Load_History වලින් delete කළ හැක.")

            del_load_id = st.text_input("🆔 Enter Load ID to Delete:")

            if del_load_id:
                # Preview what will be deleted
                master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
                if not master_pick_df.empty and 'Load Id' in master_pick_df.columns:
                    preview = master_pick_df[master_pick_df['Load Id'].astype(str).str.strip() == del_load_id.strip()]
                    if not preview.empty:
                        st.warning(f"⚠️ Load ID **{del_load_id}** සඳහා {len(preview)} records මකා දැමෙනු ඇත.")
                        st.dataframe(preview.astype(str), use_container_width=True)
                    else:
                        st.info(f"Load ID **{del_load_id}** සඳහා Master_Pick_Data හි records හමු නොවීය.")

                if st.button("🗑️ Delete by Load ID", type="primary"):
                    with st.spinner("Deleting..."):
                        master_pick_df = get_safe_dataframe(sh, "Master_Pick_Data")
                        deleted_pick = 0
                        deleted_hist = 0

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

                        hist_df = get_safe_dataframe(sh, "Load_History")
                        if not hist_df.empty and 'Generated Load ID' in hist_df.columns:
                            filtered_hist = hist_df[hist_df['Generated Load ID'].astype(str).str.strip() != del_load_id.strip()]
                            deleted_hist = len(hist_df) - len(filtered_hist)

                            ws_hist = sh.worksheet("Load_History")
                            ws_hist.clear()
                            ws_hist.append_row(SHEET_HEADERS["Load_History"])
                            if not filtered_hist.empty:
                                ws_hist.append_rows(filtered_hist.astype(str).replace('nan', '').values.tolist())

                        st.success(f"✅ Load ID **{del_load_id}** — Master_Pick_Data: {deleted_pick} records, Load_History: {deleted_hist} records මකා දමන ලදී!")
                        st.balloons()

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
                            st.balloons()

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
