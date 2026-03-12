import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

# — 1. System Config & Auth —

st.set_page_config(page_title=“Advanced WMS Picking System”, layout=“wide”, page_icon=“📦”)

# Headers Definitions

SHEET_HEADERS = {
“Users”: [“Username”, “Password”, “Role”],
“Load_History”: [‘Batch ID’, ‘Generated Load ID’, ‘SO Number’, ‘Country Name’, ‘SHIP MODE’, ‘Date’, ‘Pick Status’],
“Master_Pick_Data”: [],  # Dynamic - inventory file columns අනුව set වේ
“Master_Partial_Data”: [‘Batch ID’, ‘SO Number’, ‘Pallet’, ‘Supplier’, ‘Load ID’, ‘Country Name’, ‘Actual Qty’, ‘Partial Qty’, ‘Gen Pallet ID’],
“Summary_Data”: [‘Batch ID’, ‘SO Number’, ‘Load ID’, ‘UPC’, ‘Country’, ‘Ship Mode’, ‘Requested’, ‘Picked’, ‘Variance’, ‘Status’]
}

def footer_branding():
st.markdown(”””
<style>
.footer { position: fixed; left: 0; bottom: 0; width: 100%; text-align: center;
color: #888; font-size: 13px; padding: 10px; font-weight: bold; }
</style>
<div class="footer">Developed by Ishanka Madusanka</div>
“””, unsafe_allow_html=True)

@st.cache_resource
def get_gsheet_client():
creds = Credentials.from_service_account_info(
st.secrets[“gcp_service_account”],
scopes=[
“https://www.googleapis.com/auth/spreadsheets”,
“https://www.googleapis.com/auth/drive”
]
)
return gspread.authorize(creds)

def get_master_workbook():
client = get_gsheet_client()
return client.open_by_url(st.secrets[“general”][“spreadsheet_url”])

# ============================================================

# 🌟 CORE: Sheet & Header Management

# ============================================================

def ensure_sheet_headers(ws, headers):
“””
Sheet එකේ 1 row (header row) check කරලා,
හිස් නම් හෝ නැත්නම් headers insert කරයි.
“””
if not headers:
return  # Dynamic sheets (Master_Pick_Data) skip - ඒවා වෙනම handle වේ

```
try:
    existing = ws.row_values(1)  # 1st row ගන්නවා
except Exception:
    existing = []

if not existing or all(v.strip() == "" for v in existing):
    # Header row හිස්නම් හෝ නැත්නම් insert කරයි
    ws.update('A1', [headers])
    time.sleep(0.3)  # API rate limit avoid
```

def get_or_create_sheet(sh, name, headers=None):
“””
Sheet නැත්නම් create කරයි, headers check + insert කරයි.
headers=None නම් dynamic (Master_Pick_Data) - skip header insert.
“””
try:
ws = sh.worksheet(name)
except gspread.exceptions.WorksheetNotFound:
ws = sh.add_worksheet(title=name, rows=“5000”, cols=“70”)
time.sleep(0.5)

```
# Static headers තිබෙන sheets වලට header ensure කරයි
if headers:
    ensure_sheet_headers(ws, headers)

return ws
```

def ensure_dynamic_sheet_headers(sh, sheet_name, columns):
“””
Master_Pick_Data වැනි dynamic sheets වලට,
upload කරන file columns header එකක් නැත්නම් insert කරයි.
“””
try:
ws = sh.worksheet(sheet_name)
except gspread.exceptions.WorksheetNotFound:
ws = sh.add_worksheet(title=sheet_name, rows=“5000”, cols=“70”)
time.sleep(0.5)

```
try:
    existing_headers = ws.row_values(1)
except Exception:
    existing_headers = []

if not existing_headers or all(v.strip() == "" for v in existing_headers):
    # Headers නැත්නම් දැන් insert කරයි
    ws.update('A1', [list(columns)])
    time.sleep(0.3)

return ws
```

def get_safe_dataframe(sh, sheet_name):
“”“Sheet data safely DataFrame ලෙස ලබා ගනී.”””
try:
ws = sh.worksheet(sheet_name)
data = ws.get_all_values()
if len(data) > 1:
return pd.DataFrame(data[1:], columns=data[0])
elif len(data) == 1:
return pd.DataFrame(columns=data[0])
return pd.DataFrame()
except Exception:
return pd.DataFrame()

def init_all_sheets(sh):
“””
App start වූ විට සියලු sheets create කර headers ensure කරයි.
Master_Pick_Data dynamic නිසා skip - picking time දී handle වේ.
“””
for sheet_name, headers in SHEET_HEADERS.items():
if sheet_name == “Master_Pick_Data”:
continue  # Dynamic sheet - skip
get_or_create_sheet(sh, sheet_name, headers)

# — 2. User Management & Login —

def init_users_sheet(sh):
ws = get_or_create_sheet(sh, “Users”, SHEET_HEADERS[“Users”])
users_df = get_safe_dataframe(sh, “Users”)
if users_df.empty:
ws.append_row([“admin”, “admin@123”, “admin”])
ws.append_row([“sys”, “sys@123”, “SysUser”])
time.sleep(0.3)
return get_safe_dataframe(sh, “Users”)
return users_df

def login_section():
st.sidebar.title(“🔐 WMS Login”)
if ‘logged_in’ not in st.session_state:
st.session_state[‘logged_in’] = False

```
if not st.session_state['logged_in']:
    try:
        sh = get_master_workbook()
        users_df = init_users_sheet(sh)
        user = st.sidebar.text_input("Username")
        pw = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Login", type="primary"):
            match = users_df[
                (users_df['Username'] == user) &
                (users_df['Password'] == str(pw))
            ]
            if not match.empty:
                st.session_state.update({
                    'logged_in': True,
                    'role': match.iloc[0]['Role'],
                    'username': user
                })
                st.rerun()
            else:
                st.sidebar.error("වැරදි Username හෝ Password!")
    except Exception as e:
        st.sidebar.error(f"Google Sheets සම්බන්ධ වීමේ දෝෂයක්! {e}")
    return False
return True
```

# — 3. Inventory Logic —

def reconcile_inventory(inv_df, sh):
try:
pick_history = get_safe_dataframe(sh, “Master_Pick_Data”)
if not pick_history.empty and ‘Actual Qty’ in pick_history.columns:
pick_history[‘Actual Qty’] = pd.to_numeric(
pick_history[‘Actual Qty’], errors=‘coerce’
).fillna(0)
pick_summary = pick_history.groupby(‘Pallet’)[‘Actual Qty’].sum().reset_index()
pick_summary.columns = [‘Pallet’, ‘Total_Picked’]
inv_df = pd.merge(inv_df, pick_summary, on=‘Pallet’, how=‘left’)
inv_df[‘Total_Picked’] = inv_df[‘Total_Picked’].fillna(0)
inv_df[‘Actual Qty’] = (
pd.to_numeric(inv_df[‘Actual Qty’], errors=‘coerce’).fillna(0)
- inv_df[‘Total_Picked’]
)
inv_df = inv_df[inv_df[‘Actual Qty’] > 0].drop(columns=[‘Total_Picked’])
except Exception:
pass
return inv_df

def process_picking(inv_df, req_df, batch_id):
pick_rows, partial_rows, summary = [], [], []

```
# Ensure minimum columns (63+)
if len(inv_df.columns) < 63:
    for i in range(len(inv_df.columns), 63):
        inv_df[f"Col_{i}"] = ""

temp_inv = inv_df.copy()

for lid in req_df['Generated Load ID'].unique():
    curr_reqs = req_df[req_df['Generated Load ID'] == lid]
    so_num = str(curr_reqs['SO Number'].iloc[0])
    ship_mode = (
        str(curr_reqs['SHIP MODE: (SEA/AIR)'].iloc[0])
        if 'SHIP MODE: (SEA/AIR)' in curr_reqs.columns else ""
    )

    for _, req in curr_reqs.iterrows():
        upc = str(req['Product UPC'])
        needed = float(req['PICK QTY'])
        country = req['Country Name']

        stock = temp_inv[
            temp_inv['Supplier'].astype(str) == upc
        ].sort_values(by='Actual Qty', ascending=False)

        picked_qty = 0

        for idx, item in stock.iterrows():
            if needed <= 0:
                break
            take = min(float(item['Actual Qty']), needed)
            if take > 0:
                p_row = item.copy()
                p_row.iloc[58] = ""
                p_row.iloc[62] = f"P-{datetime.now().strftime('%m%d%H%M%S')}"
                p_row.update({
                    'Batch ID': batch_id,
                    'SO Number': so_num,
                    'Actual Qty': take,
                    'Load Id': lid,
                    'Customer Po Number': f"{country}-{lid}"
                })
                pick_rows.append(p_row)

                if take < float(item['Actual Qty']):
                    partial_rows.append({
                        'Batch ID': batch_id,
                        'Pallet': item['Pallet'],
                        'Actual Qty': item['Actual Qty'],
                        'Partial Qty': take,
                        'Load ID': lid
                    })

                temp_inv.at[idx, 'Actual Qty'] -= take
                needed -= take
                picked_qty += take

        summary.append({
            'Batch ID': batch_id,
            'SO Number': so_num,
            'Load ID': lid,
            'UPC': upc,
            'Picked': picked_qty,
            'Variance': float(req['PICK QTY']) - picked_qty
        })

return (
    pd.DataFrame(pick_rows),
    pd.DataFrame(partial_rows),
    pd.DataFrame(summary)
)
```

# ============================================================

# — 4. Main App —

# ============================================================

if login_section():
sh = get_master_workbook()

```
# 🌟 App start වූ විට සියලු sheets + headers ensure කරයි
with st.spinner("Initializing sheets..."):
    init_all_sheets(sh)

role = st.session_state.role.upper()
menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "🔄 Revert/Delete Picks"]
if role == 'ADMIN':
    menu.append("⚙️ Admin Settings")

choice = st.sidebar.radio("Menu", menu)

# --------------------------------------------------------
if choice == "🚀 Picking Operations":
    st.title("🚀 Process Picking")
    inv_f = st.file_uploader("📁 Inventory Report (CSV/Excel)")
    req_f = st.file_uploader("📋 Customer Requirement (CSV/Excel)")

    if inv_f and req_f and st.button("⚡ Generate Picks", type="primary"):
        with st.spinner("Processing..."):
            inv = (pd.read_csv(inv_f) if inv_f.name.endswith('csv')
                   else pd.read_excel(inv_f))
            req = (pd.read_csv(req_f) if req_f.name.endswith('csv')
                   else pd.read_excel(req_f))

            batch_id = f"REQ-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            req['Generated Load ID'] = "SO-" + req['SO Number'].astype(str) + "-001"

            inv = reconcile_inventory(inv, sh)
            p_df, part_df, s_df = process_picking(inv, req, batch_id)

            if not p_df.empty:
                # 🌟 Dynamic headers ensure - Master_Pick_Data
                ws_pick = ensure_dynamic_sheet_headers(
                    sh, "Master_Pick_Data", p_df.columns
                )
                ws_pick.append_rows(p_df.astype(str).values.tolist())

            if not part_df.empty:
                # Partial data - Static headers already ensured at init
                ws_part = get_or_create_sheet(
                    sh, "Master_Partial_Data", SHEET_HEADERS["Master_Partial_Data"]
                )
                # Align columns to sheet headers
                for col in SHEET_HEADERS["Master_Partial_Data"]:
                    if col not in part_df.columns:
                        part_df[col] = ""
                part_df = part_df[SHEET_HEADERS["Master_Partial_Data"]]
                ws_part.append_rows(part_df.astype(str).values.tolist())

            if not s_df.empty:
                ws_sum = get_or_create_sheet(
                    sh, "Summary_Data", SHEET_HEADERS["Summary_Data"]
                )
                for col in SHEET_HEADERS["Summary_Data"]:
                    if col not in s_df.columns:
                        s_df[col] = ""
                s_df = s_df[SHEET_HEADERS["Summary_Data"]]
                ws_sum.append_rows(s_df.astype(str).values.tolist())

            # Load History update
            ws_hist = get_or_create_sheet(
                sh, "Load_History", SHEET_HEADERS["Load_History"]
            )
            for lid in req['Generated Load ID'].unique():
                row_data = req[req['Generated Load ID'] == lid].iloc[0]
                ws_hist.append_row([
                    batch_id,
                    lid,
                    str(row_data.get('SO Number', '')),
                    str(row_data.get('Country Name', '')),
                    str(row_data.get('SHIP MODE: (SEA/AIR)', '')),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Completed'
                ])

            st.success(f"✅ Picking Completed! Batch: {batch_id}")
            st.balloons()
            st.dataframe(p_df.head(20))

# --------------------------------------------------------
elif choice == "📊 Dashboard & Tracking":
    col1, col2 = st.columns([4, 1])
    col1.title("📊 Dashboard")
    if col2.button("🔄 Refresh"):
        st.rerun()

    hist_df = get_safe_dataframe(sh, "Load_History")
    pick_df = get_safe_dataframe(sh, "Master_Pick_Data")

    m1, m2, m3 = st.columns(3)
    m1.metric(
        "Total Loads",
        hist_df['Generated Load ID'].nunique() if not hist_df.empty else 0
    )
    m2.metric("Total Picks", len(pick_df) if not pick_df.empty else 0)
    m3.metric(
        "Batches",
        hist_df['Batch ID'].nunique() if not hist_df.empty else 0
    )

    st.divider()
    if not hist_df.empty:
        st.subheader("📋 Load History")
        st.dataframe(hist_df, use_container_width=True)
    else:
        st.info("No data available.")

# --------------------------------------------------------
elif choice == "🔄 Revert/Delete Picks":
    st.title("🔄 Revert / Delete Picks")
    hist_df = get_safe_dataframe(sh, "Load_History")

    if not hist_df.empty:
        batch_options = hist_df['Batch ID'].unique().tolist()
        selected_batch = st.selectbox("Select Batch to Revert:", batch_options)
        if st.button("🗑️ Revert Selected Batch", type="primary"):
            st.warning(f"Batch {selected_batch} revert logic - implement as needed.")
    else:
        st.info("No batches found.")

# --------------------------------------------------------
elif choice == "⚙️ Admin Settings":
    st.title("⚙️ Admin Settings")

    st.subheader("🗑️ Reset Sheets")
    sheet_to_clear = st.selectbox(
        "Clear Sheet:",
        ["Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History", "ALL_DATA"]
    )

    if st.button("🗑️ Reset Sheet", type="primary"):
        target_sheets = (
            ["Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History"]
            if sheet_to_clear == "ALL_DATA"
            else [sheet_to_clear]
        )

        progress = st.progress(0)
        for i, sname in enumerate(target_sheets):
            try:
                ws = sh.worksheet(sname)
                ws.clear()
                time.sleep(0.3)

                # 🌟 Clear කළ සැණින් Header row නැවත insert කරයි
                headers = SHEET_HEADERS.get(sname, [])
                if headers:
                    ws.update('A1', [headers])
                    time.sleep(0.3)

                progress.progress((i + 1) / len(target_sheets))
            except gspread.exceptions.WorksheetNotFound:
                st.warning(f"Sheet '{sname}' not found, skipping.")

        st.success(f"✅ {sheet_to_clear} Reset complete — headers restored!")

    st.divider()
    st.subheader("👥 User Management")
    users_df = get_safe_dataframe(sh, "Users")
    if not users_df.empty:
        st.dataframe(users_df[['Username', 'Role']], use_container_width=True)

    st.subheader("➕ Add New User")
    c1, c2, c3 = st.columns(3)
    new_user = c1.text_input("Username")
    new_pw = c2.text_input("Password", type="password")
    new_role = c3.selectbox("Role", ["SysUser", "admin"])
    if st.button("Add User"):
        if new_user and new_pw:
            ws_users = get_or_create_sheet(sh, "Users", SHEET_HEADERS["Users"])
            ws_users.append_row([new_user, new_pw, new_role])
            st.success(f"User '{new_user}' added!")
            st.rerun()
        else:
            st.error("Username සහ Password ඇතුළත් කරන්න.")
```

footer_branding()