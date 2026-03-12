import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import io
import time

# --- 1. System Config & Auth ---
st.set_page_config(page_title="Advanced WMS Picking System", layout="wide", page_icon="📦")

# --- Default Headers Definition ---
SHEET_HEADERS = {
    "Load_History": ['Batch ID', 'Generated Load ID', 'SO Number', 'Country Name', 'SHIP MODE', 'Date', 'Pick Status'],
    "Summary_Data": ['Batch ID', 'SO Number', 'Load ID', 'UPC', 'Country', 'Ship Mode', 'Requested', 'Picked', 'Variance', 'Status'],
    "Master_Partial_Data": ['Batch ID', 'SO Number', 'Pallet', 'Supplier', 'Load ID', 'Country Name', 'Actual Qty', 'Partial Qty', 'Gen Pallet ID'],
    "Master_Pick_Data": ['Batch ID', 'SO Number', 'Order Type', 'Order Number', 'Store Order Number', 'Customer Po Number', 'Load Id', 'Pick Id', 'Supplier', 'Pallet', 'Actual Qty']
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
        ws = sh.add_worksheet(title=name, rows="1000", cols=str(max(20, len(headers) + 5)))
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

def login_section():
    st.sidebar.title("🔐 WMS Login")
    
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if 'role' not in st.session_state:
        st.session_state['role'] = 'user'
    if 'username' not in st.session_state:
        st.session_state['username'] = 'Unknown'

    if not st.session_state['logged_in']:
        try:
            sh = get_master_workbook()
            users_df = init_users_sheet(sh)
        except Exception as e:
            st.sidebar.error("Google Sheets සම්බන්ධ වීමේ දෝෂයක්. Secrets පරීක්ෂා කරන්න.")
            return False

        user = st.sidebar.text_input("Username")
        pw = st.sidebar.text_input("Password", type="password")
        
        if st.sidebar.button("Login", type="primary"):
            user_match = users_df[(users_df['Username'] == user) & (users_df['Password'] == str(pw))]
            if not user_match.empty:
                st.session_state['logged_in'] = True
                st.session_state['role'] = user_match.iloc[0]['Role']
                st.session_state['username'] = user
                st.rerun()
            else:
                st.sidebar.error("වැරදි Username හෝ Password එකක්!")
        return False
 
    return True

# --- 3. Inventory Logic ---
def reconcile_inventory(inv_df, sh):
    """
    FIX: Subtract all previously picked quantities from inventory.
    After subtraction, strictly remove any pallet rows where Actual Qty <= 0
    so they cannot be picked again in any future batch.
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

            # Subtract previously picked qty from inventory
            inv_df['Actual Qty'] = inv_df['Actual Qty'] - inv_df['Total_Picked']

            if 'Total_Picked' in inv_df.columns:
                inv_df = inv_df.drop(columns=['Total_Picked'])

    except Exception as e:
        st.warning(f"Inventory Reconcile Error: {e}")

    # ✅ FIX: Strictly filter out any pallet with Actual Qty <= 0 (previously fully picked)
    inv_df['Actual Qty'] = pd.to_numeric(inv_df['Actual Qty'], errors='coerce').fillna(0)
    inv_df = inv_df[inv_df['Actual Qty'] > 0].reset_index(drop=True)

    return inv_df

def process_picking(inv_df, req_df, batch_id):
    pick_rows, partial_rows, summary = [], [], []
   
    supplier_col = next((c for c in inv_df.columns if 'supplier' in str(c).lower()), None)
    pick_id_col = next((c for c in inv_df.columns if 'pick id' in str(c).lower() or 'pickid' in str(c).lower()), None)

    temp_inv = inv_df.copy()
    temp_inv['Actual Qty'] = pd.to_numeric(temp_inv['Actual Qty'], errors='coerce').fillna(0)

    # ✅ FIX: Remove zero/negative qty rows from working copy before processing starts
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
                    (temp_inv['Actual Qty'] > 0)          # ✅ FIX: Only pick from pallets with qty > 0
                ].sort_values(by='Actual Qty', ascending=False)
            else:
                stock = temp_inv[
                    (temp_inv['Supplier'].astype(str) == upc) &
                    (temp_inv['Actual Qty'] > 0)          # ✅ FIX: Only pick from pallets with qty > 0
                ].sort_values(by='Actual Qty', ascending=False)
                
            picked_qty = 0
            
            for idx, item in stock.iterrows():
                if needed <= 0:
                    break

                # ✅ FIX: Re-read the current qty from temp_inv to avoid stale values
                current_avail = float(temp_inv.at[idx, 'Actual Qty'])

                # ✅ FIX: Skip this pallet if its qty has already been depleted to 0
                if current_avail <= 0:
                    continue

                take = min(current_avail, needed)
                
                if take > 0:
                    p_row = item.copy()
                    
                    p_row['Pick Id'] = str(item[pick_id_col]) if pick_id_col else ""
                    p_row['Supplier'] = str(item[supplier_col]) if supplier_col else upc
                    
                    p_row['Batch ID'] = batch_id 
                    p_row['SO Number'] = so_num 
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
                    
                    # ✅ FIX: Update temp_inv in-place so subsequent iterations see the correct remaining qty
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
        # ✅ FIX: Final safety filter — drop any accidentally zero-qty rows
        pick_df = pick_df[pick_df['Actual Qty'] > 0]
        
        if 'Pick Id' in pick_df.columns:
            pick_df['Pick Id'] = pick_df['Pick Id'].astype(str)
        if 'Supplier' in pick_df.columns:
            pick_df['Supplier'] = pick_df['Supplier'].astype(str)
    
    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)

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
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "🔄 Revert/Delete Picks", "⚙️ Admin Settings"]
    elif current_role == 'SYSUSER':
        menu = ["📊 Dashboard & Tracking", "🚀 Picking Operations", "🔄 Revert/Delete Picks"]
    else: 
        menu = ["📊 Dashboard & Tracking", "🔄 Revert/Delete Picks"]
        
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
                    
                    if not hist_df.empty and 'SO Number' in hist_df.columns:
                        for so in hist_df['SO Number'].astype(str).unique():
                            so_history = hist_df[hist_df['SO Number'].astype(str) == so]
                            so_counts[so] = len(so_history['Generated Load ID'].unique())
                    
                    for group, data in req.groupby('Group'):
                        so_num = data['SO Number'].iloc[0]
                        if so_num not in so_counts: so_counts[so_num] = 0
                        so_counts[so_num] += 1
                        count = so_counts[so_num]
                        
                        lid = f"SO-{so_num}-{count:03d}"
                        load_id_map[group] = lid
                        
                        new_hist_entries.append([batch_id, lid, so_num, data['Country Name'].iloc[0], data['SHIP MODE: (SEA/AIR)'].iloc[0], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Pending"])
                    
                    req['Generated Load ID'] = req['Group'].map(load_id_map)
                    if new_hist_entries: ws_hist.append_rows(new_hist_entries)
                    
                    inv = reconcile_inventory(inv, sh)
                    
                    pick_df, part_df, summ_df = process_picking(inv, req, batch_id)
                    
                    if not pick_df.empty:
                        ws_pick = get_or_create_sheet(sh, "Master_Pick_Data", pick_df.columns.tolist())
                        ws_pick.append_rows(pick_df.astype(str).replace('nan','').values.tolist())
                    
                    if not part_df.empty:
                        ws_part = get_or_create_sheet(sh, "Master_Partial_Data", part_df.columns.tolist())
                        ws_part.append_rows(part_df.astype(str).replace('nan','').values.tolist())
                    
                    ws_summ = get_or_create_sheet(sh, "Summary_Data", summ_df.columns.tolist())
                    ws_summ.append_rows(summ_df.astype(str).replace('nan','').values.tolist())
                    
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
        completed_loads = len(hist_df[hist_df['Pick Status'] == 'Completed']) if not hist_df.empty and 'Pick Status' in hist_df.columns else 0

        st.subheader("📈 Overall System Summary")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Load IDs", total_loads)
        m2.metric("Total Picks Made", total_picks)
        m3.metric("Pending Loads", pending_loads)
        m4.metric("Completed Loads", completed_loads)
        st.divider()

        if total_loads == 0:
            st.info("දැනට පද්ධතියේ කිසිදු දත්තයක් නොමැත. කරුණාකර 'Picking Operations' මගින් දත්ත ඇතුලත් කරන්න.")
        else:
            st.subheader("📑 Download Total Report by Upload Batch")
            if not hist_df.empty and 'Batch ID' in hist_df.columns:
                available_batches = hist_df['Batch ID'].dropna().unique()
                if len(available_batches) > 0:
                    selected_batch = st.selectbox("Select Requirement Batch ID:", available_batches)
                    if st.button("Generate Total Batch Report"):
                        with st.spinner("Generating Total Report..."):
                            batch_picks = pick_df[pick_df.get('Batch ID', '') == selected_batch] if not pick_df.empty and 'Batch ID' in pick_df.columns else pd.DataFrame()
                            batch_summ = summ_df[summ_df.get('Batch ID', '') == selected_batch] if not summ_df.empty and 'Batch ID' in summ_df.columns else pd.DataFrame()
                            
                            out_total = io.BytesIO()
                            with pd.ExcelWriter(out_total, engine='xlsxwriter') as writer:
                                if not batch_picks.empty: batch_picks.to_excel(writer, sheet_name='Pick_Report', index=False)
                                if not batch_summ.empty: batch_summ.to_excel(writer, sheet_name='Variance_Summary', index=False)
                            
                            st.download_button("⬇️ Download Batch Excel", data=out_total.getvalue(), file_name=f"Total_Report_{selected_batch}.xlsx", mime="application/vnd.ms-excel")
            st.divider()

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
    # TAB 3: REVERT / DELETE PICKS
    # ==========================================
    elif choice == "🔄 Revert/Delete Picks":
        st.title("🔄 Revert / Delete Picked Data")
        st.info("Load ID, Pallet සහ Actual Qty අඩංගු Excel හෝ CSV ගොනුවක් Upload කිරීමෙන් එම දත්ත Master_Pick_Data එකෙන් මකා දැමිය හැක.")
        
        del_file = st.file_uploader("Upload Data to Delete", type=['csv', 'xlsx'])
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
                        
                        temp_master['MATCH_KEY'] = temp_master['LOAD ID'].astype(str).str.strip() + "_" + temp_master['PALLET'].astype(str).str.strip() + "_" + temp_master['ACTUAL QTY'].astype(float).astype(str)
                        del_df['MATCH_KEY'] = del_df['LOAD ID'].astype(str).str.strip() + "_" + del_df['PALLET'].astype(str).str.strip() + "_" + del_df['ACTUAL QTY'].astype(float).astype(str)
                        
                        keys_to_delete = del_df['MATCH_KEY'].tolist()
                        
                        filtered_master = master_pick_df[~temp_master['MATCH_KEY'].isin(keys_to_delete)]
                        deleted_count = initial_len - len(filtered_master)
                        
                        if deleted_count > 0:
                            ws_pick = sh.worksheet("Master_Pick_Data")
                            header = ws_pick.row_values(1)
                            ws_pick.clear()
                            ws_pick.append_row(header)
                            if not filtered_master.empty:
                                filtered_master = filtered_master[[c for c in header if c in filtered_master.columns]]
                                ws_pick.append_rows(filtered_master.astype(str).replace('nan','').values.tolist())
                            st.success(f"✅ සාර්ථකව වාර්තා {deleted_count} ක් මකා දමන ලදී!")
                            st.balloons()
                        else:
                            st.warning("⚠️ Upload කල දත්ත හා ගැලපෙන වාර්තා Master_Pick_Data හි හමු නොවීය.")
                    else:
                        st.error("දැනට Master_Pick_Data හි දත්ත නොමැත.")

    # ==========================================
    # TAB 4: ADMIN SETTINGS
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
                "Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History", "ALL_DATA"
            ])
            
            confirm = st.text_input("Type 'CONFIRM' to proceed:")
            if st.button("🗑️ Clear Selected Data", type="primary"):
                if confirm == 'CONFIRM':
                    try:
                        sheets_to_process = ["Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History"] if sheet_to_clear == "ALL_DATA" else [sheet_to_clear]
                        for s_name in sheets_to_process:
                            try:
                                ws = sh.worksheet(s_name)
                                ws.clear() 
                                if s_name in SHEET_HEADERS:
                                    ws.append_row(SHEET_HEADERS[s_name])
                            except: pass
                        st.success(f"✅ {sheet_to_clear} සාර්ථකව Reset කර Headers අලුතින් ඇතුලත් කරන ලදී.")
                    except Exception as e:
                        st.error(f"Error clearing data: {e}")
                else:
                    st.error("කරුණාකර CONFIRM ලෙස Type කරන්න.")

footer_branding()
