import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import io

# --- 1. System Config & Auth ---
st.set_page_config(page_title="Advanced WMS Picking System", layout="wide", page_icon="📦")

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
    except:
        ws = sh.add_worksheet(title=name, rows="1000", cols=str(max(20, len(headers) + 5)))
        ws.append_row(headers)
    return ws

# --- 2. User Management & Login ---
def init_users_sheet(sh):
    ws = get_or_create_sheet(sh, "Users", ["Username", "Password", "Role"])
    users_data = ws.get_all_records()
    if not users_data:
        # Default Admin
        ws.append_row(["admin", "admin@123", "admin"])
    return pd.DataFrame(ws.get_all_records())

def login_section():
    st.sidebar.title("🔐 WMS Login")
    
    # එක් එක් key එක වෙන වෙනම අලුතින් හඳුන්වා දීම (Safe Initialization)
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
        if st.sidebar.button("Login"):
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
    try:
        pick_ws = sh.worksheet("Master_Pick_Data")
        pick_history = pd.DataFrame(pick_ws.get_all_records())
        if not pick_history.empty:
            pick_summary = pick_history.groupby('Pallet')['Actual Qty'].sum().reset_index()
            pick_summary.columns = ['Pallet', 'Total_Picked']
            inv_df = pd.merge(inv_df, pick_summary, on='Pallet', how='left')
            inv_df['Total_Picked'] = inv_df['Total_Picked'].fillna(0)
            inv_df['Actual Qty'] = inv_df['Actual Qty'] - inv_df['Total_Picked']
            inv_df = inv_df[inv_df['Actual Qty'] > 0]
            inv_df = inv_df.drop(columns=['Total_Picked'])
    except: pass
    return inv_df

def process_picking(inv_df, req_df):
    pick_rows, partial_rows, summary = [], [], []
    temp_inv = inv_df.copy()
    
    for lid in req_df['Generated Load ID'].unique():
        current_reqs = req_df[req_df['Generated Load ID'] == lid]
        for _, req in current_reqs.iterrows():
            upc, needed, country = str(req['Product UPC']), req['PICK QTY'], req['Country Name']
            
            stock = temp_inv[temp_inv['Supplier'].astype(str) == upc].sort_values(by='Actual Qty', ascending=False)
            picked_qty = 0
            
            for idx, item in stock.iterrows():
                if needed <= 0: break
                avail = item['Actual Qty']
                take = min(avail, needed)
                
                if take > 0:
                    p_row = item.copy()
                    p_row['Actual Qty'] = take
                    p_row['Order Type'] = "Sample Orders"
                    p_row['Order Number'] = lid
                    p_row['Store Order Number'] = lid
                    p_row['Customer Po Number'] = f"{country}-{lid}"
                    p_row['Load Id'] = lid
                    p_row['Pick Id'] = f"P-{datetime.now().strftime('%m%d%H%M%S')}"
                    pick_rows.append(p_row)
                    
                    if take < avail:
                        partial_rows.append({
                            'Pallet': item['Pallet'], 'Supplier': upc, 'Load ID': lid,
                            'Country Name': country, 'Actual Qty': avail,
                            'Partial Qty': take, 'Gen Pallet ID': f"{item['Pallet']}-P{len(partial_rows)+1:04d}"
                        })
                    
                    temp_inv.at[idx, 'Actual Qty'] -= take
                    needed -= take
                    picked_qty += take

            variance = req['PICK QTY'] - picked_qty
            summary.append({
                'Load ID': lid, 'UPC': upc, 'Country': country,
                'Requested': req['PICK QTY'], 'Picked': picked_qty, 'Variance': variance,
                'Status': 'Fully Picked' if variance == 0 else 'Shortage'
            })
            
    # Filter out any 0 quantities just to be 100% safe
    pick_df = pd.DataFrame(pick_rows)
    if not pick_df.empty: pick_df = pick_df[pick_df['Actual Qty'] > 0]
    
    return pick_df, pd.DataFrame(partial_rows), pd.DataFrame(summary)

# --- 4. App UI & Navigation ---
if login_section():
    # .get() භාවිතයෙන් Error එක මඟ හැරීම
    current_user = st.session_state.get('username', 'User')
    current_role = st.session_state.get('role', 'user').upper()
    
    st.sidebar.success(f"👤 Logged in as: {current_user} ({current_role})")
    
    if st.sidebar.button("Logout"):
        # st.session_state.clear() වෙනුවට පහත ක්‍රමය පාවිච්චි කරන්න
        st.session_state['logged_in'] = False
        st.session_state['username'] = 'Unknown'
        st.session_state['role'] = 'user'
        st.rerun()

    menu = ["🚀 Picking Operations", "📊 Dashboard & Tracking"]
    if current_role == 'ADMIN':
        menu.append("⚙️ Admin Settings")
        
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
                with st.spinner("Processing Data..."):
                    inv = pd.read_csv(inv_file) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file)
                    req = pd.read_csv(req_file) if req_file.name.endswith('.csv') else pd.read_excel(req_file)
                    
                    # History and ID Generation
                    ws_hist = get_or_create_sheet(sh, "Load_History", ['Generated Load ID', 'SO Number', 'Country Name', 'SHIP MODE', 'Date', 'Pick Status'])
                    hist_df = pd.DataFrame(ws_hist.get_all_records())
                    
                    req['Group'] = req['SO Number'].astype(str) + "_" + req['Country Name'] + "_" + req['SHIP MODE: (SEA/AIR)']
                    new_hist_entries = []
                    load_id_map = {}
                    
                    for group, data in req.groupby('Group'):
                        so_num = str(data['SO Number'].iloc[0])
                        count = len(hist_df[hist_df['SO Number'].astype(str) == so_num]) + 1 if not hist_df.empty else 1
                        lid = f"SO-{so_num}-{count:03d}"
                        load_id_map[group] = lid
                        new_hist_entries.append([lid, so_num, data['Country Name'].iloc[0], data['SHIP MODE: (SEA/AIR)'].iloc[0], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Pending"])
                    
                    req['Generated Load ID'] = req['Group'].map(load_id_map)
                    if new_hist_entries: ws_hist.append_rows(new_hist_entries)
                    
                    # Picking Process
                    inv = reconcile_inventory(inv, sh)
                    pick_df, part_df, summ_df = process_picking(inv, req)
                    
                    # Save to Sheets
                    if not pick_df.empty:
                        ws_pick = get_or_create_sheet(sh, "Master_Pick_Data", pick_df.columns.tolist())
                        ws_pick.append_rows(pick_df.astype(str).replace('nan','').values.tolist())
                    if not part_df.empty:
                        ws_part = get_or_create_sheet(sh, "Master_Partial_Data", part_df.columns.tolist())
                        ws_part.append_rows(part_df.astype(str).replace('nan','').values.tolist())
                    
                    ws_summ = get_or_create_sheet(sh, "Summary_Data", summ_df.columns.tolist())
                    ws_summ.append_rows(summ_df.astype(str).replace('nan','').values.tolist())
                    
                    # Download Excel
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        if not pick_df.empty: pick_df.to_excel(writer, sheet_name='Pick_Report', index=False)
                        if not part_df.empty: part_df.to_excel(writer, sheet_name='Partial_Report', index=False)
                        if not summ_df.empty: summ_df.to_excel(writer, sheet_name='Variance_Summary', index=False)
                    
                    st.success("✅ Picking Completed Successfully!")
                    st.download_button("⬇️ Download Final Reports (Excel)", data=output.getvalue(), file_name=f"WMS_Pick_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)

    # ==========================================
    # TAB 2: DASHBOARD & TRACKING
    # ==========================================
    elif choice == "📊 Dashboard & Tracking":
        st.title("📊 Load Tracking & Dashboard")
        
        try:
            hist_df = pd.DataFrame(sh.worksheet("Load_History").get_all_records())
            summ_df = pd.DataFrame(sh.worksheet("Summary_Data").get_all_records())
            pick_df = pd.DataFrame(sh.worksheet("Master_Pick_Data").get_all_records())
        except Exception:
            st.info("දැනට පද්ධතියේ කිසිදු දත්තයක් නොමැත.")
            st.stop()
            
        if hist_df.empty:
            st.info("දැනට Load History දත්ත නොමැත.")
            st.stop()

        # Overall Metrics
        st.subheader("📈 Overall System Summary")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Load IDs", hist_df['Generated Load ID'].nunique())
        m2.metric("Total Picks Made", len(pick_df) if not pick_df.empty else 0)
        m3.metric("Pending Loads", len(hist_df[hist_df['Pick Status'] == 'Pending']))
        m4.metric("Completed Loads", len(hist_df[hist_df['Pick Status'] == 'Completed']))
        st.divider()

        # Search & Tracking
        st.subheader("🔍 Search & Update Load Details")
        col_s1, col_s2 = st.columns([3, 1])
        # Search Box
        search_lid = col_s1.selectbox("🔎 Search Load ID:", hist_df['Generated Load ID'].unique())
        
        # Status Update
        current_status = hist_df[hist_df['Generated Load ID'] == search_lid]['Pick Status'].iloc[0]
        new_status = col_s2.selectbox("📝 Update Pick Status:", ["Pending", "Processing", "Completed", "Cancelled"], index=["Pending", "Processing", "Completed", "Cancelled"].index(current_status))
        
        if col_s2.button("Update Status"):
            # Update status in Google Sheets
            ws_hist = sh.worksheet("Load_History")
            cell = ws_hist.find(search_lid)
            ws_hist.update_cell(cell.row, 6, new_status) # 6th column is Status
            st.success(f"Status updated to {new_status}!")
            st.rerun()

        # Load ID Specific Details
        st.markdown(f"### Details for: `{search_lid}`")
        tab_v, tab_p = st.tabs(["📉 Variance & Summary", "📦 Picked Items Detail"])
        
        with tab_v:
            if not summ_df.empty:
                load_summ = summ_df[summ_df['Load ID'] == search_lid]
                st.dataframe(load_summ, use_container_width=True)
                
                # Check for variances
                shortages = load_summ[load_summ['Variance'] > 0]
                if not shortages.empty:
                    st.warning("⚠️ Warning: Shortages detected in this Load ID!")
                    st.table(shortages[['UPC', 'Requested', 'Picked', 'Variance']])
            else:
                st.write("No summary data.")
                
        with tab_p:
            if not pick_df.empty:
                load_picks = pick_df[pick_df['Load Id'] == search_lid]
                st.dataframe(load_picks, use_container_width=True)
            else:
                st.write("No pick data.")

    # ==========================================
    # TAB 3: ADMIN SETTINGS
    # ==========================================
    elif choice == "⚙️ Admin Settings":
        st.title("⚙️ System Administration")
        
        col_adm1, col_adm2 = st.columns(2)
        
        # User Management
        with col_adm1:
            st.subheader("👥 Add New User")
            with st.form("add_user_form"):
                n_user = st.text_input("New Username")
                n_pass = st.text_input("New Password", type="password")
                n_role = st.selectbox("Role", ["user", "admin"])
                submitted = st.form_submit_button("Add User")
                if submitted and n_user and n_pass:
                    ws_users = sh.worksheet("Users")
                    users_data = pd.DataFrame(ws_users.get_all_records())
                    if n_user in users_data['Username'].values:
                        st.error("මෙම Username එක දැනටමත් ඇත.")
                    else:
                        ws_users.append_row([n_user, n_pass, n_role])
                        st.success("User සාර්ථකව ඇතුලත් කරන ලදී!")
        
        # Data Reset Management
        with col_adm2:
            st.subheader("⚠️ Database Management")
            st.warning("මෙමඟින් පද්ධතියේ පරණ දත්ත සම්පූර්ණයෙන්ම මකා දමයි. (Clear Database)")
            
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
                                header = ws.row_values(1) # අනුපිළිවෙල (Headers) ආරක්ෂා කරගැනීම
                                ws.clear()
                                ws.append_row(header)
                            except: pass
                        st.success(f"✅ {sheet_to_clear} සාර්ථකව Reset කරන ලදී.")
                    except Exception as e:
                        st.error(f"Error clearing data: {e}")
                else:
                    st.error("කරුණාකර CONFIRM ලෙස Type කරන්න.")
