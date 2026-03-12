import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import io

# --- 1. Configuration & Auth ---
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
        ws = sh.add_worksheet(title=name, rows="10000", cols=str(len(headers) + 5))
        ws.append_row(headers)
    return ws

# --- 2. Login Logic ---
def login():
    st.sidebar.title("🔐 Login Section")
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        user = st.sidebar.text_input("Username")
        pw = st.sidebar.text_input("Password", type="password")
        if st.sidebar.button("Login"):
            if user == "admin" and pw == "admin@123":
                st.session_state['logged_in'] = True
                st.session_state['role'] = 'admin'
                st.rerun()
            else:
                st.sidebar.error("වැරදි Username හෝ Password එකක්!")
        return False
    return True

# --- 3. Inventory Reconciliation (0 Quantity Check) ---
def reconcile_inventory(inv_df, sh):
    try:
        pick_ws = sh.worksheet("Master_Pick_Data")
        pick_history = pd.DataFrame(pick_ws.get_all_records())
        if not pick_history.empty:
            pick_summary = pick_history.groupby('Pallet')['Actual Qty'].sum().reset_index()
            pick_summary.columns = ['Pallet', 'Total_Picked_Earlier']
            inv_df = pd.merge(inv_df, pick_summary, on='Pallet', how='left')
            inv_df['Total_Picked_Earlier'] = inv_df['Total_Picked_Earlier'].fillna(0)
            inv_df['Actual Qty'] = inv_df['Actual Qty'] - inv_df['Total_Picked_Earlier']
            inv_df = inv_df[inv_df['Actual Qty'] > 0]
            inv_df = inv_df.drop(columns=['Total_Picked_Earlier'])
    except: pass
    return inv_df

# --- 4. Picking & Mapping Logic ---
def process_picking(inv_df, req_df):
    pick_rows = []
    partial_rows = []
    summary = []
    temp_inv = inv_df.copy()
    
    for lid in req_df['Generated Load ID'].unique():
        current_reqs = req_df[req_df['Generated Load ID'] == lid]
        for _, req in current_reqs.iterrows():
            upc = str(req['Product UPC'])
            needed = req['PICK QTY']
            country = req['Country Name']
            
            stock = temp_inv[temp_inv['Supplier'].astype(str) == upc].sort_values(by='Actual Qty', ascending=False)
            picked_qty = 0
            
            for idx, item in stock.iterrows():
                if needed <= 0: break
                avail = item['Actual Qty']
                take = min(avail, needed)
                
                # --- Mapping Data to Inventory Format ---
                p_row = item.copy()
                p_row['Actual Qty'] = take
                p_row['Order Type'] = "Sample Orders"
                p_row['Order Number'] = lid
                p_row['Store Order Number'] = lid
                p_row['Customer Po Number'] = f"{country}-{lid}"
                p_row['Load Id'] = lid
                p_row['Pick Id'] = f"P-{datetime.now().strftime('%m%d%H%M%S')}"
                pick_rows.append(p_row)
                
                # Partial Logic
                if take < avail:
                    partial_rows.append({
                        'Pallet': item['Pallet'], 'Supplier': upc, 'Load ID': lid,
                        'Country Name': country, 'Actual Qty': avail,
                        'Partial Qty': take, 'Gen Pallet ID': f"{item['Pallet']}-P{len(partial_rows)+1:04d}"
                    })
                
                temp_inv.at[idx, 'Actual Qty'] -= take
                needed -= take
                picked_qty += take

            summary.append({'Load ID': lid, 'UPC': upc, 'Requested': req['PICK QTY'], 'Picked': picked_qty, 'Status': 'Done' if (req['PICK QTY'] - picked_qty) == 0 else 'Shortage'})
            
    return pd.DataFrame(pick_rows), pd.DataFrame(partial_rows), pd.DataFrame(summary)

# --- 5. Main App UI ---
if login():
    st.sidebar.success(f"Logged in as: {st.session_state['role']}")
    if st.sidebar.button("Logout"):
        st.session_state['logged_in'] = False
        st.rerun()

    tab_selector = ["🚀 Picking Section", "📊 Dashboard"]
    # Admin ට පමණක් Dashboard එකේ සැකසුම් පාලනය කළ හැක (මෙහි සරලව tab පෙන්වමු)
    choice = st.sidebar.radio("Navigate To:", tab_selector)

    if choice == "🚀 Picking Section":
        st.header("Inventory Picking Operations")
        col1, col2 = st.columns(2)
        inv_file = col1.file_uploader("Inventory Report", type=['csv', 'xlsx'])
        req_file = col2.file_uploader("Customer Requirement", type=['csv', 'xlsx'])

        if inv_file and req_file:
            if st.button("Generate & Save Picks"):
                sh = get_master_workbook()
                inv = pd.read_csv(inv_file) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file)
                req = pd.read_csv(req_file) if req_file.name.endswith('.csv') else pd.read_excel(req_file)
                
                # Generate Load IDs Logic
                req['Group'] = req['SO Number'].astype(str) + "_" + req['Country Name'] + "_" + req['SHIP MODE: (SEA/AIR)']
                load_id_map = {g: f"SO-{g.split('_')[0]}-{i+1:03d}" for i, g in enumerate(req['Group'].unique())}
                req['Generated Load ID'] = req['Group'].map(load_id_map)
                
                inv = reconcile_inventory(inv, sh)
                pick_df, part_df, summ_df = process_picking(inv, req)
                
                # Master Sheet Save
                if not pick_df.empty:
                    ws_pick = get_or_create_sheet(sh, "Master_Pick_Data", pick_df.columns.tolist())
                    ws_pick.append_rows(pick_df.astype(str).replace('nan','').values.tolist())
                
                # --- Combined Excel Download ---
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    pick_df.to_excel(writer, sheet_name='Pick_Report', index=False)
                    if not part_df.empty: part_df.to_excel(writer, sheet_name='Partial_Report', index=False)
                    summ_df.to_excel(writer, sheet_name='Summary', index=False)
                
                st.success("Picking Completed!")
                st.download_button(
                    label="⬇️ Download All Reports (Excel)",
                    data=output.getvalue(),
                    file_name=f"Full_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.dataframe(summ_df)

    elif choice == "📊 Dashboard":
        st.header("Pick Tracking Dashboard")
        sh = get_master_workbook()
        try:
            pick_master = pd.DataFrame(sh.worksheet("Master_Pick_Data").get_all_records())
            
            if not pick_master.empty:
                st.subheader("Pick Tracking Summary")
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Picks", len(pick_master))
                c2.metric("Total Qty Picked", pick_master['Actual Qty'].sum())
                c3.metric("Unique Load IDs", pick_master['Load Id'].nunique())
                
                # Chart: Picks by Country
                if 'Customer Po Number' in pick_master.columns:
                    country_data = pick_master.groupby('Load Id')['Actual Qty'].sum().head(10)
                    st.bar_chart(country_data)

                st.subheader("Search & Tracking")
                search_id = st.selectbox("Select Load ID to Track", pick_master['Load Id'].unique())
                st.table(pick_master[pick_master['Load Id'] == search_id].head(10))
            else:
                st.info("දත්ත කිසිවක් හමු නොවීය.")
        except:
            st.error("Master Sheets කියවීමට නොහැක.")
