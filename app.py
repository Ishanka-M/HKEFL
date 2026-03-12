import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime

# --- 1. Google Sheets Connection Setup ---
def get_gspread_client():
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(credentials)

SHEET_URL = "YOUR_GOOGLE_SHEET_URL_HERE" # මෙතනට ඔබේ Google Sheet Link එක දෙන්න

# --- 2. SO Number Generation Logic ---
def generate_so_number(df_req, existing_sos):
    # Customer request එකෙන් පළමු පේළියේ දත්ත ගැනීම
    country = df_req['Country Name'].iloc[0]
    so_num = str(df_req['SO Number'].iloc[0])
    ship_mode = df_req['SHIP MODE: (SEA/AIR)'].iloc[0]
    
    base_format = f"{country}/{so_num}/{ship_mode}"
    
    # Duplicate වෙන්නේ නැති වෙන්න SO-XXXXX-01 විදිහට හැදීම
    count = 1
    while True:
        new_so_id = f"SO-{so_num}-{count:02d}"
        if new_so_id not in existing_sos:
            break
        count += 1
        
    return base_format, new_so_id

# --- 3. Core Picking Logic ---
def process_inventory(inv_df, req_df, new_so_id):
    pick_report_data = []
    partial_report_data = []
    
    # 0 තියෙන ඒවා අයින් කිරීම
    inv_df = inv_df[inv_df['Actual Qty'] > 0].copy()
    
    # කලින් partial වුන ඒවාට ප්‍රමුඛතාවය දීමට Sort කිරීම (ඔබේ logic එක අනුව)
    inv_df = inv_df.sort_values(by='Actual Qty') 

    partial_count = 1

    for index, req_row in req_df.iterrows():
        upc = req_row['Product UPC']
        req_qty = req_row['PICK QTY']
        
        # Product UPC එක Supplier ට සමාන කිරීම
        matching_pallets = inv_df[inv_df['Supplier'] == upc]
        
        for idx, pallet in matching_pallets.iterrows():
            if req_qty <= 0:
                break
                
            avail_qty = pallet['Actual Qty']
            
            if avail_qty <= req_qty:
                # Full Pallet එකම ගන්නවා
                req_qty -= avail_qty
                
                # Pick Report එකට දත්ත එකතු කිරීම (Inventory Report Format)
                picked_row = pallet.copy()
                picked_row['Actual Qty'] = avail_qty # ගත්තු ගාන
                pick_report_data.append(picked_row)
                
                # Main inventory එකෙන් අයින් කරනවා (0 වෙන නිසා)
                inv_df.at[idx, 'Actual Qty'] = 0 
                
            else:
                # Partial Pick එකක් වෙනවා
                picked_qty = req_qty
                balance_qty = avail_qty - req_qty
                req_qty = 0
                
                # Pick Report එකට ගත්තු ටික
                picked_row = pallet.copy()
                picked_row['Actual Qty'] = picked_qty
                pick_report_data.append(picked_row)
                
                # Inventory එකේ balance එක update කිරීම
                inv_df.at[idx, 'Actual Qty'] = balance_qty
                
                # Partial Report එකට දත්ත එකතු කිරීම
                p_id = f"P{partial_count:05d}"
                partial_report_data.append({
                    'Pallet': pallet['Pallet'],
                    'Actual Qty': avail_qty,
                    'Partial Qty': picked_qty,
                    'SO Number': req_row['SO Number'],
                    'Country Name': req_row['Country Name'],
                    'Generated SO ID': new_so_id,
                    'Pallet+Auto ID': f"{pallet['Pallet']}-{p_id}"
                })
                partial_count += 1

    # අලුත් Inventory Dataframe එක සහ Report Dataframes සෑදීම
    final_inv_df = inv_df[inv_df['Actual Qty'] > 0] # ඉතුරු වුන ටික
    pick_df = pd.DataFrame(pick_report_data)
    partial_df = pd.DataFrame(partial_report_data)
    
    return final_inv_df, pick_df, partial_df

# --- 4. Google Sheets Saving Logic ---
def save_to_gsheets(client, df, sheet_name):
    sh = client.open_by_url(SHEET_URL)
    try:
        worksheet = sh.add_worksheet(title=sheet_name, rows=str(len(df)+10), cols=str(len(df.columns)))
    except:
        # Sheet එක දැනටමත් තියෙනවා නම් ඒකටම දානවා
        worksheet = sh.worksheet(sheet_name)
        worksheet.clear()
        
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

# --- 5. Streamlit App UI ---
st.set_page_config(layout="wide", page_title="WMS Picking System")
st.title("📦 Inventory Picking & SO Management System")

# Tabs for separate views
tab1, tab2 = st.tabs(["⚙️ Process Picking", "📊 Dashboard"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        inv_file = st.file_uploader("Upload Inventory Report (CSV/Excel)", type=['csv', 'xlsx'])
    with col2:
        req_file = st.file_uploader("Upload Customer Request (CSV/Excel)", type=['csv', 'xlsx'])

    if inv_file and req_file:
        # Reading files
        if inv_file.name.endswith('.csv'):
            inv_df = pd.read_csv(inv_file)
        else:
            inv_df = pd.read_excel(inv_file)
            
        if req_file.name.endswith('.csv'):
            req_df = pd.read_csv(req_file)
        else:
            req_df = pd.read_excel(req_file)

        if st.button("🚀 Process Pick Request"):
            with st.spinner("Processing..."):
                # Google Sheets Connection
                client = get_gspread_client()
                
                # Mockup for existing SOs (මෙය Google Sheet එකෙන් read කරන්නත් පුළුවන්)
                existing_sos = [] 
                
                # 1. Generate SO
                base_format, new_so_id = generate_so_number(req_df, existing_sos)
                st.success(f"Generated SO Details: {base_format}")
                st.info(f"Unique SO ID: {new_so_id}")
                
                # 2. Process Core Logic
                final_inv_df, pick_df, partial_df = process_inventory(inv_df, req_df, new_so_id)
                
                # 3. Save Data
                save_to_gsheets(client, final_inv_df, f"Inv_{new_so_id}")
                save_to_gsheets(client, pick_df, f"Pick_{new_so_id}")
                save_to_gsheets(client, partial_df, f"Partial_{new_so_id}")
                st.success("Data successfully saved to Google Workbook!")

                # 4. Downloads
                col1, col2 = st.columns(2)
                with col1:
                    csv_pick = pick_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="⬇️ Download Pick Report",
                        data=csv_pick,
                        file_name=f"Pick_Report_{new_so_id}.csv",
                        mime='text/csv',
                    )
                with col2:
                    csv_partial = partial_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="⬇️ Download Partial Report",
                        data=csv_partial,
                        file_name=f"Partial_Report_{new_so_id}.csv",
                        mime='text/csv',
                    )
                
                # Storing Dataframes to view in dashboard
                st.session_state['pick_df'] = pick_df
                st.session_state['partial_df'] = partial_df

with tab2:
    st.header("📊 Data Dashboard")
    if 'pick_df' in st.session_state:
        st.subheader("Pick Report Data")
        st.dataframe(st.session_state['pick_df'], use_container_width=True)
        st.divider()
        st.subheader("Partial Report Data")
        st.dataframe(st.session_state['partial_df'], use_container_width=True)
    else:
        st.warning("Please process a request to view data here.")
