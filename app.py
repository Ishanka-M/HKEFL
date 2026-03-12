import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- Configuration & Auth ---
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
        ws = sh.add_worksheet(title=name, rows="5000", cols=str(len(headers) + 5))
        ws.append_row(headers)
    return ws

# --- Load ID Generation ---
def generate_load_ids(req_df, sh):
    ws_hist = get_or_create_sheet(sh, "Load_History", ['Generated Load ID', 'SO Number', 'Country Name', 'SHIP MODE', 'Date'])
    hist_df = pd.DataFrame(ws_hist.get_all_records())
    
    req_df['Group'] = req_df['SO Number'].astype(str) + "_" + req_df['Country Name'] + "_" + req_df['SHIP MODE: (SEA/AIR)']
    grouped = req_df.groupby('Group')
    
    load_id_map = {}
    new_entries = []
    
    for group_name, group_data in grouped:
        so_num = str(group_data['SO Number'].iloc[0])
        country = group_data['Country Name'].iloc[0]
        mode = group_data['SHIP MODE: (SEA/AIR)'].iloc[0]
        
        count = 1
        if not hist_df.empty:
            existing = hist_df[hist_df['SO Number'].astype(str) == so_num]
            if not existing.empty:
                count = len(existing) + 1
        
        load_id = f"SO-{so_num}-{count:03d}"
        load_id_map[group_name] = load_id
        new_entries.append([load_id, so_num, country, mode, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    
    if new_entries:
        ws_hist.append_rows(new_entries)
    req_df['Generated Load ID'] = req_df['Group'].map(load_id_map)
    return req_df

# --- Logic to prevent double picking ---
def reconcile_inventory(inv_df, sh):
    """කලින් pick කරපු ප්‍රමාණයන් ඉවත් කර දැනට ඉතිරි ප්‍රමාණයන් පමණක් ලබා ගැනීම"""
    try:
        pick_ws = sh.worksheet("Master_Pick_Data")
        pick_history = pd.DataFrame(pick_ws.get_all_records())
        
        if not pick_history.empty:
            # Pallet එක අනුව දැනට pick කර ඇති මුළු ප්‍රමාණය සෙවීම
            pick_summary = pick_history.groupby('Pallet')['Actual Qty'].sum().reset_index()
            pick_summary.columns = ['Pallet', 'Total_Picked_Earlier']
            
            # Inventory එක සමඟ merge කිරීම
            inv_df = pd.merge(inv_df, pick_summary, on='Pallet', how='left')
            inv_df['Total_Picked_Earlier'] = inv_df['Total_Picked_Earlier'].fillna(0)
            
            # සැබෑවටම ඉතිරි ප්‍රමාණය ගණනය කිරීම
            inv_df['Actual Qty'] = inv_df['Actual Qty'] - inv_df['Total_Picked_Earlier']
            
            # 0 හෝ අඩු ප්‍රමාණ ඇති Pallets ඉවත් කිරීම
            inv_df = inv_df[inv_df['Actual Qty'] > 0]
            inv_df = inv_df.drop(columns=['Total_Picked_Earlier'])
    except Exception:
        pass # Master_Pick_Data නැතිනම් අලුත් එකක් ලෙස සලකයි
    
    return inv_df

# --- Core Picking Logic ---
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
            
            # Product UPC = Supplier
            stock = temp_inv[temp_inv['Supplier'].astype(str) == upc].sort_values(by='Actual Qty', ascending=False)
            
            picked_qty = 0
            for idx, item in stock.iterrows():
                if needed <= 0: break
                
                avail = item['Actual Qty']
                if avail <= 0: continue
                
                take = min(avail, needed)
                
                # Create Pick Record
                p_row = item.copy()
                p_row['Actual Qty'] = take
                p_row['Load Id'] = lid
                p_row['Pick Id'] = f"P-{datetime.now().strftime('%m%d%H%M%S')}"
                pick_rows.append(p_row)
                
                # Partial Logic
                if take < avail:
                    partial_rows.append({
                        'Pallet': item['Pallet'], 'Supplier': upc, 'Load ID': lid,
                        'Country Name': req['Country Name'], 'Actual Qty': avail,
                        'Partial Qty': take, 'Gen Pallet ID': f"{item['Pallet']}-P{len(partial_rows)+1:04d}"
                    })
                
                temp_inv.at[idx, 'Actual Qty'] -= take
                needed -= take
                picked_qty += take

            summary.append({
                'Load ID': lid, 'UPC': upc, 'Requested': req['PICK QTY'], 
                'Picked': picked_qty, 'Variance': req['PICK QTY'] - picked_qty,
                'Status': 'Done' if (req['PICK QTY'] - picked_qty) == 0 else 'Shortage'
            })
            
    return pd.DataFrame(pick_rows), pd.DataFrame(partial_rows), pd.DataFrame(summary)

# --- Streamlit UI ---
st.set_page_config(page_title="HKEFL Picking System", layout="wide")
st.title("📦 Inventory Picking Automation")

tab1, tab2 = st.tabs(["🚀 Process New Pick", "📊 Dashboard & History"])

with tab1:
    col1, col2 = st.columns(2)
    inv_file = col1.file_uploader("Upload Inventory Report", type=['csv', 'xlsx'])
    req_file = col2.file_uploader("Upload Customer Requirement", type=['csv', 'xlsx'])

    if inv_file and req_file:
        if st.button("Generate Picks"):
            with st.spinner("Processing..."):
                sh = get_master_workbook()
                inv = pd.read_csv(inv_file) if inv_file.name.endswith('.csv') else pd.read_excel(inv_file)
                req = pd.read_csv(req_file) if req_file.name.endswith('.csv') else pd.read_excel(req_file)
                
                # 1. පරණ history එක පරීක්ෂා කර පවතින balance එක විතරක් ගන්න
                inv = reconcile_inventory(inv, sh)
                
                # 2. Generate IDs
                req_with_ids = generate_load_ids(req, sh)
                
                # 3. Picking
                pick_df, part_df, summ_df = process_picking(inv, req_with_ids)
                
                # 4. Save to Master Sheets
                if not pick_df.empty:
                    ws_pick = get_or_create_sheet(sh, "Master_Pick_Data", pick_df.columns.tolist())
                    ws_pick.append_rows(pick_df.astype(str).replace('nan','').values.tolist())
                
                if not part_df.empty:
                    ws_part = get_or_create_sheet(sh, "Master_Partial_Data", part_df.columns.tolist())
                    ws_part.append_rows(part_df.astype(str).replace('nan','').values.tolist())
                
                st.success("Successfully Processed and Saved to Master Sheets!")
                st.subheader("Summary Table")
                st.dataframe(summ_df, use_container_width=True)

with tab2:
    st.header("Search Load History")
    try:
        sh = get_master_workbook()
        hist_ws = sh.worksheet("Load_History")
        hist_df = pd.DataFrame(hist_ws.get_all_records())
        
        if not hist_df.empty:
            load_to_find = st.selectbox("Select Load ID to Download", hist_df['Generated Load ID'].unique())
            
            if st.button("Fetch Data"):
                pick_master = pd.DataFrame(sh.worksheet("Master_Pick_Data").get_all_records())
                part_master = pd.DataFrame(sh.worksheet("Master_Partial_Data").get_all_records())
                
                current_picks = pick_master[pick_master['Load Id'] == load_to_find]
                current_parts = part_master[part_master['Load ID'] == load_to_find]
                
                st.info(f"Showing results for: {load_to_find}")
                st.dataframe(current_picks, use_container_width=True)
                
                c1, c2 = st.columns(2)
                c1.download_button("⬇️ Download Pick Report", current_picks.to_csv(index=False), f"Pick_{load_to_find}.csv")
                if not current_parts.empty:
                    c2.download_button("⬇️ Download Partial Report", current_parts.to_csv(index=False), f"Partial_{load_to_find}.csv")
        else:
            st.info("No history found.")
    except Exception as e:
        st.warning("Master Sheets not found or Empty. Please process a file first.")
