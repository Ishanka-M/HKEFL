import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import hashlib
import io
import re

# -------------------- Google Sheets Setup --------------------
def get_gsheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    client = gspread.authorize(creds)
    return client

def get_worksheet(sheet_name):
    client = get_gsheet_client()
    sheet = client.open_by_key("1wLPaUtZ_-xbeIknhBpv4Y8o-X1qMth5KPydEc0TrqDE")
    try:
        ws = sheet.worksheet(sheet_name)
    except:
        ws = sheet.add_worksheet(title=sheet_name, rows=100, cols=20)
    return ws

# -------------------- Load ID Generation --------------------
def generate_load_id(so_number, country, ship_mode):
    ws = get_worksheet("LoadIDs")
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    # Filter for same combination
    mask = (df['SO_Number'].astype(str) == str(so_number)) & \
           (df['Country'] == country) & \
           (df['Ship_Mode'] == ship_mode)
    if mask.any():
        last_seq = df.loc[mask, 'Sequence'].max()
        new_seq = last_seq + 1
    else:
        new_seq = 1
    load_id = f"SO-{so_number}-{new_seq:03d}"
    # Save to sheet
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append_row([load_id, str(so_number), country, ship_mode, new_seq, "Generated", now, ""])
    return load_id

# -------------------- Inventory Availability --------------------
def get_available_inventory(product_upc):
    # Load master inventory
    ws_inv = get_worksheet("InventoryMaster")
    inv_data = ws_inv.get_all_records()
    df_inv = pd.DataFrame(inv_data)
    # Filter by Item Number = product_upc and Status = "Available"
    # Adjust column names as per actual file
    item_col = "Item Number"  # may need to adjust
    status_col = "Status"
    df_prod = df_inv[df_inv[item_col].astype(str) == str(product_upc)]
    df_prod = df_prod[df_prod[status_col] == "Available"]

    # Load picks to subtract
    ws_picks = get_worksheet("Picks")
    picks_data = ws_picks.get_all_records()
    df_picks = pd.DataFrame(picks_data)

    available = []
    for _, row in df_prod.iterrows():
        pallet = row["Pallet"]
        orig_qty = float(row["Actual Qty"])
        # sum picked from this pallet
        picked = df_picks[df_picks["OriginalPalletID"] == pallet]["PickedQty"].sum()
        remaining = orig_qty - picked
        if remaining > 0:
            available.append({
                "Pallet": pallet,
                "Supplier": row["Supplier"],
                "Location": row["Location Id"],
                "Actual Qty": remaining,
                "Original Qty": orig_qty,
                "All Details": row.to_dict()
            })
    return available

# -------------------- Picking Logic --------------------
def get_next_partial_suffix(original_pallet):
    ws_picks = get_worksheet("Picks")
    picks = ws_picks.get_all_records()
    df = pd.DataFrame(picks)
    # find all partials with this original pallet
    mask = df["OriginalPalletID"] == original_pallet
    partials = df.loc[mask, "PartialPalletID"].dropna()
    max_num = 0
    for p in partials:
        match = re.search(rf"{re.escape(original_pallet)}-P(\d+)", p)
        if match:
            max_num = max(max_num, int(match.group(1)))
    return max_num + 1

def pick_for_load(load_id):
    # Get all customer req rows for this load
    ws_cust = get_worksheet("CustomerReqs")
    cust_data = ws_cust.get_all_records()
    df_cust = pd.DataFrame(cust_data)
    df_load = df_cust[df_cust["LoadID"] == load_id]

    picks_log = []
    partial_report = []

    for _, row in df_load.iterrows():
        upc = str(row["Product UPC"])
        req_qty = float(row["PICK QTY"])
        available = get_available_inventory(upc)

        # sort by quantity descending (use largest first)
        available.sort(key=lambda x: x["Actual Qty"], reverse=True)

        to_pick = req_qty
        for inv in available:
            if to_pick <= 0:
                break
            pallet = inv["Pallet"]
            avail_qty = inv["Actual Qty"]
            if avail_qty >= to_pick:
                # take whole to_pick
                picked = to_pick
                partial = False
            else:
                # take all available
                picked = avail_qty
                partial = True

            if partial:
                # generate new partial ID
                suffix = get_next_partial_suffix(pallet)
                partial_id = f"{pallet}-P{suffix:03d}"
                # record in picks log
                picks_log.append({
                    "LoadID": load_id,
                    "ProductUPC": upc,
                    "OriginalPalletID": pallet,
                    "PartialPalletID": partial_id,
                    "PickedQty": picked,
                    "PickDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                # add to partial report
                partial_report.append({
                    "Pallet": pallet,
                    "Supplier": inv["Supplier"],
                    "Load ID": load_id,
                    "Country Name": row["Country Name"],
                    "Actual Qty": inv["Original Qty"],
                    "Partial Qty": picked,
                    "Generated Pallet ID": partial_id
                })
            else:
                # full pallet pick
                picks_log.append({
                    "LoadID": load_id,
                    "ProductUPC": upc,
                    "OriginalPalletID": pallet,
                    "PartialPalletID": "",
                    "PickedQty": picked,
                    "PickDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

            to_pick -= picked

        if to_pick > 0:
            # insufficient stock
            # add remark row to partial report? Or summary
            st.warning(f"Insufficient stock for UPC {upc}. Required {req_qty}, picked {req_qty - to_pick}")

    # Save picks to sheet
    ws_picks = get_worksheet("Picks")
    for p in picks_log:
        ws_picks.append_row(list(p.values()))

    # Update Load status to Completed
    ws_loads = get_worksheet("LoadIDs")
    loads_data = ws_loads.get_all_records()
    df_loads = pd.DataFrame(loads_data)
    row_idx = df_loads[df_loads["LoadID"] == load_id].index[0] + 2  # +2 for header and 1-index
    ws_loads.update(f"F{row_idx}", "Completed")  # assuming Status is column F
    ws_loads.update(f"G{row_idx}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Completed_Date

    return picks_log, partial_report

# -------------------- User Authentication --------------------
def check_password():
    """Simple password check (replace with proper auth)"""
    def password_entered():
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("Password incorrect")
        return False
    else:
        return True

# -------------------- Streamlit UI --------------------
def main():
    st.set_page_config(page_title="Inventory Picking Automation", layout="wide")
    st.title("📦 Inventory Picking Automation System")

    if not check_password():
        st.stop()

    menu = ["Upload Customer Req", "Generate Load IDs", "Picking", "Reports", "Dashboard", "User Management"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Upload Customer Req":
        st.subheader("Upload Customer Requirement File")
        uploaded_file = st.file_uploader("Choose Excel file", type=["xlsx"])
        if uploaded_file:
            df = pd.read_excel(uploaded_file)
            st.dataframe(df.head())
            # Save to Google Sheet "CustomerReqs"
            ws = get_worksheet("CustomerReqs")
            # Clear existing? Or append? We'll append
            ws.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            st.success("File uploaded and saved to Google Sheets.")

    elif choice == "Generate Load IDs":
        st.subheader("Generate Load IDs from Customer Reqs")
        # Load CustomerReqs that don't have LoadID yet
        ws = get_worksheet("CustomerReqs")
        data = ws.get_all_records()
        if not data:
            st.warning("No customer data found. Please upload first.")
        else:
            df = pd.DataFrame(data)
            # Check if LoadID column exists
            if "LoadID" not in df.columns:
                df["LoadID"] = ""
            # Find rows without LoadID
            unassigned = df[df["LoadID"] == ""]
            if unassigned.empty:
                st.info("All rows already have Load IDs.")
            else:
                # Group by SO Number, Country Name, SHIP MODE
                groups = unassigned.groupby(["SO Number", "Country Name", "SHIP MODE: (SEA/AIR)"])
                for (so, country, ship), idx in groups.groups.items():
                    load_id = generate_load_id(so, country, ship)
                    # Assign to all rows in this group
                    for i in idx:
                        df.at[i, "LoadID"] = load_id
                # Update the sheet
                # Need to write back the entire column
                # Simple: replace the whole sheet? Better to update only the LoadID column
                # For simplicity, we'll clear and re-upload
                ws.clear()
                ws.append_rows([df.columns.values.tolist()] + df.values.tolist())
                st.success(f"Generated {len(groups)} Load IDs and updated sheet.")

    elif choice == "Picking":
        st.subheader("Pick for a Load ID")
        # Load list of Load IDs with status "Generated"
        ws_loads = get_worksheet("LoadIDs")
        loads_data = ws_loads.get_all_records()
        df_loads = pd.DataFrame(loads_data)
        generated_loads = df_loads[df_loads["Status"] == "Generated"]["LoadID"].tolist()
        if not generated_loads:
            st.warning("No generated Load IDs available for picking.")
        else:
            selected_load = st.selectbox("Select Load ID", generated_loads)
            if st.button("Start Picking"):
                picks, partials = pick_for_load(selected_load)
                st.success(f"Picking completed for {selected_load}")
                st.subheader("Pick Log")
                st.dataframe(pd.DataFrame(picks))
                st.subheader("Partial Pallet Report")
                st.dataframe(pd.DataFrame(partials))

    elif choice == "Reports":
        st.subheader("Reports")
        report_type = st.radio("Select Report", ["Load ID Summary", "Partial Pallet Report", "Download Pick Report"])
        if report_type == "Load ID Summary":
            ws_loads = get_worksheet("LoadIDs")
            data = ws_loads.get_all_records()
            st.dataframe(pd.DataFrame(data))
        elif report_type == "Partial Pallet Report":
            ws_picks = get_worksheet("Picks")
            picks = ws_picks.get_all_records()
            df = pd.DataFrame(picks)
            partials = df[df["PartialPalletID"] != ""]
            st.dataframe(partials)
        elif report_type == "Download Pick Report":
            # Generate Excel with inventory format + picks
            ws_inv = get_worksheet("InventoryMaster")
            inv_data = ws_inv.get_all_records()
            df_inv = pd.DataFrame(inv_data)
            ws_picks = get_worksheet("Picks")
            picks_data = ws_picks.get_all_records()
            df_picks = pd.DataFrame(picks_data)
            # Merge? For simplicity, we create a new sheet with picks info
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_inv.to_excel(writer, sheet_name="Inventory", index=False)
                df_picks.to_excel(writer, sheet_name="Picks", index=False)
            st.download_button("Download Report", data=output.getvalue(), file_name="pick_report.xlsx")

    elif choice == "Dashboard":
        st.subheader("Dashboard")
        # Show counts
        ws_loads = get_worksheet("LoadIDs")
        loads = pd.DataFrame(ws_loads.get_all_records())
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Load IDs", len(loads))
        col2.metric("Completed", len(loads[loads["Status"]=="Completed"]))
        col3.metric("Pending", len(loads[loads["Status"]=="Generated"]))

        st.subheader("Recent Loads")
        st.dataframe(loads.tail(10))

    elif choice == "User Management":
        st.subheader("User Management")
        st.info("Simple password protection is enabled. To add more users, extend the authentication.")

if __name__ == "__main__":
    main()
