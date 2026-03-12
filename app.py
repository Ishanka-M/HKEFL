# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import io

st.set_page_config(page_title=“Advanced WMS Picking System”, layout=“wide”, page_icon=“📦”)

SHEET_HEADERS = {
“Users”: [“Username”, “Password”, “Role”],
“Load_History”: [“Batch ID”, “Generated Load ID”, “SO Number”, “Country Name”, “SHIP MODE”, “Date”, “Pick Status”],
“Master_Pick_Data”: [],
“Master_Partial_Data”: [“Batch ID”, “SO Number”, “Pallet”, “Supplier”, “Load ID”, “Country Name”, “Actual Qty”, “Partial Qty”, “Gen Pallet ID”],
“Summary_Data”: [“Batch ID”, “SO Number”, “Load ID”, “UPC”, “Country”, “Ship Mode”, “Requested”, “Picked”, “Variance”, “Status”]
}

def footer_branding():
st.markdown(”””
<style>
.footer { position: fixed; left: 0; bottom: 0; width: 100%;
text-align: center; color: #888; font-size: 13px;
padding: 10px; font-weight: bold; }
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

def ensure_sheet_headers(ws, headers):
if not headers:
return
try:
existing = ws.row_values(1)
except Exception:
existing = []
if not existing or all(v.strip() == “” for v in existing):
ws.update(“A1”, [headers])
time.sleep(0.3)

def get_or_create_sheet(sh, name, headers=None):
try:
ws = sh.worksheet(name)
except gspread.exceptions.WorksheetNotFound:
ws = sh.add_worksheet(title=name, rows=“5000”, cols=“70”)
time.sleep(0.5)
if headers:
ensure_sheet_headers(ws, headers)
return ws

def ensure_dynamic_sheet_headers(sh, sheet_name, columns):
try:
ws = sh.worksheet(sheet_name)
except gspread.exceptions.WorksheetNotFound:
ws = sh.add_worksheet(title=sheet_name, rows=“5000”, cols=“70”)
time.sleep(0.5)
try:
existing_headers = ws.row_values(1)
except Exception:
existing_headers = []
if not existing_headers or all(v.strip() == “” for v in existing_headers):
ws.update(“A1”, [list(columns)])
time.sleep(0.3)
return ws

def get_safe_dataframe(sh, sheet_name):
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
for sheet_name, headers in SHEET_HEADERS.items():
if sheet_name == “Master_Pick_Data”:
continue
get_or_create_sheet(sh, sheet_name, headers)

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
st.sidebar.title(“WMS Login”)
if “logged_in” not in st.session_state:
st.session_state[“logged_in”] = False
if not st.session_state[“logged_in”]:
try:
sh = get_master_workbook()
users_df = init_users_sheet(sh)
user = st.sidebar.text_input(“Username”)
pw = st.sidebar.text_input(“Password”, type=“password”)
if st.sidebar.button(“Login”, type=“primary”):
match = users_df[
(users_df[“Username”] == user) &
(users_df[“Password”] == str(pw))
]
if not match.empty:
st.session_state.update({
“logged_in”: True,
“role”: match.iloc[0][“Role”],
“username”: user
})
st.rerun()
else:
st.sidebar.error(“Invalid Username or Password!”)
except Exception as e:
st.sidebar.error(“Google Sheets connection error: “ + str(e))
return False
return True

def reconcile_inventory(inv_df, sh):
try:
pick_history = get_safe_dataframe(sh, “Master_Pick_Data”)
if not pick_history.empty and “Actual Qty” in pick_history.columns:
pick_history[“Actual Qty”] = pd.to_numeric(pick_history[“Actual Qty”], errors=“coerce”).fillna(0)
pick_summary = pick_history.groupby(“Pallet”)[“Actual Qty”].sum().reset_index()
pick_summary.columns = [“Pallet”, “Total_Picked”]
inv_df = pd.merge(inv_df, pick_summary, on=“Pallet”, how=“left”)
inv_df[“Total_Picked”] = inv_df[“Total_Picked”].fillna(0)
inv_df[“Actual Qty”] = (
pd.to_numeric(inv_df[“Actual Qty”], errors=“coerce”).fillna(0) - inv_df[“Total_Picked”]
)
inv_df = inv_df[inv_df[“Actual Qty”] > 0].drop(columns=[“Total_Picked”])
except Exception:
pass
return inv_df

def process_picking(inv_df, req_df, batch_id):
pick_rows = []
partial_rows = []
summary_rows = []
verify_rows = []

```
temp_inv = inv_df.copy()
temp_inv["Actual Qty"] = pd.to_numeric(temp_inv["Actual Qty"], errors="coerce").fillna(0)

for lid in req_df["Generated Load ID"].unique():
    curr_reqs = req_df[req_df["Generated Load ID"] == lid]
    so_num = str(curr_reqs["SO Number"].iloc[0])
    country = str(curr_reqs["Country Name"].iloc[0]) if "Country Name" in curr_reqs.columns else ""
    ship_mode = str(curr_reqs["SHIP MODE: (SEA/AIR)"].iloc[0]) if "SHIP MODE: (SEA/AIR)" in curr_reqs.columns else ""

    for _, req in curr_reqs.iterrows():
        upc = str(req["Product UPC"])
        orig_needed = float(req["PICK QTY"])
        needed = orig_needed

        stock = temp_inv[temp_inv["Supplier"].astype(str) == upc].sort_values(by="Actual Qty", ascending=False)
        picked_qty = 0

        for idx, item in stock.iterrows():
            if needed <= 0:
                break
            avail = float(item["Actual Qty"])
            if avail <= 0:
                continue
            take = min(avail, needed)

            p_row = item.copy()
            p_row["Batch ID"] = batch_id
            p_row["SO Number"] = so_num
            p_row["Actual Qty"] = take
            p_row["Load Id"] = lid
            p_row["Customer Po Number"] = country + "-" + str(lid)

            pick_id = "P-" + batch_id[-8:] + "-" + datetime.now().strftime("%H%M%S%f")[:10]
            if "Pick ID" in p_row.index:
                p_row["Pick ID"] = pick_id
            elif "Gen Pallet ID" in p_row.index:
                p_row["Gen Pallet ID"] = pick_id

            pick_rows.append(p_row)

            if take < avail:
                partial_rows.append({
                    "Batch ID": batch_id,
                    "SO Number": so_num,
                    "Pallet": item.get("Pallet", ""),
                    "Supplier": upc,
                    "Load ID": lid,
                    "Country Name": country,
                    "Actual Qty": avail,
                    "Partial Qty": take,
                    "Gen Pallet ID": pick_id
                })

            temp_inv.at[idx, "Actual Qty"] -= take
            needed -= take
            picked_qty += take

        variance = orig_needed - picked_qty
        status = "Full" if variance == 0 else ("Partial" if picked_qty > 0 else "Unfulfilled")

        summary_rows.append({
            "Batch ID": batch_id,
            "SO Number": so_num,
            "Load ID": lid,
            "UPC": upc,
            "Country": country,
            "Ship Mode": ship_mode,
            "Requested": orig_needed,
            "Picked": picked_qty,
            "Variance": variance,
            "Status": status
        })
        verify_rows.append({
            "Load ID": lid,
            "SO Number": so_num,
            "UPC": upc,
            "Country": country,
            "Requested Qty": orig_needed,
            "Picked Qty": picked_qty,
            "Variance": variance,
            "Status": status,
            "Match": "YES" if variance == 0 else "NO"
        })

return (
    pd.DataFrame(pick_rows),
    pd.DataFrame(partial_rows),
    pd.DataFrame(summary_rows),
    pd.DataFrame(verify_rows)
)
```

# ============================================================

# MAIN APP

# ============================================================

if login_section():
sh = get_master_workbook()

```
with st.spinner("Initializing sheets..."):
    init_all_sheets(sh)

role = st.session_state["role"].upper()
menu_options = ["Dashboard and Tracking", "Picking Operations", "Revert Delete Picks"]
if role == "ADMIN":
    menu_options.append("Admin Settings")

choice = st.sidebar.radio("Menu", menu_options)
st.sidebar.markdown("---")
st.sidebar.caption("User: " + st.session_state["username"] + " (" + st.session_state["role"] + ")")
if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.rerun()

# ============================================================
if choice == "Picking Operations":
    st.title("Picking Operations")

    inv_f = st.file_uploader("Inventory Report (CSV or Excel)", type=["csv", "xlsx", "xls"])
    req_f = st.file_uploader("Customer Requirement (CSV or Excel)", type=["csv", "xlsx", "xls"])

    if inv_f and req_f:
        inv = pd.read_csv(inv_f) if inv_f.name.endswith("csv") else pd.read_excel(inv_f)
        req = pd.read_csv(req_f) if req_f.name.endswith("csv") else pd.read_excel(req_f)

        with st.expander("Preview Uploaded Files", expanded=False):
            c1, c2 = st.columns(2)
            c1.subheader("Inventory Report")
            c1.dataframe(inv.head(10), use_container_width=True)
            c1.caption("Rows: " + str(len(inv)) + " | Cols: " + str(list(inv.columns)))
            c2.subheader("Customer Requirement")
            c2.dataframe(req.head(10), use_container_width=True)
            c2.caption("Rows: " + str(len(req)) + " | Cols: " + str(list(req.columns)))

        inv_required = ["Supplier", "Actual Qty", "Pallet"]
        req_required = ["SO Number", "Product UPC", "PICK QTY"]
        inv_missing = [c for c in inv_required if c not in inv.columns]
        req_missing = [c for c in req_required if c not in req.columns]

        if inv_missing:
            st.error("Inventory file missing columns: " + str(inv_missing))
        elif req_missing:
            st.error("Requirement file missing columns: " + str(req_missing))
        else:
            if st.button("Preview and Verify Picks", type="secondary"):
                with st.spinner("Generating preview..."):
                    req_copy = req.copy()
                    req_copy["Generated Load ID"] = "SO-" + req_copy["SO Number"].astype(str) + "-001"
                    inv_rec = reconcile_inventory(inv.copy(), sh)
                    _, _, _, verify_df = process_picking(inv_rec, req_copy, "PREVIEW")
                st.session_state["verify_df"] = verify_df
                st.session_state["inv_ready"] = inv
                st.session_state["req_ready"] = req
                st.rerun()

            if "verify_df" in st.session_state and st.session_state["verify_df"] is not None:
                vdf = st.session_state["verify_df"]
                st.subheader("Verification - Requirement vs Pick Preview")

                total_req = vdf["Requested Qty"].sum()
                total_pick = vdf["Picked Qty"].sum()
                total_var = vdf["Variance"].sum()
                full_ct = (vdf["Status"] == "Full").sum()
                part_ct = (vdf["Status"] == "Partial").sum()
                unful_ct = (vdf["Status"] == "Unfulfilled").sum()

                vc1, vc2, vc3, vc4, vc5, vc6 = st.columns(6)
                vc1.metric("Requested", str(int(total_req)))
                vc2.metric("Picked", str(int(total_pick)))
                vc3.metric("Variance", str(int(total_var)))
                vc4.metric("Full", str(full_ct))
                vc5.metric("Partial", str(part_ct))
                vc6.metric("Unfulfilled", str(unful_ct))

                def color_status(row):
                    if row["Status"] == "Full":
                        return ["background-color: #d4edda"] * len(row)
                    elif row["Status"] == "Partial":
                        return ["background-color: #fff3cd"] * len(row)
                    return ["background-color: #f8d7da"] * len(row)

                st.dataframe(vdf.style.apply(color_status, axis=1), use_container_width=True, height=400)

                buf_v = io.BytesIO()
                vdf.to_excel(buf_v, index=False)
                st.download_button(
                    "Download Verification Report",
                    data=buf_v.getvalue(),
                    file_name="verification_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.divider()
                if unful_ct > 0:
                    st.warning(str(unful_ct) + " line(s) cannot be fulfilled. Confirm to proceed anyway?")

                col_confirm, col_cancel = st.columns([1, 4])
                with col_confirm:
                    if st.button("Confirm and Save Picks", type="primary"):
                        with st.spinner("Saving to Google Sheets..."):
                            batch_id = "REQ-" + datetime.now().strftime("%Y%m%d-%H%M%S")
                            req_final = st.session_state["req_ready"].copy()
                            req_final["Generated Load ID"] = "SO-" + req_final["SO Number"].astype(str) + "-001"
                            inv_final = reconcile_inventory(st.session_state["inv_ready"].copy(), sh)
                            p_df, part_df, s_df, _ = process_picking(inv_final, req_final, batch_id)

                            if not p_df.empty:
                                ws_pick = ensure_dynamic_sheet_headers(sh, "Master_Pick_Data", p_df.columns)
                                ws_pick.append_rows(p_df.astype(str).values.tolist())

                            if not part_df.empty:
                                ws_part = get_or_create_sheet(sh, "Master_Partial_Data", SHEET_HEADERS["Master_Partial_Data"])
                                for col in SHEET_HEADERS["Master_Partial_Data"]:
                                    if col not in part_df.columns:
                                        part_df[col] = ""
                                ws_part.append_rows(part_df[SHEET_HEADERS["Master_Partial_Data"]].astype(str).values.tolist())

                            if not s_df.empty:
                                ws_sum = get_or_create_sheet(sh, "Summary_Data", SHEET_HEADERS["Summary_Data"])
                                for col in SHEET_HEADERS["Summary_Data"]:
                                    if col not in s_df.columns:
                                        s_df[col] = ""
                                ws_sum.append_rows(s_df[SHEET_HEADERS["Summary_Data"]].astype(str).values.tolist())

                            ws_hist = get_or_create_sheet(sh, "Load_History", SHEET_HEADERS["Load_History"])
                            for lid in req_final["Generated Load ID"].unique():
                                row_data = req_final[req_final["Generated Load ID"] == lid].iloc[0]
                                ws_hist.append_row([
                                    batch_id,
                                    lid,
                                    str(row_data.get("SO Number", "")),
                                    str(row_data.get("Country Name", "")),
                                    str(row_data.get("SHIP MODE: (SEA/AIR)", "")),
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "Completed"
                                ])

                        st.session_state.pop("verify_df", None)
                        st.session_state.pop("inv_ready", None)
                        st.session_state.pop("req_ready", None)
                        st.success("Picking Saved! Batch ID: " + batch_id)
                        st.balloons()

                        if not p_df.empty:
                            buf2 = io.BytesIO()
                            p_df.to_excel(buf2, index=False)
                            st.download_button(
                                "Download Pick File",
                                data=buf2.getvalue(),
                                file_name="picks_" + batch_id + ".xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                with col_cancel:
                    if st.button("Cancel"):
                        st.session_state.pop("verify_df", None)
                        st.session_state.pop("inv_ready", None)
                        st.session_state.pop("req_ready", None)
                        st.rerun()

# ============================================================
elif choice == "Dashboard and Tracking":
    col1, col2 = st.columns([4, 1])
    col1.title("Dashboard and Tracking")
    if col2.button("Refresh"):
        st.rerun()

    with st.spinner("Loading data..."):
        hist_df    = get_safe_dataframe(sh, "Load_History")
        pick_df    = get_safe_dataframe(sh, "Master_Pick_Data")
        partial_df = get_safe_dataframe(sh, "Master_Partial_Data")
        summary_df = get_safe_dataframe(sh, "Summary_Data")

    with st.expander("Sheet Status Debug", expanded=False):
        dc = st.columns(4)
        dc[0].info("Load_History\nRows: " + str(len(hist_df)))
        dc[1].info("Master_Pick_Data\nRows: " + str(len(pick_df)))
        dc[2].info("Master_Partial_Data\nRows: " + str(len(partial_df)))
        dc[3].info("Summary_Data\nRows: " + str(len(summary_df)))

    st.subheader("Overview")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Batches", hist_df["Batch ID"].nunique() if not hist_df.empty and "Batch ID" in hist_df.columns else 0)
    m2.metric("Loads", hist_df["Generated Load ID"].nunique() if not hist_df.empty and "Generated Load ID" in hist_df.columns else 0)
    m3.metric("Pick Rows", len(pick_df) if not pick_df.empty else 0)
    m4.metric("Partial Picks", len(partial_df) if not partial_df.empty else 0)
    if not summary_df.empty and "Variance" in summary_df.columns:
        total_var = pd.to_numeric(summary_df["Variance"], errors="coerce").fillna(0).sum()
        m5.metric("Total Variance", str(int(total_var)))
    else:
        m5.metric("Total Variance", "N/A")

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["Load History", "Pick Data", "Partial Data", "Summary"])

    with tab1:
        if not hist_df.empty:
            if "Batch ID" in hist_df.columns:
                batches = ["All"] + sorted(hist_df["Batch ID"].dropna().unique().tolist())
                sel = st.selectbox("Filter by Batch ID:", batches, key="hist_filter")
                filtered = hist_df if sel == "All" else hist_df[hist_df["Batch ID"] == sel]
            else:
                filtered = hist_df
            st.dataframe(filtered, use_container_width=True, height=400)
            st.caption("Rows: " + str(len(filtered)))
        else:
            st.warning("Load_History is empty. Run a Picking Operation first.")

    with tab2:
        if not pick_df.empty:
            col_a, col_b = st.columns([3, 1])
            show_all = col_b.checkbox("Show all columns", key="pick_all")
            if "Batch ID" in pick_df.columns:
                opts = ["All"] + sorted(pick_df["Batch ID"].dropna().unique().tolist())
                sel_p = col_a.selectbox("Filter by Batch:", opts, key="pick_filter")
                fp = pick_df if sel_p == "All" else pick_df[pick_df["Batch ID"] == sel_p]
            else:
                fp = pick_df
            key_cols = ["Batch ID", "SO Number", "Load Id", "Pallet", "Supplier", "Actual Qty", "Customer Po Number", "Pick ID", "Gen Pallet ID"]
            disp = [c for c in key_cols if c in fp.columns]
            st.dataframe(fp if show_all or not disp else fp[disp], use_container_width=True, height=400)
            st.caption("Rows: " + str(len(fp)))
            buf = io.BytesIO()
            fp.to_excel(buf, index=False)
            st.download_button(
                "Download Pick Data",
                data=buf.getvalue(),
                file_name="pick_data_" + datetime.now().strftime("%Y%m%d") + ".xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("Master_Pick_Data is empty.")

    with tab3:
        if not partial_df.empty:
            st.dataframe(partial_df, use_container_width=True, height=400)
            st.caption("Rows: " + str(len(partial_df)))
        else:
            st.info("No partial picks.")

    with tab4:
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True, height=400)
            if "UPC" in summary_df.columns and "Variance" in summary_df.columns:
                chart_df = summary_df.copy()
                chart_df["Variance"] = pd.to_numeric(chart_df["Variance"], errors="coerce").fillna(0)
                chart_df = chart_df[chart_df["Variance"] != 0]
                if not chart_df.empty:
                    st.subheader("Variance by UPC")
                    st.bar_chart(chart_df.set_index("UPC")["Variance"])
        else:
            st.warning("Summary_Data is empty.")

# ============================================================
elif choice == "Revert Delete Picks":
    st.title("Revert and Delete Picks")
    hist_df = get_safe_dataframe(sh, "Load_History")
    if not hist_df.empty and "Batch ID" in hist_df.columns:
        batch_options = hist_df["Batch ID"].unique().tolist()
        selected_batch = st.selectbox("Select Batch to Revert:", batch_options)
        st.dataframe(hist_df[hist_df["Batch ID"] == selected_batch], use_container_width=True)
        if st.button("Revert Selected Batch", type="primary"):
            st.warning("Revert logic for " + selected_batch + " - implement row deletion as needed.")
    else:
        st.info("No batches found.")

# ============================================================
elif choice == "Admin Settings":
    st.title("Admin Settings")

    st.subheader("Reset Sheets")
    sheet_to_clear = st.selectbox(
        "Select Sheet to Reset:",
        ["Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History", "ALL_DATA"]
    )
    if st.button("Reset Sheet", type="primary"):
        targets = (
            ["Master_Pick_Data", "Master_Partial_Data", "Summary_Data", "Load_History"]
            if sheet_to_clear == "ALL_DATA"
            else [sheet_to_clear]
        )
        progress = st.progress(0)
        for i, sname in enumerate(targets):
            try:
                ws = sh.worksheet(sname)
                ws.clear()
                time.sleep(0.3)
                headers = SHEET_HEADERS.get(sname, [])
                if headers:
                    ws.update("A1", [headers])
                    time.sleep(0.3)
                progress.progress((i + 1) / len(targets))
            except gspread.exceptions.WorksheetNotFound:
                st.warning("Sheet not found: " + sname)
        st.success("Reset complete for: " + ", ".join(targets))

    st.divider()
    st.subheader("User Management")
    users_df = get_safe_dataframe(sh, "Users")
    if not users_df.empty:
        st.dataframe(users_df[["Username", "Role"]], use_container_width=True)

    st.subheader("Add New User")
    c1, c2, c3 = st.columns(3)
    new_user = c1.text_input("Username")
    new_pw   = c2.text_input("Password", type="password")
    new_role = c3.selectbox("Role", ["SysUser", "admin"])
    if st.button("Add User"):
        if new_user and new_pw:
            ws_u = get_or_create_sheet(sh, "Users", SHEET_HEADERS["Users"])
            ws_u.append_row([new_user, new_pw, new_role])
            st.success("User added: " + new_user)
            st.rerun()
        else:
            st.error("Please enter Username and Password.")
```

footer_branding()