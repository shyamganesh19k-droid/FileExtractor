from flask import Flask, render_template, request, send_file, session, redirect, url_for, make_response, jsonify, flash
import json
import io
import uuid
import time
import pandas as pd
from datetime import datetime
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)
app.secret_key = "supersecretkey"

ALLOWED_EXTENSIONS = {"xlsx"}

# In-memory cache for generated output files.
OUTPUT_CACHE = {}

# Application lists (unchanged)
PROJECT_TEMPLATES = [
    {"id": "BDISTR15PC", "desc": "Basic Distribution 15% Markup"},
    {"id": "BDISTR10PC", "desc": "Basic Distribution 10% Markup"},
    {"id": "BDISTR5PCT", "desc": "Basic Distribution 5% Markup"},
    {"id": "BDISTR0PCT", "desc": "Basic Distribution Passthrough"},
    {"id": "TRSO 15PCT", "desc": "Transmission 15% Markup"},
    {"id": "TRSO 10PCT", "desc": "Transmission 10% Markup"},
    {"id": "TRSO 5PCT", "desc": "Transmission 5% Markup"},
    {"id": "TRSO 0PCT", "desc": "Transmission Passthrough"},
    {"id": "TE 15PCT", "desc": "T&E 15% Markup"},
    {"id": "TE 10PCT", "desc": "T&E 10% Markup"},
    {"id": "TE 5PCT", "desc": "T&E 5% Markup"},
    {"id": "TE 0PCT", "desc": "T&E Passthrough"},
]

CUSTOMERS = [
    {"id": "AEP01", "desc": "American Electric Power (AEP)"},
    {"id": "BAM01", "desc": "Burns And Mcdonnell Consultant, Pc"},
    {"id": "BGE01", "desc": "Baltimore Gas & Electric (BGE)"},
    {"id": "DIG01", "desc": "DigitalPath Inc"},
    {"id": "FER01", "desc": "Ferreira Power West LLC"},
    {"id": "FPL01", "desc": "Florida Light & Power"},
    {"id": "HEC01", "desc": "Hawaiian Electric"},
    {"id": "HEL01", "desc": "Hawaii Electric Light Company"},
    {"id": "LNW01", "desc": "Linewerx Inc."},
    {"id": "MAN01", "desc": "Mana Construction Inc"},
    {"id": "MEC01", "desc": "Maui Electric"},
    {"id": "NEX01", "desc": "Nextera Energy Resources"},
    {"id": "NRE01", "desc": "New River Electrical Corporation"},
    {"id": "PEC01", "desc": "PECO Energy Co."},
    {"id": "POW01", "desc": "Power Pros Powerline Solutions Corp."},
    {"id": "RBA01", "desc": "Ritchie Bros Auctioneers (America) Inc"},
    {"id": "RPLE1", "desc": "RPLE Rokstad Power Line Employees"},
    {"id": "SCE01", "desc": "Southern California Edison"},
    {"id": "TGO01", "desc": "Terra-Gen Operating Company, LLC"},
    {"id": "WFE01", "desc": "Western Farmers Electric Cooperative (WFEC)"},
]

BRANCHES = [
    {"id": "CALI", "desc": "California Office"},
    {"id": "HAWI", "desc": "Hawaii Office"},
    {"id": "NEAS", "desc": "Northeast Office"},
    {"id": "ROK", "desc": "Rok Power"},
    {"id": "RPLE", "desc": "Rok Power Line Employees"},
    {"id": "SOPL", "desc": "Southern Plains"},
    {"id": "TRSO", "desc": "Transmission Solutions"},
]

TASK_TYPES = [
    {"id": "Cost and Revenue Task", "desc": "Cost and Revenue Task"},
    {"id": "Cost Task", "desc": "Cost Task"},
    {"id": "Revenue Task", "desc": "Revenue Task"}
]


@app.after_request
def add_cache_headers(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def load_users():
    try:
        with open("users.json", "r") as f:
            data = json.load(f)
        return data.get("users", [])
    except Exception:
        return []


def find_value_next_to_key_in_df(df, key, search_rows=50):
    key_lower = key.strip().lower()
    nrows = min(len(df), search_rows)
    for r in range(nrows):
        for c in range(len(df.columns)):
            cell = str(df.iat[r, c]).strip()
            if cell.lower() == key_lower:
                if c + 1 < len(df.columns):
                    val = df.iat[r, c + 1]
                    if pd.isna(val):
                        return ""
                    return str(val).strip()
                for cc in range(c + 1, len(df.columns)):
                    val = df.iat[r, cc]
                    if not pd.isna(val) and str(val).strip() != "":
                        return str(val).strip()
    return ""


def extract_pricing_summary_from_bytes(file_bytes):
    project_id = ""
    description = ""
    try:
        bio = io.BytesIO(file_bytes)
        xls = pd.ExcelFile(bio)
        sheet_name = next((s for s in xls.sheet_names if "pricing summary" in s.lower()), None)
        if sheet_name:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None, dtype=str).fillna("")
            project_id = find_value_next_to_key_in_df(df, "PO")
            if not project_id:
                for alt in ["PURCHASE ORDER", "P.O.", "PO #", "PO#"]:
                    if not project_id:
                        project_id = find_value_next_to_key_in_df(df, alt)
            description = find_value_next_to_key_in_df(df, "WORK PACKAGE")
            if not description:
                for alt in ["WORK_PACKAGE", "WORKPACKAGE", "WORK-PACKAGE", "PACKAGE"]:
                    if not description:
                        description = find_value_next_to_key_in_df(df, alt)
    except Exception as e:
        print("extract_pricing_summary_from_bytes error:", e)
    return project_id or "", description or ""


def process_workorders_from_bytes(file_bytes, project_id, project_template, customer_id, branch_id, type_value, description):
    skipped_sheets = []
    all_data = []
    summary_df = pd.DataFrame(columns=[
        "Project ID (Current Project/Job Number)",
        "Project Task (Work Order/Unit)",
        "Description (Pole Number, other Identifier)",
        "Type"
    ])

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        all_sheets = xls.sheet_names

        for sheet in all_sheets:
            if "pricing summary" in sheet.lower():
                continue
            try:
                df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None, dtype=object)
                date_val = ""
                nrows_to_scan = min(20, len(df_raw))
                for r in range(nrows_to_scan):
                    row = df_raw.iloc[r]
                    for c in range(len(row)):
                        cell = str(row.iat[c]).strip().lower()
                        if "date" in cell:
                            if c + 1 < len(row):
                                cand = row.iat[c + 1]
                                parsed = pd.to_datetime(cand, errors="coerce")
                                if parsed is not None and not pd.isna(parsed):
                                    date_val = parsed.strftime("%Y-%b-%d")
                                    break
                    if date_val:
                        break

                project_task_value = ""
                description_value = ""
                header_search_rows = min(20, len(df_raw))
                for r in range(header_search_rows):
                    row_vals = [str(x).strip() for x in df_raw.iloc[r].tolist() if pd.notna(x) and str(x).strip() != ""]
                    for idx, cell in enumerate(row_vals):
                        lower_cell = cell.lower()
                        if "work order" in lower_cell and idx + 1 < len(row_vals):
                            project_task_value = row_vals[idx + 1]
                        if "description" in lower_cell and idx + 1 < len(row_vals):
                            description_value = row_vals[idx + 1]

                header_idx = None
                for i, row in df_raw.iterrows():
                    if any("unit code" in str(x).lower() for x in row if pd.notna(x)):
                        header_idx = i
                        break
                if header_idx is None:
                    skipped_sheets.append(sheet)
                    continue

                table = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, skiprows=header_idx, dtype=object)
                table.columns = [str(c).strip() for c in table.columns]

                qty_col = next((c for c in table.columns if str(c).strip().lower() in ["quantity", "qty", "quan", "quantity "]), None)
                unit_col = next((c for c in table.columns if "unit code" in str(c).lower()), None)
                total_price_col = next((c for c in table.columns if "total price" in str(c).lower()), None)
                price_col = next((c for c in table.columns if str(c).strip().lower() == "price"), None)

                if not unit_col:
                    skipped_sheets.append(sheet)
                    continue

                filtered = table[table[unit_col].notna()].copy()
                if qty_col:
                    filtered[qty_col] = pd.to_numeric(filtered[qty_col], errors="coerce")
                    filtered = filtered[filtered[qty_col].notna() & (filtered[qty_col] > 0)]

                final_df = pd.DataFrame({
                    "Project ID (Current Project/Job Number)": [project_id] * len(filtered),
                    "Project Description": [description] * len(filtered),
                    "Project Template": [project_template] * len(filtered),
                    "Customer ID": [customer_id] * len(filtered),
                    "Branch ID": [branch_id] * len(filtered),
                    "Project Start Date": [date_val] * len(filtered),
                    "Project End Date": [""] * len(filtered),
                    "Project Task": [project_task_value] * len(filtered),
                    "Inventory ID (ex. Unit Code)": filtered[unit_col].astype(str).tolist(),
                    "Quantity": filtered[qty_col].tolist() if qty_col else [""] * len(filtered),
                    "Unit Price": filtered[total_price_col].tolist() if total_price_col else [""] * len(filtered),
                    "Unit Cost": filtered[price_col].tolist() if price_col else [""] * len(filtered),
                    "Cost Code": [""] * len(filtered)
                })

                # ✅ Add $ formatting for currency columns
                for col in ["Unit Price", "Unit Cost"]:
                    if col in final_df.columns:
                        final_df[col] = final_df[col].apply(
                            lambda x: f"${x:,.2f}" if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else x
                        )

                all_data.append(final_df)
            except Exception as e:
                print(f"Error in sheet '{sheet}': {e}")
                skipped_sheets.append(sheet)

        # Summary Sheet
        for sheet in all_sheets:
            if "pricing summary" not in sheet.lower():
                continue
            try:
                df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None, dtype=object)
                header_idx = None
                for i, row in df.iterrows():
                    row_text = [str(x).lower().strip() for x in row if pd.notna(x)]
                    if any("work order" in x for x in row_text) and any("pole" in x for x in row_text):
                        header_idx = i
                        break
                if header_idx is None:
                    for i, row in df.iterrows():
                        row_text = [str(x).lower().strip() for x in row if pd.notna(x)]
                        if any("work order" in x for x in row_text) and (any("description" in x for x in row_text) or any("pole" in x for x in row_text)):
                            header_idx = i
                            break
                if header_idx is None:
                    skipped_sheets.append(sheet)
                    continue
                df2 = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, skiprows=header_idx, dtype=object)
                df2.columns = [str(c).strip().lower() for c in df2.columns]
                wo_col = next((c for c in df2.columns if "work order" in c), None)
                pole_col = next((c for c in df2.columns if "pole" in c or "description" in c), None)
                if not (wo_col and pole_col):
                    skipped_sheets.append(sheet)
                    continue
                df_out = df2[[wo_col, pole_col]].dropna(how="all").copy()
                df_out.columns = [
                    "Project Task (Work Order/Unit)",
                    "Description (Pole Number, other Identifier)"
                ]
                df_out.insert(0, "Project ID (Current Project/Job Number)", project_id)
                df_out["Type"] = type_value
                summary_df = pd.concat([summary_df, df_out], ignore_index=True)
            except Exception as e:
                print(f"Error summary {sheet}: {e}")
                skipped_sheets.append(sheet)

        # Build output Excel in memory
        output_bio = io.BytesIO()
        with pd.ExcelWriter(output_bio, engine="openpyxl") as writer:
            if all_data:
                pd.concat(all_data, ignore_index=True).to_excel(writer, index=False, sheet_name="Work Order Details")
            summary_df.to_excel(writer, index=False, sheet_name="Summary Details")
        output_bio.seek(0)
        output_bytes = output_bio.read()

        # ✅ Output filename pattern
        base_name, ext = os.path.splitext(secure_filename(request.files['file'].filename))
        output_filename = f"cleaned_{base_name}{ext}"

        return output_bytes, output_filename, skipped_sheets

    except Exception as e:
        print("process_workorders_from_bytes error:", e)
        return None, "", skipped_sheets


@app.route("/extract_info", methods=["POST"])
def extract_info():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400
    if not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type. Use .xlsx only."}), 400
    file_bytes = file.read()
    project_id, description = extract_pricing_summary_from_bytes(file_bytes)
    return jsonify({"project_id": project_id, "description": description})


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        users = load_users()
        for user in users:
            if user.get("username") == username and user.get("password") == password:
                session["user"] = username
                return redirect(url_for("home"))
        return render_template("login.html", error="Invalid username or password")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("user", None)
    resp = make_response(redirect("/login"))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/")
def home():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template(
        "index.html",
        project_templates=PROJECT_TEMPLATES,
        customers=CUSTOMERS,
        branches=BRANCHES,
        task_types=TASK_TYPES,
        user=session.get("user")
    )


@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files.get("file")
    if not file or file.filename == "":
        flash("No file selected")
        return redirect(url_for("home"))

    if not allowed_file(file.filename):
        flash("Please upload an Excel (.xlsx) file.")
        return redirect(url_for("home"))

    project_id = request.form.get("project_id", "").strip()
    description = request.form.get("description", "").strip()
    project_template = request.form.get("project_template", "")
    customer_id = request.form.get("customer_id", "")
    branch_id = request.form.get("branch_id", "")
    type_value = request.form.get("type_value", "")

    filename = secure_filename(file.filename)
    file_bytes = file.read()

    if not project_id or not description:
        extracted_project_id, extracted_description = extract_pricing_summary_from_bytes(file_bytes)
        if not project_id and extracted_project_id:
            project_id = extracted_project_id
        if not description and extracted_description:
            description = extracted_description

    if project_id is None:
        project_id = ""

    output_bytes, output_filename, skipped_sheets = process_workorders_from_bytes(
        file_bytes, project_id, project_template, customer_id, branch_id, type_value, description
    )

    if output_bytes is None:
        flash("Failed to process the uploaded file. Check server logs.")
        return redirect(url_for("home"))

    token = str(uuid.uuid4())
    OUTPUT_CACHE[token] = {"bytes": output_bytes, "filename": output_filename, "ts": time.time()}

    return render_template(
        "index.html",
        download_token=token,
        cleaned_file=output_filename,
        skipped_sheets=skipped_sheets,
        project_templates=PROJECT_TEMPLATES,
        customers=CUSTOMERS,
        branches=BRANCHES,
        task_types=TASK_TYPES
    )


@app.route("/download/<token>")
def download_file(token):
    entry = OUTPUT_CACHE.pop(token, None)
    if not entry:
        return "File not found or expired", 404
    bio = io.BytesIO(entry["bytes"])
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name=entry["filename"], mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    app.run(debug=True)
