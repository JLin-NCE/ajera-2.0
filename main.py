import requests
import concurrent.futures
import os
import json
import sys
import traceback
from datetime import datetime, timedelta
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill
import pandas as pd
import urllib.parse

# ========================
# CONFIGURATION
# ========================
def validate_url(url):
    """Validate and potentially fix the API URL"""
    try:
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme:
            url = f"https://{url}"
        return url
    except Exception as e:
        print(f"❌ Error validating URL: {str(e)}")
        raise

# Base configuration
AJERA_BASE_URL = validate_url("https://ajera.com/V006275/AjeraAPI.ashx?ew0KICAiQ2xpZW50SUQiOiA2Mjc1LA0KICAiRGF0YWJhc2VJRCI6IDEzMDI5LA0KICAiSXNTYW1wbGVEYXRhIjogZmFsc2UNCn0%3d")
CREDENTIALS = {
    "USERNAME": "powerquerynv",
    "PASSWORD": "Testt1m3!!123"
}
FILE_NAMES = {
    "NEW": "new_data.xlsx",
    "PREVIOUS": "previous_data.xlsx",
    "BACKLOG_DIR": "backlog"
}
BATCH_CONFIG = {
    "SIZE": 10,
    "MAX_THREADS": 5
}

# ========================
# API ERROR HANDLING
# ========================
class AjeraAPIError(Exception):
    """Custom exception for Ajera API errors"""
    def __init__(self, message, error_id=None, response=None):
        self.message = message
        self.error_id = error_id
        self.response = response
        super().__init__(self.message)

def check_api_response(response_json):
    """Check API response for errors"""
    if "Errors" in response_json and response_json["Errors"]:
        error = response_json["Errors"][0]
        error_message = error.get("ErrorMessage", "Unknown error")
        error_id = error.get("ErrorID")
        
        if error_id == -150:
            raise AjeraAPIError(
                f"Authentication failed: {error_message}. Please verify your credentials and API URL.",
                error_id,
                response_json
            )
        else:
            raise AjeraAPIError(
                f"API Error {error_id}: {error_message}",
                error_id,
                response_json
            )

# ========================
# DIRECTORY MANAGEMENT
# ========================
def get_current_week_dir():
    """Get the directory for the current week, starting from Monday."""
    try:
        today = datetime.today()
        start_of_week = today - timedelta(days=today.weekday())
        week_dir = os.path.join(FILE_NAMES["BACKLOG_DIR"], start_of_week.strftime("%Y-%m-%d"))
        
        os.makedirs(week_dir, exist_ok=True)
        print(f"📁 Created/verified directory: {week_dir}")
        return week_dir
    except Exception as e:
        print(f"❌ Error creating directory: {str(e)}")
        raise

# ========================
# CORE API FUNCTIONS
# ========================
def create_session():
    """Establish API connection with enhanced error handling"""
    try:
        payload = {
            "Method": "CreateAPISession",
            "Username": CREDENTIALS["USERNAME"],
            "Password": CREDENTIALS["PASSWORD"],
            "APIVersion": 2
        }
        
        print(f"\n🔍 Attempting to connect to: {AJERA_BASE_URL}")
        print(f"👤 Using username: {CREDENTIALS['USERNAME']}")
        
        # Test URL availability first
        try:
            test_response = requests.get(AJERA_BASE_URL, timeout=5)
            print(f"🌐 API endpoint status: {test_response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Warning: Could not reach API endpoint: {str(e)}")
            print("🔄 Proceeding with login attempt anyway...")
        
        response = requests.post(AJERA_BASE_URL, json=payload)
        
        # Print raw response for debugging
        print(f"📡 Raw response status: {response.status_code}")
        print(f"📡 Raw response headers: {dict(response.headers)}")
        print(f"📡 Raw response content: {response.text}")
        
        # Check status code before processing JSON
        response.raise_for_status()
        
        json_response = response.json()
        
        # Check for API-specific errors
        check_api_response(json_response)
        
        # Validate response structure
        if "Content" not in json_response:
            raise KeyError("Response missing 'Content' field")
            
        if "SessionToken" not in json_response["Content"]:
            raise KeyError("Response missing 'SessionToken' in Content")
            
        session_token = json_response["Content"]["SessionToken"]
        print(f"✅ Successfully obtained session token: {session_token[:10]}...")
        
        return session_token
        
    except requests.exceptions.RequestException as e:
        print(f"\n🌐 Network error: {str(e)}")
        print("\n💡 Troubleshooting tips:")
        print("1. Verify the API URL is correct")
        print("2. Check your network connection")
        print("3. Ensure the API endpoint is accessible from your network")
        print(f"4. Try accessing {AJERA_BASE_URL} in a browser")
        raise
    except json.JSONDecodeError as e:
        print(f"\n📝 JSON parsing error: {str(e)}")
        print(f"📝 Raw content that couldn't be parsed: {response.text}")
        raise
    except AjeraAPIError as e:
        print(f"\n🚫 Ajera API Error (ID: {e.error_id}): {e.message}")
        if e.error_id == -150:
            print("\n💡 Troubleshooting tips:")
            print("1. Verify your username and password")
            print("2. Check if your API URL includes the correct company ID")
            print("3. Ensure your account has API access permissions")
            print("4. Try logging into the Ajera web interface to verify credentials")
        raise
    except KeyError as e:
        print(f"\n🔑 Response structure error: {str(e)}")
        print(f"🔑 Actual response structure: {json_response}")
        raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        raise

def fetch_projects(session_token):
    """Retrieve active projects with enhanced error handling"""
    try:
        payload = {
            "Method": "ListProjects",
            "SessionToken": session_token,
            "MethodArguments": {"FilterByStatus": ["Active"]}
        }
        
        print("\n📤 Sending project list request...")
        response = requests.post(AJERA_BASE_URL, json=payload)
        response.raise_for_status()
        
        json_response = response.json()
        check_api_response(json_response)
        
        projects = json_response["Content"].get("Projects", [])
        if not projects:
            print("⚠️ Warning: No active projects found")
        else:
            print(f"✅ Successfully retrieved {len(projects)} projects")
        
        return projects
        
    except Exception as e:
        print(f"\n❌ Error fetching projects: {str(e)}")
        raise

# ========================
# DATA PROCESSING
# ========================
def process_batches(session_token, projects):
    """Parallel processing of project batches with enhanced error handling"""
    try:
        if not projects:
            print("⚠️ No projects to process")
            return []

        print(f"\n🔄 Starting batch processing of {len(projects)} projects...")
        results = []
        failed_batches = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_CONFIG["MAX_THREADS"]) as executor:
            futures = []
            
            for i in range(0, len(projects), BATCH_CONFIG["SIZE"]):
                batch = projects[i:i+BATCH_CONFIG["SIZE"]]
                print(f"📦 Queuing batch {i//BATCH_CONFIG['SIZE'] + 1} with {len(batch)} projects")
                futures.append(executor.submit(
                    fetch_project_details, session_token, [p["ProjectKey"] for p in batch]
                ))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    if result := future.result():
                        results.extend(result.get("Projects", []))
                    else:
                        failed_batches += 1
                except Exception as e:
                    print(f"⚠️ Batch processing error: {str(e)}")
                    failed_batches += 1
            
        if failed_batches:
            print(f"⚠️ {failed_batches} batch(es) failed to process")
        
        print(f"✅ Successfully processed {len(results)} projects in {len(futures)} batches")
        return results
            
    except Exception as e:
        print(f"\n❌ Error in batch processing: {str(e)}")
        raise

def fetch_project_details(session_token, batch):
    """Fetch details for a batch of projects with enhanced error handling"""
    try:
        payload = {
            "Method": "GetProjects",
            "SessionToken": session_token,
            "MethodArguments": {"RequestedProjects": batch}
        }
        
        print(f"📤 Fetching details for batch of {len(batch)} projects...")
        response = requests.post(AJERA_BASE_URL, json=payload)
        response.raise_for_status()
        
        json_response = response.json()
        check_api_response(json_response)
        
        return json_response["Content"]
        
    except Exception as e:
        print(f"⚠️ Error fetching project batch: {str(e)}")
        return None

# ========================
# DATA COMPARISON ENGINE
# ========================
class DataComparator:
    def __init__(self):
        self.current_data = {"projects": {}, "phases": {}}
        self.old_data = {"projects": {}, "phases": {}}
    
    def load_data(self):
        """Load previous and new datasets with enhanced error handling"""
        try:
            print("\n📂 Loading data files...")
            if os.path.exists(FILE_NAMES["PREVIOUS"]):
                print("📚 Loading previous data...")
                self.old_data = self.read_excel(FILE_NAMES["PREVIOUS"])
                print(f"✅ Loaded {len(self.old_data['projects'])} previous projects")
            else:
                print("ℹ️ No previous data file found")
            
            print("📚 Loading current data...")
            self.current_data = self.read_excel(FILE_NAMES["NEW"])
            print(f"✅ Loaded {len(self.current_data['projects'])} current projects")
            
        except Exception as e:
            print(f"\n❌ Error loading data: {str(e)}")
            raise
    
    def read_excel(self, filename):
        """Read Excel file into structured format with enhanced error handling"""
        try:
            print(f"📖 Reading file: {filename}")
            wb = load_workbook(filename)
            data = {
                "projects": self.parse_sheet(wb["Projects"], "project"),
                "phases": self.parse_sheet(wb["Phases"], "phase")
            }
            print(f"✅ Successfully read {filename}")
            return data
            
        except Exception as e:
            print(f"\n❌ Error reading Excel file {filename}: {str(e)}")
            raise
    
    def parse_sheet(self, ws, data_type):
        """Parse sheet into a structured dictionary with enhanced error handling"""
        try:
            data = {}
            row_count = 0
            
            for row in ws.iter_rows(min_row=2, values_only=True):
                if not any(row):  # Skip empty rows
                    continue
                
                if data_type == "project":
                    key = row[0]  # Project ID
                    amount = row[2]  # Total Contract Amount
                else:
                    key = (row[0], row[1])  # (Project ID, Phase ID)
                    amount = row[3]  # Phase Contract Amount
                
                if key in data:
                    print(f"⚠️ Warning: Duplicate {data_type} key found: {key}")
                
                data[key] = {"amount": amount, "raw": row}
                row_count += 1
            
            print(f"✅ Parsed {row_count} rows from {ws.title} sheet")
            return data
            
        except Exception as e:
            print(f"\n❌ Error parsing sheet: {str(e)}")
            raise
    
    def detect_changes(self):
        """Identify changes in both project-level and phase-level contract amounts"""
        try:
            print("\n🔍 Analyzing changes...")
            changes = []
            total_comparisons = 0
            
            # Compare project-level Total Contract Amount
            for project_id, current in self.current_data["projects"].items():
                old = self.old_data["projects"].get(project_id, {"amount": 0})
                total_comparisons += 1
                
                if old["amount"] != current["amount"]:
                    changes.append({
                        "Type": "Project",
                        "Project ID": project_id,
                        "Phase ID": "N/A",
                        "Old Amount": old["amount"],
                        "New Amount": current["amount"],
                        "Change": current["amount"] - old["amount"]
                    })
            
            # Compare phase-level Phase Contract Amount
            for phase_key, current in self.current_data["phases"].items():
                old = self.old_data["phases"].get(phase_key, {"amount": 0})
                total_comparisons += 1
                
                if old["amount"] != current["amount"]:
                    changes.append({
                        "Type": "Phase",
                        "Project ID": phase_key[0],
                        "Phase ID": phase_key[1],
                        "Old Amount": old["amount"],
                        "New Amount": current["amount"],
                        "Change": current["amount"] - old["amount"]
                    })
            
            print(f"✅ Compared {total_comparisons} items and found {len(changes)} changes")
            return changes
            
        except Exception as e:
            print(f"\n❌ Error detecting changes: {str(e)}")
            raise

# ========================
# REPORTING & VISUALS
# ========================
class ReportGenerator:
    def __init__(self, changes):
        self.changes = changes
        self.week_dir = get_current_week_dir()
    
    def create_backlog(self):
        """Save changes to a backlog Excel file with enhanced error handling"""
        try:
            if not self.changes:
                print("\nℹ️ No changes to report")
                return None

            filename = os.path.join(
                self.week_dir, 
                f"backlog_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
            )
            
            print(f"\n📝 Creating backlog file: {filename}")
            wb = Workbook()
            ws = wb.active
            ws.title = "Phase Changes"
            
            # Add headers with styling
            headers = [
                "Project ID", "Phase ID", "Old Amount", 
                "New Amount", "Change"
            ]
            ws.append(headers)
            
            # Style header row
            for cell in ws[1]:
                cell.font = Font(bold=True)
            
            # Add data
            for change in self.changes:
                row = [
                    change["Project ID"],
                    change["Phase ID"],
                    change["Old Amount"],
                    change["New Amount"],
                    change["Change"]
                ]
                ws.append(row)
                
                # Color negative changes in red, positive in green
                change_cell = ws.cell(row=ws.max_row, column=5)
                if change["Change"] < 0:
                    change_cell.fill = PatternFill(start_color="FFCDD2", end_color="FFCDD2", fill_type="solid")
                elif change["Change"] > 0:
                    change_cell.fill = PatternFill(start_color="C8E6C9", end_color="C8E6C9", fill_type="solid")
            
            wb.save(filename)
            print(f"✅ Backlog file created successfully with {len(self.changes)} changes")
            return filename
            
        except Exception as e:
            print(f"\n❌ Error creating backlog: {str(e)}")
            raise
    
    def generate_table_view(self):
        """Display phase differences as a table with enhanced error handling"""
        try:
            print("\n📊 Generating table view...")
            df = pd.DataFrame(self.changes)
            
            if df.empty:
                print("✅ No significant changes detected")
                return
            
            df_sorted = df.sort_values(by=["Project ID", "Phase ID"])
            print("\nChanges detected:")
            print(df_sorted.to_string(index=False))
            
            # Print summary statistics
            print("\nSummary:")
            print(f"Total changes: {len(df)}")
            print(f"Total absolute change: {abs(df['Change']).sum():,.2f}")
            print(f"Average change: {df['Change'].mean():,.2f}")
            print(f"Largest increase: {df['Change'].max():,.2f}")
            print(f"Largest decrease: {df['Change'].min():,.2f}")
            
        except Exception as e:
            print(f"\n❌ Error generating table view: {str(e)}")
            raise

# ========================
# EXCEL WRITING FUNCTION
# ========================
def _populate_sheets(wb, data):
    """Populate Excel sheets with project and phase data with enhanced error handling"""
    try:
        print("\n📝 Populating Excel sheets...")
        # Projects sheet
        projects_ws = wb.active
        projects_ws.title = "Projects"
        
        # Add headers with styling
        project_headers = [
            "Project ID", "Description", "Total Contract Amount",
            "Status", "Last Modified", "Date Fetched"
        ]
        projects_ws.append(project_headers)
        
        for cell in projects_ws[1]:
            cell.font = Font(bold=True)
        
        # Add project data
        project_count = 0
        for project in data:
            projects_ws.append([
                project.get("ID"),
                project.get("Description"),
                project.get("TotalContractAmount", 0),
                project.get("Status"),
                project.get("LastModifiedDate"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])
            project_count += 1
        
        # Phases sheet
        phases_ws = wb.create_sheet("Phases")
        
        # Add headers with styling
        phase_headers = [
            "Project ID", "Phase ID", "Description",
            "Phase Contract Amount", "Last Modified", "Date Fetched"
        ]
        phases_ws.append(phase_headers)
        
        for cell in phases_ws[1]:
            cell.font = Font(bold=True)
        
        # Add phase data
        phase_count = 0
        for project in data:
            for phase in project.get("Phases", []):
                phases_ws.append([
                    project.get("ID"),
                    phase.get("ID"),
                    phase.get("Description"),
                    phase.get("TotalContractAmount", 0),
                    phase.get("LastModifiedDate"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])
                phase_count += 1
        
        print(f"✅ Excel sheets populated successfully with {project_count} projects and {phase_count} phases")
        
    except Exception as e:
        print(f"\n❌ Error populating Excel sheets: {str(e)}")
        raise

# ========================
# MAIN EXECUTION FLOW
# ========================
def main():
    try:
        print("\n🚀 Starting Ajera API integration...")

        # ========================
        # STEP 1: FILE BACKUP MANAGEMENT
        # ========================
        print("\n📁 Checking existing files...")

        if os.path.exists(FILE_NAMES["NEW"]):
            print(f"✅ Found existing {FILE_NAMES['NEW']}")

            if os.path.exists(FILE_NAMES["PREVIOUS"]):
                print(f"🗑️ Removing old {FILE_NAMES['PREVIOUS']} to update backup...")
                os.remove(FILE_NAMES["PREVIOUS"])  # Remove previous backup

            print(f"🔄 Backing up {FILE_NAMES['NEW']} as {FILE_NAMES['PREVIOUS']}...")
            os.rename(FILE_NAMES["NEW"], FILE_NAMES["PREVIOUS"])  # Backup new_data.xlsx
            print("📁 Backup completed successfully!")

        else:
            print(f"⚠️ No existing {FILE_NAMES['NEW']} found. A fresh data pull will be performed.")

        # ========================
        # STEP 2: FETCH LATEST DATA
        # ========================
        session_token = create_session()
        projects = fetch_projects(session_token)
        processed_data = process_batches(session_token, projects)

        # Verify that data was fetched successfully
        print("\n🔍 Verifying processed data...")
        if not processed_data:
            raise Exception("❌ No data received from API. New file will NOT be created.")

        # ========================
        # STEP 3: SAVE NEW DATA TO EXCEL
        # ========================
        print("\n💾 Saving new data to Excel...")

        wb = Workbook()
        _populate_sheets(wb, processed_data)

        # Ensure we don’t save an empty file
        if wb.active.max_row <= 1:  # Only header row
            raise Exception("❌ No data to save - would create an empty Excel file.")

        wb.save(FILE_NAMES["NEW"])
        print(f"✅ Successfully saved new data to {FILE_NAMES['NEW']} with {wb.active.max_row - 1} rows.")

        # ========================
        # STEP 4: COMPARE NEW & OLD DATA
        # ========================
        print("\n🔍 Analyzing data changes...")
        comparator = DataComparator()
        comparator.load_data()
        changes = comparator.detect_changes()

        # ========================
        # STEP 5: GENERATE REPORTS
        # ========================
        print("\n📋 Generating reports...")
        reporter = ReportGenerator(changes)
        backlog_file = reporter.create_backlog()
        reporter.generate_table_view()

        if backlog_file:
            print(f"\n📊 Change report saved: {backlog_file}")

        print("\n✨ Process completed successfully!")

    except Exception as e:
        print(f"\n🚨 Critical error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
