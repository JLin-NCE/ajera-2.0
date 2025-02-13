import requests
import concurrent.futures
import time
from datetime import datetime
from openpyxl import Workbook

AJERA_BASE_URL = "https://ajera.com/V006275/AjeraAPI.ashx?ew0KICAiQ2xpZW50SUQiOiA2Mjc1LA0KICAiRGF0YWJhc2VJRCI6IDEzMDI5LA0KICAiSXNTYW1wbGVEYXRhIjogZmFsc2UNCn0%3d"
USERNAME = "powerquerynv"
PASSWORD = "Testt1m3!!123"
BATCH_SIZE = 10  # Number of projects per batch
MAX_THREADS = 5  # Maximum concurrent API calls

def create_api_session():
    """Create and return an API session token"""
    payload = {
        "Method": "CreateAPISession",
        "Username": USERNAME,
        "Password": PASSWORD,
        "APIVersion": 2,
        "UseSessionCookie": "false"
    }
    response = requests.post(AJERA_BASE_URL, json=payload)
    response.raise_for_status()
    
    if not (session_token := response.json().get("Content", {}).get("SessionToken")):
        raise ValueError("No session token found in API response.")
    return session_token

def list_active_projects(session_token):
    """Retrieve list of active projects"""
    payload = {
        "Method": "ListProjects",
        "SessionToken": session_token,
        "MethodArguments": {"FilterByStatus": ["Active"]}
    }
    response = requests.post(AJERA_BASE_URL, json=payload)
    response.raise_for_status()
    return response.json().get("Content", {}).get("Projects", [])

def get_project_details(session_token, project_keys):
    """Get full project details for multiple projects"""
    payload = {
        "Method": "GetProjects",
        "SessionToken": session_token,
        "MethodArguments": {"RequestedProjects": project_keys}
    }
    response = requests.post(AJERA_BASE_URL, json=payload)
    response.raise_for_status()
    
    content = response.json().get("Content", {})
    return [
        {"Project": proj, "Phases": content.get("Phases", [])}
        for proj in content.get("Projects", [])
    ]

def process_project_batch(session_token, project_keys, batch_number, total_batches):
    """Process a batch of projects and return results"""
    try:
        print(f"🚀 Processing batch {batch_number}/{total_batches} ({len(project_keys)} projects)...")
        start_time = time.time()
        
        results = get_project_details(session_token, project_keys)
        
        elapsed_time = time.time() - start_time
        print(f"✅ Completed batch {batch_number}/{total_batches} in {elapsed_time:.2f} sec")
        return results
    except Exception as e:
        print(f"❌ Error processing batch {batch_number}: {e}")
        return []

def write_to_excel(output_filename, combined_data):
    """Export data to Excel with formatted columns"""
    wb = Workbook()
    
    ws_projects = wb.active
    ws_projects.title = "Projects"
    ws_projects.append([
        "Project ID", "Description", "Total Contract Amount",
        "Status", "Last Modified", "Date Fetched"
    ])
    
    ws_phases = wb.create_sheet("Phases")
    ws_phases.append([
        "Project ID", "Phase ID", "Description", 
        "Phase Contract Amount", "Last Modified", "Date Fetched"
    ])

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for record in combined_data:
        project = record["Project"]
        phases = record["Phases"]

        ws_projects.append([
            project.get("ID"),
            project.get("Description"),
            project.get("TotalContractAmount", 0),
            project.get("Status"),
            project.get("LastModifiedDate"),
            current_timestamp
        ])

        for phase in phases:
            ws_phases.append([
                project.get("ID"),
                phase.get("ID"),
                phase.get("Description"),
                phase.get("TotalContractAmount", 0),
                phase.get("LastModifiedDate"),
                current_timestamp
            ])

    wb.save(output_filename)

if __name__ == "__main__":
    try:
        # Initialize API session
        session_token = create_api_session()
        print("✅ API session established")
        
        # Fetch active projects
        active_projects = list_active_projects(session_token)
        total_projects = len(active_projects)
        print(f"📊 Found {total_projects} active projects")
        
        # Group projects into batches
        project_batches = [
            [proj["ProjectKey"] for proj in active_projects[i:i + BATCH_SIZE]]
            for i in range(0, total_projects, BATCH_SIZE)
        ]
        total_batches = len(project_batches)

        # Process batches in parallel
        combined_results = []
        success_count = 0
        error_count = 0

        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_batch = {
                executor.submit(process_project_batch, session_token, batch, batch_idx + 1, total_batches): batch
                for batch_idx, batch in enumerate(project_batches)
            }
            
            for idx, future in enumerate(concurrent.futures.as_completed(future_to_batch), 1):
                try:
                    results = future.result()
                    combined_results.extend(results)
                    success_count += len(results)

                    # Estimated time remaining
                    elapsed_time = time.time() - start_time
                    avg_time_per_batch = elapsed_time / idx
                    remaining_time = avg_time_per_batch * (total_batches - idx)
                    print(f"📈 Progress: {idx}/{total_batches} batches completed - Estimated time left: {remaining_time:.2f} sec")

                except Exception as e:
                    error_count += len(future_to_batch[future])
                    print(f"❌ Error processing batch {idx}: {str(e)}")

        # Export to Excel
        print("💾 Saving results to Excel...")
        write_to_excel("ajera_projects_export.xlsx", combined_results)
        
        # Final report
        total_time = time.time() - start_time
        print("\n📝 Processing Report:")
        print(f"- Total projects: {total_projects}")
        print(f"- Successfully processed: {success_count}")
        print(f"- Failed/Skipped: {error_count}")
        print(f"✅ Output saved to: ajera_projects_export.xlsx")
        print(f"⏳ Total processing time: {total_time:.2f} sec")
        
    except Exception as e:
        print(f"🔥 Critical error: {str(e)}")
    finally:
        print("🏁 Process completed")
