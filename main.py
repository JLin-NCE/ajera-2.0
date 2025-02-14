import requests
import json
import os
import sys
from datetime import datetime
import time
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill

class ProjectFetcher:
    def __init__(self):
        self.API_URL = "https://ajera.com/V006275/AjeraAPI.ashx?ew0KICAiQ2xpZW50SUQiOiA2Mjc1LA0KICAiRGF0YWJhc2VJRCI6IDEzMDI5LA0KICAiSXNTYW1wbGVEYXRhIjogZmFsc2UNCn0%3d"
        self.CREDENTIALS = {
            "USERNAME": "powerquerynv",
            "PASSWORD": "Testt1m3!!123"
        }
        self.FILES = {
            "NEW": "data_10.xlsx",
            "PREVIOUS": "previous_10.xlsx",
            "BACKLOG": "changes_10.xlsx"
        }
        self.PROJECT_LIMIT = 10
        self.BATCH_SIZE = 5
        self.session_token = None
        self.retry_count = 3
        self.retry_delay = 1  # seconds

    def create_session(self):
        """Create API session and get token"""
        try:
            print("\n🔑 Creating API session...")
            payload = {
                "Method": "CreateAPISession",
                "Username": self.CREDENTIALS["USERNAME"],
                "Password": self.CREDENTIALS["PASSWORD"],
                "APIVersion": 2
            }
            
            response = requests.post(self.API_URL, json=payload)
            response.raise_for_status()
            
            data = response.json()
            if "Errors" in data and data["Errors"]:
                raise Exception(f"API Error: {data['Errors'][0].get('ErrorMessage')}")
            
            self.session_token = data["Content"]["SessionToken"]
            print("✅ Session created successfully")
            return self.session_token
            
        except Exception as e:
            print(f"❌ Session creation failed: {str(e)}")
            raise

    def fetch_projects(self):
        """Fetch limited number of active projects"""
        try:
            print(f"\n📋 Fetching {self.PROJECT_LIMIT} projects...")
            payload = {
                "Method": "ListProjects",
                "SessionToken": self.session_token,
                "MethodArguments": {
                    "FilterByStatus": ["Active"]
                }
            }
            
            response = requests.post(self.API_URL, json=payload)
            response.raise_for_status()
            
            data = response.json()
            if "Errors" in data and data["Errors"]:
                raise Exception(f"API Error: {data['Errors'][0].get('ErrorMessage')}")
            
            projects = data["Content"].get("Projects", [])[:self.PROJECT_LIMIT]
            print(f"✅ Retrieved {len(projects)} projects")
            return projects
            
        except Exception as e:
            print(f"❌ Project fetch failed: {str(e)}")
            raise

    def fetch_project_details(self, project_key, attempt=1):
        """Fetch details for a single project with retry logic"""
        try:
            payload = {
                "Method": "GetProjects",
                "SessionToken": self.session_token,
                "MethodArguments": {
                    "RequestedProjects": [project_key]
                }
            }
            
            response = requests.post(self.API_URL, json=payload)
            response.raise_for_status()
            
            data = response.json()
            if "Errors" in data and data["Errors"]:
                raise Exception(f"API Error: {data['Errors'][0].get('ErrorMessage')}")
            
            project = data["Content"].get("Projects", [])[0]
            # Ensure phases are included
            project["Phases"] = data["Content"].get("Phases", [])
            return project
            
        except Exception as e:
            if attempt < self.retry_count:
                print(f"⚠️ Retry {attempt} for project {project_key}")
                time.sleep(self.retry_delay * attempt)
                return self.fetch_project_details(project_key, attempt + 1)
            print(f"❌ Failed to fetch details for project {project_key}: {str(e)}")
            return None

    def process_projects_in_batches(self, projects):
        """Process projects in small batches"""
        try:
            if not projects:
                return []

            total_projects = len(projects)
            processed_data = []
            
            print(f"\n🔄 Processing {total_projects} projects in batches of {self.BATCH_SIZE}...")
            
            for i in range(0, total_projects, self.BATCH_SIZE):
                batch = projects[i:i + self.BATCH_SIZE]
                print(f"\n📦 Processing batch {(i//self.BATCH_SIZE) + 1} ({len(batch)} projects)")
                
                for project in batch:
                    project_key = project["ProjectKey"]
                    print(f"  Processing project {project_key}...")
                    
                    if project_details := self.fetch_project_details(project_key):
                        processed_data.append(project_details)
                    
                # Small delay between batches
                if i + self.BATCH_SIZE < total_projects:
                    time.sleep(0.5)
            
            print(f"\n✅ Successfully processed {len(processed_data)} projects")
            return processed_data
            
        except Exception as e:
            print(f"❌ Batch processing failed: {str(e)}")
            raise

    def save_to_excel(self, data):
        """Save project and phase data to Excel"""
        try:
            print("\n💾 Saving data to Excel...")
            wb = Workbook()
            
            # Projects sheet
            ws_projects = wb.active
            ws_projects.title = "Projects"
            
            headers = [
                "Project ID", 
                "Description",
                "Total Contract Amount",
                "Status",
                "Last Modified"
            ]
            ws_projects.append(headers)
            
            for cell in ws_projects[1]:
                cell.font = Font(bold=True)
            
            project_count = 0
            for project in data:
                ws_projects.append([
                    project.get("ID"),
                    project.get("Description"),
                    project.get("TotalContractAmount", 0),
                    project.get("Status"),
                    project.get("LastModifiedDate")
                ])
                project_count += 1
            
            # Phases sheet
            ws_phases = wb.create_sheet("Phases")
            headers = [
                "Project ID",
                "Phase ID",
                "Description",
                "Total Contract Amount",
                "Status",
                "Last Modified"
            ]
            ws_phases.append(headers)
            
            for cell in ws_phases[1]:
                cell.font = Font(bold=True)
            
            phase_count = 0
            for project in data:
                project_id = project.get("ID")
                for phase in project.get("Phases", []):
                    ws_phases.append([
                        project_id,
                        phase.get("ID"),
                        phase.get("Description"),
                        phase.get("TotalContractAmount", 0),
                        phase.get("Status"),
                        phase.get("LastModifiedDate")
                    ])
                    phase_count += 1
            
            # Backup existing file if present
            if os.path.exists(self.FILES["NEW"]):
                if os.path.exists(self.FILES["PREVIOUS"]):
                    os.remove(self.FILES["PREVIOUS"])
                os.rename(self.FILES["NEW"], self.FILES["PREVIOUS"])
            
            # Save new file
            wb.save(self.FILES["NEW"])
            print(f"✅ Saved {project_count} projects and {phase_count} phases to {self.FILES['NEW']}")
            
            return project_count, phase_count
            
        except Exception as e:
            print(f"❌ Save failed: {str(e)}")
            raise

    def compare_data(self):
        """Compare current and previous data"""
        try:
            if not os.path.exists(self.FILES["PREVIOUS"]):
                print("\nℹ️ No previous data to compare")
                return
            
            print("\n🔍 Comparing current and previous data...")
            old_wb = load_workbook(self.FILES["PREVIOUS"])
            new_wb = load_workbook(self.FILES["NEW"])
            
            changes = []
            
            # Compare projects
            old_projects = {row[0].value: row[2].value for row in old_wb["Projects"].iter_rows(min_row=2) if row[0].value}
            new_projects = {row[0].value: row[2].value for row in new_wb["Projects"].iter_rows(min_row=2) if row[0].value}
            
            for project_id in set(old_projects) | set(new_projects):
                old_amount = old_projects.get(project_id, 0) or 0
                new_amount = new_projects.get(project_id, 0) or 0
                
                if old_amount != new_amount:
                    changes.append({
                        "Type": "Project",
                        "ID": project_id,
                        "Old Amount": old_amount,
                        "New Amount": new_amount,
                        "Change": new_amount - old_amount
                    })
            
            # Compare phases
            old_phases = {(row[0].value, row[1].value): row[3].value 
                         for row in old_wb["Phases"].iter_rows(min_row=2) 
                         if row[0].value and row[1].value}
            new_phases = {(row[0].value, row[1].value): row[3].value 
                         for row in new_wb["Phases"].iter_rows(min_row=2) 
                         if row[0].value and row[1].value}
            
            for phase_key in set(old_phases) | set(new_phases):
                old_amount = old_phases.get(phase_key, 0) or 0
                new_amount = new_phases.get(phase_key, 0) or 0
                
                if old_amount != new_amount:
                    changes.append({
                        "Type": "Phase",
                        "Project ID": phase_key[0],
                        "Phase ID": phase_key[1],
                        "Old Amount": old_amount,
                        "New Amount": new_amount,
                        "Change": new_amount - old_amount
                    })
            
            if changes:
                self.save_changes_report(changes)
            
            print(f"✅ Found {len(changes)} changes")
            return changes
            
        except Exception as e:
            print(f"❌ Comparison failed: {str(e)}")
            raise

    def save_changes_report(self, changes):
        """Save changes to Excel report"""
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Changes"
            
            headers = ["Type", "Project ID", "Phase ID", "Old Amount", "New Amount", "Change"]
            ws.append(headers)
            
            for cell in ws[1]:
                cell.font = Font(bold=True)
            
            for change in changes:
                row = [
                    change["Type"],
                    change.get("Project ID", change.get("ID")),
                    change.get("Phase ID", "N/A"),
                    change["Old Amount"],
                    change["New Amount"],
                    change["Change"]
                ]
                ws.append(row)
                
                # Color code changes
                change_cell = ws.cell(row=ws.max_row, column=6)
                if change["Change"] < 0:
                    change_cell.fill = PatternFill(
                        start_color="FFCDD2",
                        end_color="FFCDD2",
                        fill_type="solid"
                    )
                else:
                    change_cell.fill = PatternFill(
                        start_color="C8E6C9",
                        end_color="C8E6C9",
                        fill_type="solid"
                    )
            
            wb.save(self.FILES["BACKLOG"])
            print(f"✅ Changes report saved to {self.FILES['BACKLOG']}")
            
        except Exception as e:
            print(f"❌ Report generation failed: {str(e)}")
            raise

def main():
    try:
        print("\n🚀 Starting Project Data Collection (Limited to 10 Projects)")
        
        fetcher = ProjectFetcher()
        
        # Create session
        fetcher.create_session()
        
        # Fetch and process projects
        projects = fetcher.fetch_projects()
        processed_data = fetcher.process_projects_in_batches(projects)
        
        # Save to Excel
        project_count, phase_count = fetcher.save_to_excel(processed_data)
        
        # Compare with previous data
        changes = fetcher.compare_data()
        
        print("\n✨ Process completed successfully!")
        print(f"📊 Summary:")
        print(f"  - Projects processed: {project_count}")
        print(f"  - Phases processed: {phase_count}")
        print(f"  - Changes detected: {len(changes) if changes else 0}")
        
    except Exception as e:
        print(f"\n🚨 Critical error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
