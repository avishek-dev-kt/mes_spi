import os
import re
import json
import shutil
import threading
import time
import logging
import requests
import schedule
from datetime import datetime
from collections import defaultdict
import xml.etree.ElementTree as ET


# Define the password for reset (retrieve from environment variable for security)
RESET_PASSWORD = os.getenv('RESET_PASSWORD', 'Kayneskt01')  # Changes based on plant - WIN - set RESET_PASSWORD=anypassword

# Configure logging
log_file_path = os.path.join(os.path.dirname(__file__), 'SPI_app.log')
logging.basicConfig(filename=log_file_path, 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Get the current directory of the app
current_directory = os.path.dirname(os.path.abspath(__file__))

# Defining folder names
folders = {
    "JSON_Data_Folder": os.path.join(current_directory, "JSON_Data_Folder"),
    "Scan_Folder": os.path.join(current_directory, "Scan_Folder"),
    "Backup_Folder": os.path.join(current_directory, "Backup_Folder"),
    "Logs_Folder": os.path.join(current_directory, "Logs_Folder"),
    "Done_Folder": os.path.join(current_directory, "Done_Folder"),  # Added cmd.py_10
}

# Defining log files in Logs_Folder
log_folders = {
    "Copy_Logs": os.path.join(folders["Logs_Folder"], "Copy_Logs"),
    "Backup_Logs": os.path.join(folders["Logs_Folder"], "Backup_Logs"),
    "Parser_Logs": os.path.join(folders["Logs_Folder"], "Parser_Logs"),
    "Skipped_Logs": os.path.join(folders["Logs_Folder"], "Skipped_Logs"),
}

# Create folders. error handling added in cmd_3.py
def create_folders():
    try:
        for folder in folders.values():
            os.makedirs(folder, exist_ok=True)
        for log_folder in log_folders.values():
            os.makedirs(log_folder, exist_ok=True)
        logging.info("Folders created successfully.")
    except Exception as e:
        logging.error(f"Error creating folders: {e}")
        print(f"Error creating folders: {e}")

# Config file creation. Changed to JSON file in cmd_5.py
def write_folder_paths_to_file(api_key, api_secret, erp_url, machine_data_folder, json_folder1, json_folder2):
    try:
        config_data = {
            "API_Key": api_key,
            "API_Secret": api_secret,
            "ERP_URL": erp_url,
            "Machine_Data_Folder": machine_data_folder,
            "LM_JSON_FOLDER": json_folder1,
            "LM_BKP_JSON_FOLDER": json_folder2,
            "Folders": folders,
            "Log_Folders": log_folders
        }

        config_file = os.path.join(current_directory, "config.json")
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=4)
        logging.info("Configuration file created.")
    except Exception as e:
        logging.error(f"Failed to write configuration file: {e}")
        print(f"Failed to write configuration file: {e}")

# Function to load inputs from the config file
def load_inputs_from_file():
    try:
        config_file = os.path.join(current_directory, "config.json")
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                logging.info("Inputs loaded from Config file.")
                return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load configuration file: {e}")
    return None

# Function to get inputs from the user (if not available in the config file)
def get_inputs():
    print("Please provide the following inputs:")
    api_key = input("API Key (api.py): ")
    api_secret = input("API Secret (api.py): ")
    erp_url = input("ERP URL (api.py): ")
    machine_data_folder = input("Machine Target DATA folder Path (mvf.py): ")
    json_folder1 = input("Enter Laser Marking Data Folder: ")
    json_folder2 = input("Enter Laser Marking Backup Data Folder: ")
    logging.info("User Input Registered.")
    return api_key, api_secret, erp_url, machine_data_folder, json_folder1, json_folder2


# File Mover Functionality with error handling
def copy_new_files(src_folder, dest_folder, log_folder_path):
    try:
        # Gather all copied file names from all logs in the Copy_Logs folder
        copied_files = []
        for log_file_name in os.listdir(log_folder_path):
            log_file_path = os.path.join(log_folder_path, log_file_name)
            if os.path.isfile(log_file_path) and log_file_name.startswith("copy_logs_") and log_file_name.endswith(".log"):
                with open(log_file_path, 'r') as log_file:
                    copied_files.extend(log_file.read().splitlines())
        
        # Get the list of files in the source folder
        src_files = os.listdir(src_folder)
        
        # Copy files that haven't been logged
        for file_name in src_files:
            src_file_path = os.path.join(src_folder, file_name)
            dest_file_path = os.path.join(dest_folder, file_name)
            
            # Only copy if file_name is not in any of the logs
            if file_name not in copied_files:
                shutil.copy2(src_file_path, dest_file_path)
                
                # Log the newly copied file in the current date log file
                current_log_file = os.path.join(log_folder_path, f"copy_logs_{datetime.now().strftime('%Y-%m-%d')}.log")
                with open(current_log_file, 'a') as log:
                    log.write(f"{file_name}\n")
                
                print(f"Copied {file_name} to {dest_folder}")
                logging.info(f"Copied {file_name} to {dest_folder}")
                
    except Exception as e:
        logging.error(f"Error during file copying: {e}")
        print(f"Error during file copying: {e}")

# Function to move only successfully parsed files to the backup folder, considering skipped files
# If file exists, add a number at the end and copy it - CHANGED
def move_files_to_backup(src_folder, backup_folder, backup_log_file, successfully_parsed_files, skipped_files):
    try:
        # Check for files in the source folder
        src_files = os.listdir(src_folder)
        if not src_files:
            logging.info(f"No files found in {src_folder} to move.")
            return

        for file_name in src_files:
            # Only move files that were successfully parsed and not skipped
            if file_name in successfully_parsed_files and file_name not in skipped_files:
                src_file_path = os.path.join(src_folder, file_name)
                backup_file_path = os.path.join(backup_folder, file_name)

                try:
                    # Move the file
                    shutil.move(src_file_path, backup_file_path)

                    # Log the move
                    with open(backup_log_file, 'a') as log:
                        log.write(f"{file_name} moved from {src_folder} to {backup_folder} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    print(f"Moved {file_name} to {backup_folder}")
                    logging.info(f"Backed up {file_name} to {backup_folder}")
                except Exception as e:
                    logging.error(f"Error moving {file_name}: {e}")
            elif file_name in skipped_files:
                logging.info(f"Skipped file {file_name} as it is in the skipped log.")
                print(f"Skipped file {file_name} as it is in the skipped log.")
    except Exception as e:
        logging.error(f"Error during backup process: {e}")
        print(f"Error during backup process: {e}")


# Function to get the list of skipped files from the skipped log
def get_skipped_files(log_file_path):
    skipped_files = []
    try:
        # Check if the skipped files log exists
        # print("Skipped_parsed_cheking")
        if not os.path.exists(log_file_path):
            logging.error(f"Skipped log file {log_file_path} does not exist.")
            return skipped_files
        
        with open(log_file_path, 'r') as log_file:
            for line in log_file:
                # Check if the line indicates a skipped file
                if "Skipped" in line:
                    # Extract the XML filename from the log entry (last part of the path)
                    parts = line.split(' ')
                    if len(parts) > 1:
                        file_name = parts[1].strip()  # The filename will be the second part of the log entry
                        file_name_only = file_name.split('/')[-1]  # Get the file name without path
                        skipped_files.append(file_name_only)
    except Exception as e:
        logging.error(f"Error reading skipped log file {log_file_path}: {e}")
    return skipped_files


# Function to read the parser log and extract successfully parsed filenames
def get_successfully_parsed_files(log_file_path):
    # print("Succefully_parsed_cheking")
    parsed_files = []
    try:
        # Check if the log file exists before attempting to read it
        if not os.path.exists(log_file_path):
            logging.error(f"Log file {log_file_path} does not exist.")
            return parsed_files
        
        with open(log_file_path, 'r') as log_file:
            for line in log_file:
                # Check if the line indicates a successful parse
                if "parsed successfully" in line:
                    # Extract the XML filename from the log entry (first part of the path)
                    parts = line.split(' ')
                    if len(parts) > 0:
                        file_name = parts[0].strip()  # The filename will be the first part of the log entry
                        file_name_only = file_name.split('/')[-1]  # Get the file name without path
                        parsed_files.append(file_name_only)
    except Exception as e:
        logging.error(f"Error reading log file {log_file_path}: {e}")
    return parsed_files

#---------------------------------------------------------------------Parser----------

def check_panel_barcode_in_json(serial_no, json_folder1, json_folder2, skipped_log_folder):
    print("Entered Search")
    
    json_folders = [json_folder1, json_folder2]
    
    for folder in json_folders:
        for json_file in os.listdir(folder):
            if json_file.endswith(".json"):
                json_file_path = os.path.join(folder, json_file)
                print("Checking file:", json_file_path)
                
                try:
                    with open(json_file_path, 'r') as f:
                        data = json.load(f)
                        # Check if the serial_no exists in the JSON file's laser_marking list
                        for entry in data.get("laser_marking", []):
                            if entry.get("serial_no") == serial_no:
                                print("Match found in file:", json_file_path)
                                
                                # Remove the CSV file from skipped logs
                                remove_from_skipped_logs(skipped_log_folder, serial_no)
                                
                                return data.get("model_id"), True
                except Exception as e:
                    logging.error(f"Error reading JSON file {json_file_path}: {e}")
                    print("Error reading JSON file:", json_file_path)
    
    return None, False

def remove_from_skipped_logs(skipped_log_folder, serial_no):
    try:
        # Iterate through all log files in the folder
        for log_file in os.listdir(skipped_log_folder):
            if log_file.endswith(".log"):  # Assuming log files have .log extension
                log_file_path = os.path.join(skipped_log_folder, log_file)
                
                # Read all lines from the log file
                with open(log_file_path, 'r') as file:
                    lines = file.readlines()
                
                # Filter out lines containing the serial_no
                updated_lines = [line for line in lines if serial_no not in line]
                
                # Write the updated lines back to the log file
                with open(log_file_path, 'w') as file:
                    file.writelines(updated_lines)
                
                print(f"Removed {serial_no} from log file: {log_file_path}")
    except Exception as e:
        logging.error(f"Error updating skipped logs in folder {skipped_log_folder}: {e}")
        print("Error updating skipped logs:", e)


# Updated parse_xml_to_json function to capture and pass model_id
def parse_xml_to_json(xml_file, json_file, log_file, model_id, json_folder1, json_folder2, skipped_log_file, skipped_log_folder):
    try:
        print("Parser Begins")
        tree = ET.parse(xml_file)
        root = tree.getroot()
        panel_barcode = root.find('.//Panels/Panel/PanelBarcode').text if root.find('.//Panels/Panel/PanelBarcode') is not None else None
        print(panel_barcode)
        # process_ng_log(xml_file, panel_barcode, NG_log_file)

        if not panel_barcode:
            print("Not found in XML")
            logging.error(f"panel_barcode not found in {xml_file}")
            return

        # Get model_id dynamically from JSON file matching panel_barcode
        model_id, found = check_panel_barcode_in_json(panel_barcode, json_folder1, json_folder2, skipped_log_folder)
        if found:
            logging.info(f"Found panel_barcode {panel_barcode} in JSON files with model_id {model_id}. Proceeding with parsing.")
            print("Found in LM")
            existing_data, last_pd_no = load_existing_json(json_file)

            # Extract model_id and other data for parsing as before
            soldier_paste_inspection_data = existing_data.get("soldier_paste_inspection", [])

            # Initialize dictionary to hold parsed data
            parsed_data = {}

            # Extract required fields from <Board>
            board = root.find('Board')
            if board is not None:
                pcb_name = board.find('PcbName').text if board.find('PcbName') is not None else None
                parsed_data['pcb_name'] = pcb_name
                parsed_data['inspectstart_datentime'] = board.find('InspectStartDateTime').text if board.find('InspectStartDateTime') is not None else None
                parsed_data['inspectend_datentime'] = board.find('InspectEndDateTime').text if board.find('InspectEndDateTime') is not None else None
                parsed_data['defectend_datentime'] = board.find('DefectEndDateTime').text if board.find('DefectEndDateTime') is not None else None
                parsed_data['defectresult_name'] = board.find('DefectResultName').text if board.find('DefectResultName') is not None else None
                
                # Determine SIDE from PcbName
                parsed_data['side'] = determine_side(pcb_name)
            
            # Extract required fields from <Panels>
            panels = root.find('Panels')
            if panels is not None:
                panel = panels.find('Panel')
                if panel is not None:
                    parsed_data['serial_no'] = panel.find('PanelBarcode').text if panel.find('PanelBarcode') is not None else None
                    parsed_data['panel_result_name'] = panel.find('PanelResultName').text if panel.find('PanelResultName') is not None else None
                    parsed_data['panel_defect_result_name'] = panel.find('PanelDefectResultName').text if panel.find('PanelDefectResultName') is not None else None
                    last_pd_no = generate_pd_no(last_pd_no)  # Generate the next PD number
                    parsed_data['pd_no'] = last_pd_no  # Assign the generated PD number

            # Process Pads, only capturing relevant pad data if conditions are met
            pads = root.find('Pads')
            if pads is not None:
                for pad in pads.findall('Pad'):
                    result_name = pad.find('ResultName').text if pad.find('ResultName') is not None else "GOOD"
                    defect_result_name = pad.find('DefectResultName').text if pad.find('DefectResultName') is not None else "GOOD"
                    
                    # Only include pads where ResultName or DefectResultName is not "GOOD"
                    if result_name != "GOOD" or defect_result_name != "GOOD":
                        ng_count = ng_count_log(xml_file) # Get ng count
                        # Add the necessary pad information directly to parsed_data (no pads array)
                        parsed_data.update({
                            'pad_id': pad.get('ID'),
                            'component_id': pad.find('ComponentId').text if pad.find('ComponentId') is not None else None,
                            'result_name': result_name,
                            'defect_result_name': defect_result_name,
                            'ng': ng_count
                        })
                        # Break after first relevant pad found as per your example structure
                        break

            # Add the new parsed data to the 'soldier_paste_inspection' list
            soldier_paste_inspection_data.append(parsed_data)

            # Update existing data with new entries and model_id
            existing_data['soldier_paste_inspection'] = soldier_paste_inspection_data
            existing_data['model_id'] = model_id  # Ensure model_id is included

            # Write the updated data to JSON
            with open(json_file, 'w') as json_output:
                json.dump(existing_data, json_output, indent=4)

            # Log success
            logging.info(f"Data from {xml_file} has been parsed and saved to {json_file}")
            print(f"Data from {xml_file} has been parsed and saved to {json_file}")

            # Log parsed XML file
            log_parsed_file(log_file, xml_file)

        else:
            logging.info(f"panel_barcode {panel_barcode} not found in any JSON files. Skipping this file.")
            log_skipped_file(skipped_log_file, xml_file)

    except Exception as e:
        logging.error(f"Error parsing XML file {xml_file}: {e}")
        print(f"Error parsing XML file {xml_file}: {e}")

# Helper function to load existing JSON data - Parser
def load_existing_json(json_file):
    # Check if the JSON file exists and is not empty
    if not os.path.exists(json_file) or os.path.getsize(json_file) == 0:
        print(f"File {json_file} is empty or doesn't exist. Creating a new file with empty data.")
        # Initialize empty structure for new data
        data = {"model_id": "", "soldier_paste_inspection": []}
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=4)
        return data, "PD0000"  # Return initialized data and default PD number

    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Ensure data is a dictionary and has the structure we expect
        if not isinstance(data, dict):
            data = {"model_id": "", "soldier_paste_inspection": []}
        
        # Access the 'soldier_paste_inspection' list from the loaded data
        soldier_paste_inspection_data = data.get('soldier_paste_inspection', [])
        
        if soldier_paste_inspection_data:
            last_pd_no = soldier_paste_inspection_data[-1].get('pd_no', "PD0000")
        else:
            last_pd_no = "PD0000"  # Default value if there is no data in soldier_paste_inspection
        
        return data, last_pd_no
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file {json_file}. Returning empty data.")
        return {"model_id": "", "soldier_paste_inspection": []}, "PD0000"
    except Exception as e:
        logging.error(f"Unexpected error while reading {json_file}: {e}")
        return {"model_id": "", "soldier_paste_inspection": []}, "PD0000"

# Helper function to generate a new PD number
def generate_pd_no(last_pd_no):
    # Increment last pd_no to generate a new one
    last_number = int(last_pd_no[2:])  # Strip off the 'PD' prefix
    new_number = last_number + 1
    return f"PD{new_number:04d}"

# Helper function to log parsed XML files
def log_parsed_file(log_file, xml_file):
    with open(log_file, 'a') as log:
        log.write(f"Parsed {xml_file}\n")

# Helper function to log skipped files
def log_skipped_file(skipped_log_file, xml_file):
    with open(skipped_log_file, 'a') as skipped_log:
        skipped_log.write(f"Skipped {xml_file}\n")

# Count NG value for Board
def ng_count_log(xml_file):
    try:
        # Dynamically get the folder path for "Copy_Logs"
        copy_log_folder = os.path.dirname(get_log_file_path("Copy_Logs", datetime.now().strftime('%Y-%m-%d')))
        
        # Extract serial number from xml_file
        filename = os.path.basename(xml_file)
        serial_no = filename.split('_')[0]  # Serial number is the part before the first underscore

        # Initialize count for the serial number
        serial_count = 0
        
        # Define the file pattern for log files (e.g., copy_logs_YYYY-MM-DD.log)
        log_file_pattern = r"^copy_logs_\d{4}-\d{2}-\d{2}\.log$"

        # Loop over each file in the directory
        for log_file_name in os.listdir(copy_log_folder):
            # Only process files that match the log file pattern
            if re.match(log_file_pattern, log_file_name):
                log_file_path = os.path.join(copy_log_folder, log_file_name)
                
                # Read the file and count occurrences of the serial number
                with open(log_file_path, 'r') as log_file:
                    log_lines = log_file.readlines()
                    serial_count += sum(1 for line in log_lines if serial_no in line)

        # Log and return the count of occurrences
        if serial_count > 0:
            logging.info(f"Serial number {serial_no} found {serial_count} times in copy log files.")
        else:
            logging.info(f"Serial number {serial_no} not found in any copy log file.")
        
        return serial_count

    except Exception as e:
        logging.error(f"Error processing copy log folder {copy_log_folder}: {e}")
        return 0

# Function to determine the side based on PcbName
def determine_side(pcb_name):
    if pcb_name.endswith('SS'):
        return "BOTTOM"
    elif pcb_name.endswith('CS'):
        return "TOP"
    return "UNKNOWN"  # Default case if no match

# Function to log parsed XML file 
def log_parsed_file(log_file, xml_file):
    with open(log_file, 'a') as log:
        log.write(f"{xml_file} parsed successfully\n")

#---------------------------------------------------------------API---------------------
# Function to handle API requests with logging and error handling - changes needed cmd_12.py
# Helper function to load existing JSON data - API
def load_existing_json_2(json_file):
    if os.path.exists(json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        soldier_paste_inspection_data = data.get('soldier_paste_inspection', [])
        
        if soldier_paste_inspection_data:
            last_pd_no = soldier_paste_inspection_data[-1].get('pd_no', "PD0000")
        else:
            last_pd_no = "PD0000"
        
        return data, last_pd_no  # Return full data instead of just soldier_paste_inspection list
    else:
        return None, "PD0000"
    
# Function to check if a parent record exists for the given model_id
def get_parent_record(model_id, api_key, api_secret, erp_url):
    headers = {
        "Authorization": f"token {api_key}:{api_secret}",
        "Content-Type": "application/json"
    }

    # Ensure proper URL encoding for the filter
    filters = json.dumps([["model_id", "=", model_id]])
    url = f"{erp_url}?filters={filters}"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        records = data.get("data", [])
        
        if records:
            parent_name = records[0].get("name")
            print(f"Found existing parent record: {parent_name}")
            return parent_name
        else:
            print(f"No parent record found for model_id {model_id}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve parent record: {e}")
        return None

# Function to check if ERP server is reachable
def is_erp_server_running(erp_url, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            # Sending a HEAD request to check if server is up
            response = requests.head(erp_url, timeout=10)
            response.raise_for_status()  # Will raise HTTPError for bad responses
            return True  # Server is up
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to ERP server: {e}")
            logging.error(f"Error connecting to ERP server: {e}")
            attempt += 1
            if attempt < retries:
                print(f"Retrying connection in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. ERP server is down.")
                return False

# Function to send data to ERPNext with API POST & PUT modes
def send_to_erpnext(data, api_key, api_secret, erp_url, retries=3, delay=15, timeout=10):
    logging.info("Triggered API functionality.")
    
    headers = {
        "Authorization": f"token {api_key}:{api_secret}",
        "Content-Type": "application/json"
    }
    
    model_id = data.get("model_id")
    soldier_paste_inspection = data.get("soldier_paste_inspection", [])

    if not model_id:
        print("No model_id found in JSON data.")
        return False

    # Check if ERP server is reachable before proceeding
    if not is_erp_server_running(erp_url, retries=3, delay=5):
        return False  # Exit if the server is not reachable

    # Fetch parent document based on model_id
    parent_name = get_parent_record(model_id, api_key, api_secret, erp_url)
    logging.info(f"Parent Name: {parent_name}")

    child_data = [{
        "pcb_name": record.get("pcb_name", ""),
        "inspectstart_datentime": record.get("inspectstart_datentime", ""),
        "inspectend_datentime": record.get("inspectend_datentime", ""),
        "defectend_datentime": record.get("defectend_datentime", ""),
        "defectresult_name": record.get("defectresult_name", ""),
        "side": record.get("side", ""),
        "serial_no": record.get("serial_no", ""),
        "panel_result_name": record.get("panel_result_name", ""),
        "panel_defect_result_name": record.get("panel_defect_result_name", ""),
        "pd_no": record.get("pd_no", ""),
        "ng": record.get("ng", ""),
        "pad_id": record.get("pad_id", ""),
        "component_id": record.get("component_id", ""),
        "result_name": record.get("result_name", ""),
        "defect_result_name": record.get("defect_result_name", "")
    } for record in soldier_paste_inspection]

    all_successful = True  # Flag to track overall success
    
    # Process each child record individually
    for record in child_data:
        attempt = 0
        success = False  # Track success for this record
        
        while attempt < retries:
            try:
                if parent_name:
                    # If parent exists, fetch existing child records and update
                    url = f"{erp_url}/{parent_name}"
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    
                    existing_data = response.json()
                    existing_soldier_paste_inspection = existing_data.get("data", {}).get("soldier_paste_inspection", [])

                    # Check if the serial_no exists in the existing data
                    existing_record = next((item for item in existing_soldier_paste_inspection if item["serial_no"] == record["serial_no"]), None)

                    if existing_record:
                        # Update the existing record with the new data
                        existing_record.update(record)
                        payload = {"soldier_paste_inspection": existing_soldier_paste_inspection}
                        response = requests.put(url, headers=headers, data=json.dumps(payload), timeout=timeout)
                    else:
                        # If no conflict, add the new record
                        existing_soldier_paste_inspection.append(record)
                        payload = {"soldier_paste_inspection": existing_soldier_paste_inspection}
                        response = requests.put(url, headers=headers, data=json.dumps(payload), timeout=timeout)

                else:
                    # If parent doesn't exist, create a new parent document (POST request)
                    url = erp_url
                    payload = {
                        "model_id": model_id,
                        "serial_no": record.get("serial_no", ""),
                        "soldier_paste_inspection": [record],  # Send just the current record
                        "docstatus": 0
                    }
                    response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)

                response.raise_for_status()

                if response.status_code in [200, 201]:
                    print(f"Successfully submitted record with serial_no {record['serial_no']}")
                    success = True
                    break  # Exit retry loop for this record

            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 409:
                    logging.warning(f"Conflict (409) for serial_no {record['serial_no']}. Retrying as PUT.")
                else:
                    logging.error(f"HTTP error for serial_no {record['serial_no']}: {err}")
                    
                attempt += 1
                if attempt < retries:
                    print(f"Retrying record {record['serial_no']} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"Max retries reached for serial_no {record['serial_no']}.")
                    break  # Stop trying this record after max retries
            except requests.exceptions.RequestException as e:
                logging.error(f"Request exception for serial_no {record['serial_no']}: {e}")
                attempt += 1
                if attempt < retries:
                    print(f"Retrying record {record['serial_no']} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"Max retries reached for serial_no {record['serial_no']}.")
                    break

        if not success:
            all_successful = False  # Mark overall success as False if any record fails

    return all_successful

#-----------------------------------------------------------------------------------------

def move_to_done_folder(json_file):
    try:
        if os.path.exists(json_file):  # Check if the file exists
            done_folder = folders["Done_Folder"]
            shutil.move(json_file, os.path.join(done_folder, os.path.basename(json_file)))
            logging.info(f"Moved {json_file} to Done Folder.")
            print(f"Moved {json_file} to Done Folder.")
        else:
            logging.error(f"File not found: {json_file}")
            print(f"File not found: {json_file}")
    except Exception as e:
        logging.error(f"Error moving file to Done Folder: {e}")
        print(f"Error moving {json_file} file to Done Folder: {e}")
        

# Main task workflow with user-specified schedule frequency - CGC-2 - cmd_12.py additions for existing json file handling
def task_workflow(api_key, api_secret, erp_url, machine_data_folder, json_folder1, json_folder2):
    # 1. Process pending JSON files from JSON_Data_Folder first
    process_pending_json_files(api_key, api_secret, erp_url)

    # 2. mvf.py: Copy new files from machine_data_folder to Scan_Folder
    # copy_log_file = get_log_file_path("Copy_Logs", datetime.now().strftime('%Y-%m-%d'))
    copy_new_files(machine_data_folder, folders["Scan_Folder"], log_folders["Copy_Logs"])

    # 3. psr.py: Parse XML files to JSON in Scan_Folder
    log_file = get_log_file_path("Parser_Logs", datetime.now().strftime('%Y-%m-%d'))
    skipped_log_file = get_log_file_path("Skipped_Logs", datetime.now().strftime('%Y-%m-%d'))
    
    json_file = os.path.join(folders["JSON_Data_Folder"], f"data_{datetime.now().strftime('%Y-%m-%d_%H_%M')}.json")
    xml_files = [file for file in os.listdir(folders["Scan_Folder"]) if file.endswith('.xml')]
    
    for xml_file in xml_files:
        parse_xml_to_json(os.path.join(folders["Scan_Folder"], xml_file), json_file, log_file, 'None', json_folder1, json_folder2, skipped_log_file, log_folders["Skipped_Logs"])
        logging.info(f"JSON file created {json_file}")

    # 4. Process the newly created JSON file
    process_json_file(json_file, api_key, api_secret, erp_url)

    # 5. Backup: Move files to Backup_Folder
    # Get the list of successfully parsed files from the parser log
    successfully_parsed_files = get_successfully_parsed_files(log_file)

    # Get the list of skipped files from the skipped log
    skipped_files = get_skipped_files(skipped_log_file)

    # If we have successfully parsed files, move them to the backup folder
    if successfully_parsed_files:
        print("Entered taskflow backup")
        backup_log_file = get_log_file_path("Backup_Logs", datetime.now().strftime('%Y-%m-%d'))
        # Pass skipped_files to the move_files_to_backup function
        move_files_to_backup(folders["Scan_Folder"], folders["Backup_Folder"], backup_log_file, successfully_parsed_files, skipped_files)
    else:
        logging.info("No successfully parsed files to move to backup.")


# Process pending JSON files first before new ones
def process_pending_json_files(api_key, api_secret, erp_url):
    pending_json_files = [file for file in os.listdir(folders["JSON_Data_Folder"]) if file.endswith('.json')]
    for json_file in sorted(pending_json_files):  # Ensuring oldest files are processed first
        process_json_file(os.path.join(folders["JSON_Data_Folder"], json_file), api_key, api_secret, erp_url)


# Process each JSON file by loading its content, sending data to ERP, and moving it to Done folder
def process_json_file(json_file, api_key, api_secret, erp_url):
    data, last_pd_no = load_existing_json_2(json_file)
    
    if data:  # Only proceed if data exists
        all_successful = True

        success = send_to_erpnext(data, api_key, api_secret, erp_url)
        if not success:
            all_successful = False

        if all_successful:
            print("Moved to done!")  # Change for confirmation message
            move_to_done_folder(json_file)
        return all_successful
    else:
        print("No data found to process in the JSON file.")
        return False

# Helper function to get log file path for different operations
def get_log_file_path(log_type, date_str):
    return os.path.join(log_folders[log_type], f"{log_type.lower()}_{date_str}.log")

# # Helper function to get log file path - single named, single file
# def get_log_file_path_2(log_type):
#     return os.path.join(log_folders[log_type], f"{log_type.lower()}.log")



# Function to reset config data for API key, API secret, ERP URL, and machine data folder
def reset_config_file():
    # Ask the user for a password
    entered_password = input("Enter password to reset configuration: ")

    if entered_password == RESET_PASSWORD:
        print("Password accepted. Resetting configuration...")
        
        # Prompt for new inputs
        api_key = input("Enter new API Key (api.py): ")
        api_secret = input("Enter new API Secret (api.py): ")
        erp_url = input("Enter new ERP URL (api.py): ")
        machine_data_folder = input("Enter new Machine Target DATA folder Path (mvf.py): ")
        json_folder1 = input("Enter Laser Marking Data Folder: ")
        json_folder2 = input("Enter Laser Marking Backup Data Folder: ")
        logging.info("User Input Registered. Config.json file changed")
        
        # Load the existing config file
        config_file = os.path.join(current_directory, "config.json")
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config_data = json.load(f)

            # Update only the necessary fields
            config_data["API_Key"] = api_key
            config_data["API_Secret"] = api_secret
            config_data["ERP_URL"] = erp_url
            config_data["Machine_Data_Folder"] = machine_data_folder
            config_data["LM_JSON_FOLDER"] = json_folder1
            config_data["LM_BKP_JSON_FOLDER"] = json_folder2

            # Write the updated config back to the file
            with open(config_file, 'w') as f:
                json.dump(config_data, f, indent=4)

            print("Configuration reset successfully!")
            logging.info("RESET Successfull.")
        else:
            print("Error: Config file not found. Unable to reset.")
            logging.info("RESET Unsuccessfull.")
    else:
        print("Incorrect password. Returning to normal operation...")
        logging.info("RESET Unsuccessfull. Incorrect Password.")

# Function to check for 'STOP' or 'RESET' input in a separate thread
def control_program():
    while True:
        user_input = input("Program Started Successfully... \nType 'STOP' to exit or 'RESET' to reset configuration: ").strip().upper()

        if user_input == 'STOP':
            print("Stopping program...")
            logging.info("Program Stopped.")
            os._exit(0)

        elif user_input == 'RESET':
            print("Resetting configuration...")
            logging.info("Reset Called.")
            reset_config_file()

# Function to create and start the control thread
def start_control_thread():
    control_thread = threading.Thread(target=control_program)
    control_thread.daemon = True  # Daemon thread will not block program exit
    control_thread.start()

# Main execution logic
def main():
    logging.info("SPI Program started by the user.")
    create_folders()

    # Load inputs from config file if it exists, otherwise prompt user
    config = load_inputs_from_file()
    if config:
        api_key = config["API_Key"]
        api_secret = config["API_Secret"]
        erp_url = config["ERP_URL"]
        machine_data_folder = config["Machine_Data_Folder"]
        json_folder1 = config["LM_JSON_FOLDER"]
        json_folder2 = config["LM_BKP_JSON_FOLDER"]
    else:
        api_key, api_secret, erp_url, machine_data_folder, json_folder1, json_folder2 = get_inputs()
        write_folder_paths_to_file(api_key, api_secret, erp_url, machine_data_folder, json_folder1, json_folder2)

    # Scheduling frequency as user input
    schedule_freq = input("Enter the scheduling frequency in minutes: ")
    try:
        schedule_freq = int(schedule_freq)
    except ValueError:
        print("Invalid input. Setting default scheduling frequency to 10 minutes.")
        schedule_freq = 10

    # Schedule the task workflow at the user-defined interval
    schedule.every(schedule_freq).minutes.do(task_workflow, api_key, api_secret, erp_url, machine_data_folder, json_folder1, json_folder2)

    # Start a separate thread to monitor the STOP and RESET commands
    start_control_thread()

    # Keeps script running to execute the scheduled tasks. Catching ISR calls
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Program stopped by the user.")
        logging.info("Program stopped by the user using Keyboard Interrupt.")


if __name__ == "__main__":
    main()
