import subprocess
import csv
import re
import argparse

def get_pod_logs(namespace="default", label_selector="app=consumer"):
    """Fetches logs from the consumer pod."""
    cmd = f"kubectl logs -l {label_selector} -n {namespace} --tail=-1"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
    except Exception as e:
        print(f"Error executing kubectl: {e}")
        return ""
    if result.returncode != 0:
        print(f"Error fetching logs: {result.stderr}")
        return ""
    if result.stdout is None:
        return ""
    return result.stdout

def parse_logs(logs):
    """Parses logs to extract arrival and processing times."""
    if not logs:
        return []
    data = []
    # Regex to match log lines
    # [x] Received Message at 1707930000.0 at 2024-02-14T12:00:00.000000
    # [x] Done in 0.5000s at 2024-02-14T12:00:00.500000
    
    received_pattern = re.compile(r"\[x\] Received (.*?) at (.*?)$")
    done_pattern = re.compile(r"\[x\] Done in (.*?)s at (.*?)$")
    
    current_message = {}
    
    for line in logs.splitlines():
        received_match = received_pattern.search(line)
        done_match = done_pattern.search(line)
        
        if received_match:
            current_message = {
                "message_content": received_match.group(1),
                "arrival_time": received_match.group(2)
            }
        elif done_match and current_message:
            current_message["processing_time"] = done_match.group(1)
            current_message["completion_time"] = done_match.group(2)
            data.append(current_message)
            current_message = {} # Reset
            
    return data

def save_to_csv(data, filename="simulation_data.csv"):
    """Saves parsed data to a CSV file."""
    if not data:
        print("No data to save.")
        return

    keys = ["message_content", "arrival_time", "processing_time", "completion_time"]
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    logs = get_pod_logs()
    parsed_data = parse_logs(logs)
    save_to_csv(parsed_data)
