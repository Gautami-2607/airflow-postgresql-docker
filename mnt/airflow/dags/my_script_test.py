# Working
import requests

uri = "https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io/api/v1/dags/forex_data_pipeline/tasks"
username = "airflow" 
password = "airflow" 

print("uri", uri)

# Option 1: Basic Authentication (assuming it's the correct method)
auth = requests.auth.HTTPBasicAuth(username, password)

# Use a session to handle cookies or additional headers if needed
session = requests.Session()

# Set appropriate authentication based on the chosen method
session.auth = auth  # For basic auth
# session.headers.update(headers)  # For API token

# Add any additional headers if required (e.g., Content-Type)
headers = {
    "Origin": "*",  # Might need to be adjusted based on CORS configuration
    "Content-Type": "application/json",
    "Accept": "application/json"
}

try:
    # Send GET request with authentication using the session
    response = session.get(uri, headers=headers)

    # Check for successful response
    if response.status_code == 200:
        data = response.json()
        print("Data retrieved successfully!")
        print(data)
    else:
        print(f"Error retrieving data: {response.status_code}")
        
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")