import requests

uri = "https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io/api/v1/dags/forex_data_pipeline/tasks"
username = "airflow"
password = "airflow"

# Prepare basic authentication credentials
auth = requests.auth.HTTPBasicAuth(username, password)

# Use a session to handle cookies or additional headers if needed
session = requests.Session()
session.auth = auth

# Add any additional headers if required
headers = {
    "Content-Type": "application/json",
    # Add any other headers required by the API
}

try:
  # Send GET request with basic authentication using the session
  response = session.get(uri, headers=headers)

  # Check for successful response
  if response.status_code == 200:
    # Parse JSON data
    data = response.json()
    print("Data retrieved successfully!")
    print(data)
  else:
    print(f"Error retrieving data: {response.status_code}")
except requests.exceptions.RequestException as e:
  print(f"An error occurred: {e}")