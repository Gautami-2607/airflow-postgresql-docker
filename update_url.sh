# Set the new workspace URL
NEW_WORKSPACE_URL=$GITPOD_WORKSPACE_URL

# Add 'https://8080-' at the beginning
NEW_WORKSPACE_URL_WITH_PORT="https://8080-${NEW_WORKSPACE_URL#https://}"

# Set the path to your Python file (replace 'path/to/your/file.py' with the actual path)
PY_FILE_PATH="mnt/airflow/dags/airflow_tasks_2.py"

# Replace the value of the variable in the .py file
sed -i "s|old_value|new_value|g" "$PY_FILE_PATH"

echo "Updated variable in $PY_FILE_PATH to $NEW_WORKSPACE_URL_WITH_PORT (assuming 'new_value' replaces 'old_value')"