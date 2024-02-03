FROM dpage/pgadmin4:latest

# Set environment variables for pgAdmin 4
ENV PGADMIN_DEFAULT_EMAIL=username@domainname.com
ENV PGADMIN_DEFAULT_PASSWORD=strongpassword

# Expose the pgAdmin 4 port
EXPOSE 80

# Create a volume for pgAdmin data
VOLUME /opt/airflow/dags/pgadmin