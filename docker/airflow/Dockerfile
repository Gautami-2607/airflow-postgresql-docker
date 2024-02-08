# Base Image
FROM python:3.9
LABEL maintainer="MarcLamberti"

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=2.6.0
ARG AIRFLOW_HOME=/opt/airflow

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies and tools
RUN apt-get update -y && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    python3-dev \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    # netcat \
    && apt-get autoremove -yqq --purge && apt-get clean

# Upgrade pip
# Create airflow user 
# Install apache airflow with subpackages
# RUN pip install --upgrade "pip==23.0.0" && \
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install apache-airflow==${AIRFLOW_VERSION} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.7.txt && \
    pip install apache-airflow-providers-http==4.3.0 && \
    pip install apache-airflow-providers-postgres==5.4.0 && \
    pip install numpy==1.21.6 && \
    pip install pandas==1.3.5 && \
    pip install google-api-python-client==1.12.11 && \
    pip install google-auth-oauthlib==0.8.0 && \
    pip install google-auth==2.17.3 

# Copy the airflow.cfg file (config)
#COPY ./config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./start-airflow.sh ./start-airflow.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./start-airflow.sh

# Set the username to use
USER airflow

# Create the folder dags inside $AIRFLOW_HOME
RUN mkdir -p ${AIRFLOW_HOME}/dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute start-airflow.sh
CMD [ "./start-airflow.sh" ]