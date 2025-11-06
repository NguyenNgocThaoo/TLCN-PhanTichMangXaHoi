# Dockerfile tùy chỉnh cho Airflow với dependencies để chạy notebook

FROM apache/airflow:2.7.1

USER root

# Cài đặt các công cụ cần thiết (bao gồm Docker CLI)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Fix compatibility issue: Upgrade openlineage provider nếu cần
RUN pip install --no-cache-dir --upgrade \
    'apache-airflow-providers-openlineage>=1.8.0' || true

# Cài đặt Python packages
RUN pip install --no-cache-dir \
    papermill==2.4.0 \
    nbconvert==7.9.2 \
    nbformat==5.9.2 \
    pyspark==3.5.0 \
    requests==2.31.0 \
    boto3==1.28.85 \
    apache-airflow-providers-apache-spark==4.7.1 \
    ipykernel==6.25.2 \
    jupyter==1.0.0

# Cài đặt IPython kernel để chạy notebooks
RUN python -m ipykernel install --user --name=python3

# Verify installations
RUN pip list | grep -E "papermill|nbconvert|nbformat|pyspark|requests|boto3|ipykernel|jupyter"

# Verify kernel đã được cài đặt
RUN jupyter kernelspec list

