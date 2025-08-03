FROM python:3.10-slim

# 1. Cài đặt các thư viện hệ thống cần thiết
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jdk \
    git \
    curl \
    libpq-dev \
    gcc \
    unzip \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYTHONUNBUFFERED=1

# 2. Tạo thư mục làm việc
WORKDIR /opt/dagster/app

# 3. Copy file requirements
COPY requirements.txt .

# 4. Cài đặt Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# 5. Copy toàn bộ code vào image
COPY . .

# 6. Tạo DAGSTER_HOME nếu chưa có
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
