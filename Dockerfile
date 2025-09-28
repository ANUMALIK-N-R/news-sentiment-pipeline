FROM python:3.10-slim

# Install Java for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-21-jdk \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java 21
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . /app
