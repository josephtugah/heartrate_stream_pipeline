# Set the base image to Python 3.10.0
FROM python:3.10.0-slim

# Set environment variables for Spark (if needed, optional)
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3.3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Working directory
WORKDIR /app

# Install necessary packages including kafkacat
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    kafkacat \
    && apt-get clean

# Upgrade pip and install dependencies
RUN pip install --upgrade pip
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY . .

# Copy the service account key (Optional if mounting instead)
COPY compute-engine-key.json /app/compute-engine-key.json

# Set the command to run the producer script
CMD ["python", "-m", "stream.producer"]
