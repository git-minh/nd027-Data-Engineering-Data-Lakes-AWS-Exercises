# Use an official Python runtime as a parent image
FROM python:3.7-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Add current directory files to the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install apache-airflow
RUN pip install apache-airflow

# Add Python bin directory to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Copy the Airflow configuration file into the Docker image
COPY airflow.cfg /root/airflow/airflow.cfg

# Initialize the Airflow database
RUN airflow db init

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV NAME World

# Run airflow when the container launches
CMD ["airflow", "webserver", "-p", "8080"]