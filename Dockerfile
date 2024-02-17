# Use an official Python runtime as a base image
FROM python:3.8-slim

# Set environment variables to reduce Python buffering and to specify the Flask app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV FLASK_APP app/app.py
ENV FLASK_RUN_HOST 0.0.0.0

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (if any)
# RUN apt-get update && apt-get install -y --no-install-recommends <your-dependencies-here> && rm -rf /var/lib/apt/lists/*

# Copy the requirements files into the container at /app
COPY requirements.txt requirements.txt

# Install any needed packages specified in the requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Document that the service listens on port 5000
EXPOSE 5000

# Define the command to run the app using `flask run`
CMD ["flask", "run"]