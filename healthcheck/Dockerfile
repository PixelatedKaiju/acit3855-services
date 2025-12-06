FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy application code
COPY app.py /app
COPY requirements.txt /app

# Create folders for config + data
RUN mkdir -p /config/healthcheck
RUN mkdir -p /data

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port for the Health Check service
EXPOSE 8120

# Run the service
CMD ["python", "app.py"]
