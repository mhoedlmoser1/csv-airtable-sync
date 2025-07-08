# Use official slim Python image
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY . .

# Run via Gunicorn on port 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--timeout", "1800", "wrapper:app"]
