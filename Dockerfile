# Use official Python 3.10 base image
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies (only if needed)
RUN apt-get update && apt-get install -y \
    libsm6 libxext6 libgl1 libxrender1 libfontconfig1 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install essential Python tools
RUN pip install --upgrade pip

# Copy only requirements file first (better caching)
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Install Flask and Gunicorn
RUN pip install Flask gunicorn

# Copy application code (after dependencies for better caching)
COPY . /app

# Set environment variables
ENV FLASK_APP=app.py
ENV PORT=5000

# Expose the application port
EXPOSE 5000

# Use Gunicorn to serve the Flask app
#CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "3", "--threads", "8", "app:app"]
CMD exec gunicorn --bind :$PORT --workers 3 --threads 8 app:app
