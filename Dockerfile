FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libfbclient2 \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy your script and dependencies
COPY requirements.txt ./
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the script
CMD ["python", "main.py"]