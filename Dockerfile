FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libfbclient2 \
    tzdata \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# local time for the scheduler and date.today()
ENV TZ=Europe/Budapest

# Set the working directory
WORKDIR /app

# Copy your script and dependencies
COPY requirements.txt ./
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Keep running, the scheduler starts main.py at RUN_AT
CMD ["python", "-u", "scheduler.py"]