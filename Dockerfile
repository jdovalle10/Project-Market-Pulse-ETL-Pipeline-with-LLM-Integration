# Dockerfile - improved

# Base image
FROM python:3.11-slim

# Metadata
LABEL maintainer="jdovalle10"

# Prevent Python from buffering stdout/stderr and avoid writing .pyc files
ENV PYTHONUNBUFFERED=1 \
	PYTHONDONTWRITEBYTECODE=1

# Workdir
WORKDIR /app

# Create a non-root user to run the app
RUN adduser --disabled-password --gecos "" app || true

# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt .

# Upgrade pip and install requirements
RUN pip install --upgrade pip && \
	pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project, setting ownership to the non-root user
COPY --chown=app:app . .

# Use non-root user
USER app

# Entrypoint and default command
ENTRYPOINT ["python", "Main.py"]
CMD ["--help"]