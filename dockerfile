# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app code
COPY Agents/ ./Agents/
COPY app.py .
COPY asyncutils ./asyncutils


# Expose port 8080 for Cloud Run
EXPOSE 8080
ENV PORT 8080

CMD ["hypercorn", "--bind", "0.0.0.0:8080", "app:app"]
