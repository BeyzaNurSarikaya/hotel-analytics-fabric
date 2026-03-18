FROM python:3.9-slim

WORKDIR /app

# Install the necessary libraries
RUN pip install pandas azure-eventhub

# Copy the files
COPY stream_producer.py .
COPY hotel_raw_stream.csv . 

# Run the script
CMD ["python", "stream_producer.py"]