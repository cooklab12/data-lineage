#!/bin/bash

# Set the URL of your Spring Boot application
URL="http://localhost:8080/api/query"

# Set the directory where you want to save the CSV file
SAVE_DIR="/path/to/save/directory"

# Create the save directory if it doesn't exist
mkdir -p "$SAVE_DIR"

# Set the filename for the CSV (you can modify this as needed)
FILENAME="query_result_$(date +%Y%m%d_%H%M%S).csv"

# Full path for the CSV file
FULL_PATH="$SAVE_DIR/$FILENAME"

# Call the REST service and save the result to the CSV file
curl -o "$FULL_PATH" "$URL"

# Check if the curl command was successful
if [ $? -eq 0 ]; then
    echo "CSV file saved successfully at: $FULL_PATH"
else
    echo "Error: Failed to retrieve data from the REST service"
    exit 1
fi