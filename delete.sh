#!/bin/bash

# Set the directory where the files are stored
directory=/path/to/directory

# Get the current time
current_time=$(date +%s)

# Calculate the time 6 months ago
six_months_ago=$(($current_time - 15552000))

# Loop through all the files in the directory
for file in $directory/*; do
  # Check if the file is older than 6 months
  if [ $(stat -c %Y "$file") -lt $six_months_ago ]; then
    # Delete the file
    rm "$file"
  fi
done
