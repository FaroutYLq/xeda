#!/bin/bash

# Path to the runlist.txt file
FILE="/ospool/uc-shared/project/xenon/yuanlq/copy/to_dali_202312221423_sr0_all.txt"
# Path to the exceptions file
EXCEPTION_FILE="/ospool/uc-shared/project/xenon/yuanlq/copy/exceptions_202312221423.txt"

# Check if the file exists
if [ ! -f "$FILE" ]; then
    echo "File not found: $FILE"
    exit 1
fi

# Clear the exceptions file or create it if it doesn't exist
> "$EXCEPTION_FILE"

# Calculate the total number of lines in the file
total_lines=$(wc -l < "$FILE")
current_line=0

# Read each line in the file
while IFS= read -r line
do
    # Extract the rule (in this case, the whole line)
    rule="$line"

    # Execute the rucio add-rule command and capture errors
    if ! rucio add-rule "$rule" 1 UC_DALI_USERDISK 2>> "$EXCEPTION_FILE"; then
        # If the command fails, log the rule
        echo "Failed rule: $rule" >> "$EXCEPTION_FILE"
    fi

    # Update progress
    ((current_line++))
    percent=$((current_line * 100 / total_lines))
    echo -ne "Progress: $percent% ($current_line/$total_lines)\r"

    # Sleep for one minute
    sleep 60
done < "$FILE"

echo -ne '\nComplete!\n'