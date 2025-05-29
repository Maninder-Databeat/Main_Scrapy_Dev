#!/bin/bash

# Function containing the lines that should always run
cleanup() {
  echo "This will always run, even if the script errors or exits."
  # Add your cleanup code here
  # For example:
  # rm -f /tmp/tempfile.txt
  # deactivate  # If you're using a virtual environment
  # Deactivate the virtual environment (important!)
  cd "$home"
  deactivate
}

# Set the trap to call the cleanup function on exit (0), error (1), and interrupt (2)
trap cleanup EXIT ERR INT

# Script to run a Python script within a virtual environment

# --- Configuration ---

# the actual path to your project directory
project_dir="/home/shani/uv_projects/scrapy_ads_txt"

# Change directory to the project directory
cd "$project_dir"

# Activate the virtual environment
source $project_dir/.venv/bin/activate

# Replace 'your_python_file.py' with the actual name of your Python file
python3 $project_dir/ads_txt/main.py
# python3 $project_dir/ads_txt/main.py
# >> $project_dir/main_start.log 2>&1

# Deactivate venv
# deactivate

# /home/shani/uv_projects/ads_txt/main_run.sh
# --- End of Script ---

# ps -eo pid,etime,cmd | grep main_run.sh

# 44 15 * * * /home/shani/uv_projects/scrapy_ads_txt/main_run.sh >> /home/shani/uv_projects/scrapy_ads_txt/main_run.log 2>&1