#!/bin/bash

# File to store the IP address
LOG_FILE="ip_log.txt"

# Get the current public IP
CURRENT_IP=$(curl -s ifconfig.me)

# Append the IP and timestamp to the log file
echo "$(date): $CURRENT_IP" >> $LOG_FILE