#!/bin/bash
set -e

echo "Starting data generation..."

cat << 'EOF' > /data/generate.py
import csv
import random
import uuid
from datetime import datetime, timedelta

# Configuration
TOTAL_RECORDS = 2000000
WHALE_CALLER = "WHALE_CALLER_001"
WHALE_COUNT = int(TOTAL_RECORDS * 0.1) # 10%

def generate_timestamp(base_time, offset_seconds):
    dt = base_time + timedelta(seconds=offset_seconds)
    return dt.isoformat()

def generate():
    output_file = "/data/cdr_data.csv"
    base_time = datetime(2023, 1, 1, 0, 0, 0)
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow(["caller_id", "receiver_id", "duration_sec", "tower_id", "timestamp", "call_type", "charge_amount"])
        
        # We need at least WHALE_COUNT for whale
        # The rest is random
        for i in range(TOTAL_RECORDS):
            is_whale = i < WHALE_COUNT
            
            caller_id = WHALE_CALLER if is_whale else f"CALLER_{random.randint(1000, 9999)}"
            receiver_id = f"RECEIVER_{random.randint(1000, 9999)}"
            
            # Whale makes longer calls on average
            if is_whale:
                duration_sec = int(random.gauss(300, 50))
            else:
                duration_sec = int(random.gauss(150, 40))
                
            duration_sec = max(1, duration_sec) # avoid negative
            
            # Occasional anomaly
            if random.random() < 0.001:
                duration_sec += 2000
                
            tower_id = f"TOWER_{random.randint(1, 50)}"
            timestamp = generate_timestamp(base_time, random.randint(0, 31536000)) # random within a year
            call_type = random.choice(["VOICE", "SMS", "DATA"])
            
            # Simple charge logic
            charge_amount = round(duration_sec * 0.05, 2)
            
            writer.writerow([caller_id, receiver_id, duration_sec, tower_id, timestamp, call_type, charge_amount])

if __name__ == "__main__":
    generate()
EOF

python3 /data/generate.py
echo "Data generation completed successfully."
