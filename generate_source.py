import socket
import time
import json
import random
import pandas as pd
import os
from datetime import datetime, timedelta

# --- PART A: GENERATE BATCH FILES (History) ---
print(">>> [Generator] Creating data for India, USA, and Germany...")

os.makedirs("data/india_lab", exist_ok=True)
os.makedirs("data/us_cloud", exist_ok=True)
os.makedirs("data/germany_hpc", exist_ok=True)

# 1. India (CSV)
india_data = []
for i in range(10000):
    india_data.append({
        'server_code': f'IND-SRV-{i}',
        'timestamp_ist': (datetime.now() - timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
        'temp_celsius': random.randint(35, 85),
        'cpu_usage_percent': random.randint(10, 95)
    })
pd.DataFrame(india_data).to_csv("data/india_lab/server_metrics.csv", index=False)
print("   [✓] India (CSV) generated.")

# 2. USA (JSON)
us_data = []
for i in range(10000):
    us_data.append({
        'instance_id': f'us-cloud-{i}',
        'time_utc': (datetime.utcnow() - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        'temp_fahrenheit': random.randint(95, 185),
        'load_fraction': round(random.uniform(0.1, 0.9), 2),
        'voltage_v': round(random.uniform(11.8, 12.2), 2)
    })
with open('data/us_cloud/cloud_metrics.json', 'w') as f:
    for entry in us_data:
        json.dump(entry, f)
        f.write('\n')
print("   [✓] USA (JSON) generated.")

# 3. Germany (Parquet)
germany_data = []
for i in range(10000):
    germany_data.append({
        'node_name': f'DE-HPC-{i}',
        'epoch_time': int(time.time()) - (i * 60),
        'temp_kelvin': random.randint(308, 358),
        'power_watts': random.randint(400, 1200)
    })
pd.DataFrame(germany_data).to_parquet("data/germany_hpc/cluster_metrics.parquet")
print("   [✓] Germany (Parquet) generated.")


# --- PART B: REAL-TIME SOCKET (Live Stream) ---
# FORCE IPv4 and New Port
HOST = '127.0.0.1'
PORT = 9988

print(f"\n>>> [Generator] Starting Real-Time Socket Stream on {HOST}:{PORT}")
print(">>> [Generator] Run your 'streaming_pipeline.py' script now.")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)
conn, addr = s.accept()
print(f">>> [Generator] Connected by {addr}")

try:
    while True:
        # Randomly pick a Region Identity
        region_type = random.choice(['IND', 'USA', 'DE'])
        
        if region_type == 'IND':
            fake_host = f'IND-SRV-{random.randint(1, 50)}'
            volts = 0.0 
            watts = 0.0
        elif region_type == 'USA':
            fake_host = f'us-cloud-{random.randint(1000, 9999)}'
            volts = round(random.uniform(11.5, 12.5), 2)
            watts = 0.0
        else: # DE
            fake_host = f'DE-HPC-{random.randint(100, 200)}'
            volts = 0.0
            watts = round(random.uniform(400.0, 900.0), 1)

        data = {
            'host_id': fake_host,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'temp_c': random.randint(35, 95),
            'cpu_load': random.randint(5, 99),
            'voltage': volts,
            'power_watts': watts
        }
        
        message = json.dumps(data) + "\n"
        conn.sendall(message.encode('utf-8'))
        print(f"   [Stream] Sent {fake_host}")
        time.sleep(0.5) 
        
except Exception as e:
    print(f">>> Error: {e}")
finally:
    conn.close()