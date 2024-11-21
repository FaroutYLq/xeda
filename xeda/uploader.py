# This script is used to manually upload data into rucio and update DB. It does:
# 1. Read the data names from the input directory.
# 2. Check if the rule exists in rucio. 
# 3. If yes, do admix upload with rundb update. 
#   3.1. If upload succeeds, record the uploaded dataname into the success log.
#   3.2. If upload fails, record the failed dataname and errors into the failed log.
# 4. If no, record the dataname into the existed data log.
# In the whole time, record the CPU usage and memory usage every 10 seconds, 
# and save them into a npy file.

import psutil
import threading
import time
import os
import numpy as np
from functools import wraps
import admix
import sys
from rucio.common.exception import DataIdentifierNotFound
from datetime import timezone
from datetime import datetime

DATA_FOLDER = "/dali/lgrandi/xenonnt/processed/"
DESTINATION = "UC_DALI_USERDISK"
UPDATE_DB = True
INTERVAL = 10 # seconds

# 1. Read the data names from the input directory.
_, data_name_npy_path = sys.argv
list_name = os.path.basename(data_name_npy_path).split('.')[0]
folder_name = os.path.dirname(data_name_npy_path)
monitor_output_file = os.path.join(folder_name, f"{list_name}_monitor.npy")
succeeded_output_file = os.path.join(folder_name, f"{list_name}_succeeded.txt")
failed_output_file = os.path.join(folder_name, f"{list_name}_failed.txt")
existed_output_file = os.path.join(folder_name, f"{list_name}_existed.txt")
data_names = np.load(data_name_npy_path)

# A decorator that record the resources usage of the function.
def monitor_usage(output_file, interval=INTERVAL):
    """
    Monitors CPU and memory usage at regular intervals and writes data to a .npy file.
    """
    process = psutil.Process(os.getpid())

    def monitor():
        usage_stats = []
        while monitoring[0]:  # Continue monitoring as long as flag is True
            timestamp = time.time()
            cpu = process.cpu_percent(interval=None)
            memory = process.memory_info().rss / (1024 * 1024)  # Convert to MB
            usage_stats.append((timestamp, cpu, memory))
            np.save(output_file, np.array(usage_stats))  # Save stats to file
            time.sleep(interval)  # Wait for the specified interval before next check

    monitoring = [True]
    thread = threading.Thread(target=monitor)
    thread.start()
    return monitoring, thread

def stop_monitoring(monitoring, thread):
    """
    Stops the monitoring thread.
    """
    monitoring[0] = False  # Signal the thread to stop
    thread.join()  # Wait for the thread to finish

def monitor_function_during_run(output_file, interval=INTERVAL):
    """
    Decorator to monitor CPU and memory usage of a function during its execution.
    Writes resource usage data to a .npy file.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Start monitoring
            monitoring, thread = monitor_usage(output_file, interval=interval)

            # Run the decorated function
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()

            # Stop monitoring
            stop_monitoring(monitoring, thread)

            print(f"Execution time: {end_time - start_time:.2f} seconds")
            return result
        return wrapper
    return decorator

@monitor_function_during_run(monitor_output_file)
def uploader():
    admix.clients._init_clients()

    # 2. Check if the rule exists in rucio.
    for data_name in data_names:
        # Make did
        runid, name, lhash = data_name.split("-")
        did = f"xnt_{runid}:{name}-{lhash}"

        print(f"Checking data name: {data_name}")
        try:
            found_rules = admix.rucio.list_rules(did)
            # 4. If no, record the dataname into the existed data log.
            print(f"Found rules for {did}: {found_rules}, and we will skip this one.")
            with open(existed_output_file, "a") as f:
                f.write(data_name + "\n")
        except DataIdentifierNotFound as e:
            print(f"Data identifier not found: {did}, and we can upload this")
            try:
                dtnow = datetime.now(timezone.utc).isoformat()
                misc = {"creation_time": dtnow}
                admix.upload(
                    os.path.join(DATA_FOLDER, data_name), 
                    update_db=UPDATE_DB,
                    rse=DESTINATION, did=did, miscellaneous=misc
                )
                # 3.1. If upload succeeds, record the uploaded dataname into the success log.
                # TODO: This is not necessarily successful, because when upload fails 
                # the exception is not caught here.
                with open(succeeded_output_file, "a") as f:
                    f.write(data_name + "\n")
            except Exception as e:
                # 3.2. If upload fails, record the failed dataname and errors into the failed log.
                print(f"Failed to upload {data_name}: {e}")
                with open(failed_output_file, "a") as f:
                    f.write(data_name + "\n")
                    f.write(str(e) + "\n")
                    f.write("\n")
        

if __name__ == "__main__":
    uploader()
