import multiprocessing as mp
import subprocess
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import time

# -X maximum file size, u=unlimited -O output directory
CMD = "prefetch -X u -O /mnt/mycephfs/sradownloads SRR{0}"
MAX_WORKERS = 18
SUCCESS_LOG = "/mnt/mycephfs/sradownloads/success.log"
ERROR_LOG = "/mnt/mycephfs/sradownloads/error.log"
INPUT_FILE = "/mnt/mycephfs/sradownloads/queue.csv"
MASTER_LIST = "/mnt/mycephfs/sradownloads/missingSRR.csv"

def parallel_prefetch(srr_ids):
    """Execute commands in parallel using ThreadPoolExecutor."""
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(run_command, srr_id): srr_id for srr_id in srr_ids}
        
        with open(SUCCESS_LOG, "a", buffering=1) as success_log, open(ERROR_LOG, "w") as error_log:
            for future in as_completed(futures):
                status, message = future.result()
                if status == "SUCCESS":
                    success_log.write(message)
                else:
                    error_log.write(message)

def run_command(srr_id):
    """Format and execute the prefetch command for a given SRR ID."""
    command = CMD.format(srr_id)
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return "SUCCESS", f"{srr_id}{result.stdout}"
    except subprocess.CalledProcessError as e:
        return "ERROR", f"{srr_id}{e.stderr}"

def recreate_queue():
    with open(MASTER_LIST, "r") as master, open(SUCCESS_LOG, "r") as successlog, open(INPUT_FILE, "w") as queue:
        missing = [line for line in master if line not in successlog]
        queue.write("".join(missing))
        print(f"Queue Restored, {len(missing)} records to be downloaded...")

def main():
    start = time.perf_counter()
    recreate_queue()

    # Read SRR IDs from the input file
    with open(INPUT_FILE, 'r') as file:
        srr_ids = [line for line in file]
    
    # Execute commands in parallel
    parallel_prefetch(srr_ids)
    print(f"Results saved in '{SUCCESS_LOG}' and '{ERROR_LOG}'")
    elapsed = time.perf_counter() - start
    print(f"Program completed in {elapsed:0.5f} seconds.")

if __name__ == "__main__":
    main()