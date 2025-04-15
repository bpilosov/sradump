import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import argparse
import sys
from typing import List


# -X maximum file size, u=unlimited -O output directory
PREFETCH_CMD = "prefetch -X u -O /mnt/mycephfs/sradownloads SRR{0}"
MAX_WORKERS = 18
PROJECT_DIR = "/home/ubuntu/script"
SUCCESS_LOG = f"{PROJECT_DIR}/sradump/proj/records/success.log"
ERROR_LOG = f"{PROJECT_DIR}/sradump/proj/records/error.log"
INPUT_FILE = f"{PROJECT_DIR}/sradump/proj/records/queue.csv"
MASTER_LIST = f"{PROJECT_DIR}/sradump/proj/records/missingSRR.csv"


def parallel_prefetch(srr_ids: List[int]):
    """Execute commands in parallel using ThreadPoolExecutor."""
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(run_prefetch_command, srr_id): srr_id for srr_id in srr_ids}
        
        with open(SUCCESS_LOG, "a", buffering=1) as success_log, open(ERROR_LOG, "w") as error_log:
            for future in as_completed(futures):
                status, message = future.result()
                if status == "SUCCESS":
                    success_log.write(message)
                else:
                    error_log.write(message)


def parallel_sratofastq(srr_ids: List[int]):
    with open(INPUT_FILE, "r") as queue:
        pass


def parallel_fastqtosqueakr(srr_ids: List[int]):
    with open(INPUT_FILE, "r") as queue:
        pass


def run_prefetch_command(srr_id):
    """Format and execute the prefetch command for a given SRR ID."""
    command = PREFETCH_CMD.format(srr_id)
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return "SUCCESS", f"{srr_id}{result.stdout}"
    except subprocess.CalledProcessError as e:
        return "ERROR", f"{srr_id}{e.stderr}"

def recreate_queue(limit: int):
    with open(MASTER_LIST, "r") as master, open(SUCCESS_LOG, "r") as successlog, open(INPUT_FILE, "w") as queue:
        missing = [line for line in master if line not in successlog][:limit]
        queue.write("".join(missing))
        print(f"Queue Restored, {len(missing)} records to be downloaded...")

def main():
    # Sanitizing arguments first.
    parser = argparse.ArgumentParser(
        description="A program to help download sra files and follow a process to conver them to .squeakr format via a worker pool."
    )
    parser.add_argument("-n", "--num", type=int, default=100_000, help="The number of sra files to download and convert. Defaults to 100,000.")
    
    args = None
    try:
        args = parser.parse_args()
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except argparse.ArgumentError as e:
        print(f"Argument Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
    
    
    # Recreate queue first
    start = time.perf_counter()
    recreate_queue(limit=args.num)

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