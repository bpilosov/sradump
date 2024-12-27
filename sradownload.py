import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

CMD = "prefetch -pX u SRR{1}"
MAX_WORKERS = 10
SUCCESS_LOG = "records/success.log"
ERROR_LOG = "records/error.log"
INPUT_FILE = "records/test.csv"

def parallel_prefetch(srr_ids):
    """Execute commands in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(run_command, srr_id): srr_id for srr_id in srr_ids}
        
        with open(SUCCESS_LOG, "w") as success_log, open(ERROR_LOG, "w") as error_log:
            for future in as_completed(futures):
                status, message = future.result()
                if status == "SUCCESS":
                    success_log.write(message + "\n")
                else:
                    error_log.write(message + "\n")

def run_command(srr_id):
    """Format and execute the prefetch command for a given SRR ID."""
    command = CMD.format(srr_id)
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return "SUCCESS", f"SRR{srr_id}\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        return "ERROR", f"SRR{srr_id}\n{e.stderr}"

def main():
    # Read SRR IDs from the input file
    srr_ids = [line for line in INPUT_FILE]
    
    # Execute commands in parallel
    parallel_prefetch(srr_ids)
    print(f"Results saved in '{SUCCESS_LOG}' and '{ERROR_LOG}'")

if __name__ == "__main__":
    main()