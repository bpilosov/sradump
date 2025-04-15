from typing import List
from tasks import run_prefetch_command
from celery.result import AsyncResult

PREFETCH_SUCCESS_LOG = "./records/prefetch_success.log"
PREFETCH_ERROR_LOG = "./records/prefetch_error.log"

def distributed_prefetch(srr_ids: List[str]):
    """launch distributed prefetch tasks via celery"""
    # start tasks and get task ids
    task_results = [run_prefetch_command.delay(srr_id) for srr_id in srr_ids]
    
    # track tasks to completion
    with (
        open(PREFETCH_SUCCESS_LOG, "w", buffering=1) as success_log,
        open(PREFETCH_ERROR_LOG, "w") as error_log,
    ):
        for task in task_results:
            # wait for the task to complete
            status, message = task.get()  # blocks until task completes
            if status == "SUCCESS":
                success_log.write(message)
            else:
                error_log.write(message)

def main():
    INPUT_FILE = "./records/test.csv"
    # read srr ids
    with open(INPUT_FILE, "r") as file:
        srr_ids = [line.strip() for line in file if line.strip()]

    to_prefetch = filter_completed_ids(srr_ids, "prefetch")
    if to_prefetch:
        print(f"Starting prefetch for {len(to_prefetch)} files...")
        distributed_prefetch(to_prefetch)
    else:
        print("All files already prefetched.")

def filter_completed_ids(srr_ids: List[str], stage: str) -> List[str]:
    """filter out already done ids"""
    log_map = {
        "prefetch": PREFETCH_SUCCESS_LOG,
    }
    completed = get_successful_ids(log_map[stage])
    return [id for id in srr_ids if id not in completed]

def get_successful_ids(logfile: str) -> set:
    """get ids already done"""
    try:
        with open(logfile, "r") as f:
            return {
                line.split("SRR")[1].split()[0]
                for line in f
                if line.strip() and "SRR" in line
            }
    except FileNotFoundError:
        return set()
    
if __name__ == "__main__":
    main()