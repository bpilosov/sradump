from typing import List
import time
from tasks import run_prefetch_command
from celery.result import AsyncResult

PREFETCH_SUCCESS_LOG = "./records/prefetch_success.log"
PREFETCH_ERROR_LOG = "./records/prefetch_error.log"

def distributed_prefetch(srr_ids: List[str]):
    """launch distributed prefetch tasks via celery"""
    # start tasks and get task ids
    task_results = [run_prefetch_command.delay(srr_id) for srr_id in srr_ids]
    
    # monitor task completion and log results
    pending_tasks = {result.id: (result, srr_id) for result, srr_id in zip(task_results, srr_ids)}
    
    while pending_tasks:
        for task_id, (task, srr_id) in list(pending_tasks.items()):
            if task.ready():
                # task completed, remove from pending
                pending_tasks.pop(task_id)
                
                # get result and log it
                try:
                    # get the task result values
                    status, message = task.get()
                    # format date for logging
                    date_done = task.date_done.isoformat() if task.date_done else "unknown"
                    
                    # log based on status
                    if status == "SUCCESS":
                        log_result(PREFETCH_SUCCESS_LOG, f"SRR{srr_id} {message} Date:{date_done}")
                    else:
                        log_result(PREFETCH_ERROR_LOG, f"SRR{srr_id} {message} Date:{date_done}")
                except Exception as e:
                    # unexpected error in task
                    log_result(PREFETCH_ERROR_LOG, f"SRR{srr_id} Exception: {str(e)}")
        
        # avoid hammering the system with checks
        if pending_tasks:
            time.sleep(1)

def log_result(logfile: str, message: str):
    """append a message to the specified log file"""
    with open(logfile, "a") as f:
        f.write(f"{message}\n")

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