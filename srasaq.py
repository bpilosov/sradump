# TODO does not work

import saq
import asyncio
import time
from typing import List, Tuple
import redis.asyncio as redis

CMD = "prefetch -X u SRR{0}"
INPUT_FILE = "records/test.csv"
REDIS_TASK_QUEUE = "redis://localhost:6379"
REDIS_STORAGE = "redis://localhost:6380"

# Define the Queue
queue = saq.Queue.from_url(REDIS_TASK_QUEUE)
redis_client = redis.from_url(REDIS_STORAGE)

async def enqueue_tasks(srr_ids: List[str]):
    """Process a batch of SRR IDs using SAQ."""
    # Enqueue all tasks
    for srr_id in srr_ids:
        await queue.enqueue("process_srr", srr_id=srr_id) 

    # # Process results as they complete
    # for task in tasks:
    #     result = await task.result()
        # await log_result(result)


async def process_srr(ctx, srr_id: str) -> Tuple[str, str]:
    """Process a single SRR ID using the prefetch command."""
    command = CMD.format(srr_id)
    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            
            capture_output=True, text=True
        )
        stdout, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            ctx["status"] = "SUCCESS"
            ctx["msg"] = f"SRR{srr_id}\n{stdout.decode()}"
        else:
            ctx["status"] =  "ERROR"
            ctx["msg"] =  f"SRR{srr_id}\n{stderr.decode()}"
    except Exception as e:
        ctx["status"] = "ERROR"
        ctx["msg"] = f"SRR{srr_id}\n{str(e)}"

# Redis keys for storing results
SUCCESS_KEY = "success"
ERROR_KEY = "error"

async def log_result(ctx):
    """Log the result to Redis hash sets."""
    status, message = ctx["result"], ctx["msg"]
    srr_id = message.split()[0]  # Extract SRR ID from the message
    
    if status == "SUCCESS":
        await redis_client.hset(SUCCESS_KEY, srr_id, message)
    else:
        await redis_client.hset(ERROR_KEY, srr_id, message)


async def main():
    start = time.perf_counter()

    # Clear previous results
    await redis_client.delete(SUCCESS_KEY, ERROR_KEY)
    
    # Read SRRs
    with open(INPUT_FILE, 'r') as file:
        srr_ids = [line.strip() for line in file]

    # Process the batch
    await enqueue_tasks(srr_ids)
    
    # Get and display results summary
    # summary = await get_results_summary()

    elapsed = time.perf_counter() - start
    print(f"\nProgram completed in {elapsed:0.5f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())



settings = {
    "queue": queue,
    "functions": [process_srr],
    "concurrency": 10,
    "after_process": [log_result],
    # "startup": startup,
    # "shutdown": shutdown,
    # "before_process": before_process,
}



# async def get_results_summary():
#     """Get a summary of processing results from Redis."""
#     success_count = await redis_client.hlen(SUCCESS_KEY)
#     error_count = await redis_client.hlen(ERROR_KEY)
    
#     # Get some sample results (limit to 5 of each for demonstration)
#     success_samples = await redis_client.hgetall(SUCCESS_KEY)
#     error_samples = await redis_client.hgetall(ERROR_KEY)
    
#     print(f"\nProcessing Summary:")
#     print(f"Successful processes: {success_count}")
#     print(f"Failed processes: {error_count}")
#     print("\nSample successful results:")
#     for srr_id, result in success_samples.items():
#         print(f"- {srr_id}: {result[:100]}...")
#     print("\nSample error results:")
#     for srr_id, result in error_samples.items():
#         print(f"- {srr_id}: {result[:100]}...")
    
#     return {
#         "success_count": success_count,
#         "error_count": error_count,
#         "success_samples": dict(list(success_samples.items())),
#         "error_samples": dict(list(error_samples.items()))
#     }

