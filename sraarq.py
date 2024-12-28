import subprocess
from arq import create_pool
from arq.connections import RedisSettings

INPUT_FILE = "records/test.csv"

async def enqueue_tasks():
    """Enqueue prefetch tasks."""
    # Connect to Redis
    redis = await create_pool(RedisSettings())
    
    # Read SRR IDs from the file
    with open(INPUT_FILE, 'r') as file:
        srr_ids = [line for line in file]
    
    # Enqueue tasks
    for srr_id in srr_ids:
        await redis.enqueue_job("prefetch_command", srr_id)
    print(f"Enqueued {len(srr_ids)} tasks.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(enqueue_tasks())

COMMAND_TEMPLATE = "prefetch -X u SRR{0}"

async def prefetch_command(ctx, srr_id):
    """Task to execute the prefetch command."""
    command = COMMAND_TEMPLATE.format(srr_id)
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return {"status": "SUCCESS", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "ERROR", "error": e.stderr}

class WorkerSettings:
    """Worker configuration."""
    functions = [prefetch_command]
    redis_settings = RedisSettings()

