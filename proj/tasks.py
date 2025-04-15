from celery import Celery
import subprocess

# setup celery with redis as broker
app = Celery('prefetch_tasks', 
             broker='redis://localhost:6379/0',
             backend='redis://localhost:6380/0')

# configure celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

PREFETCH_CMD = "prefetch -X u -O ./tmp/sra SRR{0}"

@app.task(bind=True, retry_backoff=True, max_retries=3)
def run_prefetch_command(self, srr_id):
    """task that runs prefetch cmd for an srr id"""
    command = PREFETCH_CMD.format(srr_id)
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        return "SUCCESS", f"{srr_id}{result.stdout}"
    except subprocess.CalledProcessError as e:
        # retry mechanism built in
        self.retry(exc=e)
        return "ERROR", f"{srr_id}{e.stderr}"