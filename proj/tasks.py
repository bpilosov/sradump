import subprocess
from celery import Celery

app = Celery('proj', backend='redis://localhost:6380/0', 
             broker='redis://localhost:6379/0', include=['tasks'])
# keep results in backend, spawn 32 procs
app.conf.update(result_expires=0, worker_concurrency=10)
app.conf.update(broker_connection_retry_on_startup=True)
# app.conf.update(task_ignore_result=True, task_store_errors_even_if_ignored=True)

# if __name__ == '__main__':
#     app.start()

# -L log errors, -X maximum file size u=unlimited, -O output directory
CMD = "prefetch -E -L 3 -X u -O /mnt/mycephfs/sradownloads SRR{0}"
@app.task
def srrDownload(srrindex):
    cmd = CMD.format(srrindex)
    res = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    return (srrindex, res.returncode, res.stdout, res.stderr)



TEST_CMD = f"echo {0} >> ./test/celery_test.txt"
@app.task
def testDownload(index):
    cmd = TEST_CMD.format(index)
    res = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    return (index, res.returncode, res.stdout, res.stderr)