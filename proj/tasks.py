import subprocess
from celery import Celery

app = Celery('proj', backend='redis://localhost:6380/0', 
             broker='redis://localhost:6379/0', include=['tasks'])

# if __name__ == '__main__':
#     app.start()

# -X maximum file size, u=unlimited -O output directory
CMD = "prefetch -X u -O /mnt/mycephfs/sradownloads SRR{0}"
@app.task
def srrDownload(srrindex):
    cmd = CMD.format(srrindex)
    res = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    return (srrindex, res.returncode, res.stdout, res.stderr)
