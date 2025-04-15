SraAccList.csv - all records that match filter (srr, err, drr). 

already-have.csv - ground truth i think?

missing.csv - SraAccList minus already-have

missingSRR.csv - missing.csv only SRRs

queue.csv - subset of missingSRR. must be updated

todo:
set up celery worker DONE
test with 10 in queue DONE
extract successes DONE
update queue DONE

multiple workers
multiple hosts

source ./.venv/bin/activate
usage: 
python3 enqueue.py
new terminal:
celery -A tasks worker --loglevel=info
