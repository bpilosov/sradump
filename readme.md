SraAccList.csv - all records that match filter (srr, err, drr). 

already-have.csv - ground truth i think?

missing.csv - SraAccList minus already-have

missingSRR.csv - missing.csv only SRRs

queue.csv - subset of missingSRR. must be updated

todo:
set up celery worker DONE
test with 10 in queue DONE
extract successes..? enqueue inf blocking, not writing to log BROKEN
update queue TODO

multiple workers
multiple hosts

