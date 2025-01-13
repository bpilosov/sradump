from tasks import srrDownload
import csv

def upload_records(file_path='/mnt/mycephfs/sradownloads/queue.csv'):
    tasks = 0
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            record = row[0]
            tasks += 1
            # Enqueue each record as a task
            srrDownload.delay(record)
            print("sent task " + record)
    print(f"Uploaded {tasks} records.")

upload_records()




