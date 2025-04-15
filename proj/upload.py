from tasks import srrDownload, testDownload
import csv

def upload_records(file_path='/mnt/mycephfs/sradownloads/first10k.csv'):
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

def upload_test_records():
    tasks = 0
    with open('/home/ubuntu/script/sradump/test.log', 'r') as f:
        for id in f:
            tasks += 1
            # Enqueue each id as a task
            testDownload.delay(id)
            print("sent task " + id, end="")
    print(f"Uploaded {tasks} records.")


if __name__ == "__main__":
    # upload_records()
    upload_test_records()
    pass




