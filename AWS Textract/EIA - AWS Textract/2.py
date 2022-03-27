import boto3
import time
from sys import argv


scriptPath, jobId = argv

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status = {"+status+"}")


isJobComplete(jobId)