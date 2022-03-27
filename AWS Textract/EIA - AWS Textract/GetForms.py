import boto3
import time
import pandas as pd
from trp import Document
from sys import argv
from PyPDF2 import PdfFileWriter, PdfFileReader
import os


scriptpath, path, filename, ExcelFile, PageNo = argv
PageNo = int(PageNo)


def GetPdfPage(pageNum, folderPath, fileName):
    inputPDF = PdfFileReader(open(folderPath+fileName, "rb"))
    output = PdfFileWriter()
    output.addPage(inputPDF.getPage(pageNum))
    with open(folderPath+'SinglePage.pdf', "wb") as outputStream:
        output.write(outputStream)
    print("done making new page")


def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3BucketName,
                'Name': objectName
            }},FeatureTypes=["FORMS"])

    return response["JobId"]


def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while (status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status


def getJobResults(jobId):
    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    doc = Document(response)
    pages.append(response)
    print("Result set page recieved : {}".format(len(pages)))
    nextToken = None
    if ('NextToken' in response):
        nextToken = response['NextToken']

    while (nextToken):
        time.sleep(5)

        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if ('NextToken' in response):
            nextToken = response['NextToken']

    return doc


def UploadToS3(bucket, path, filename):
    s3 = boto3.resource('s3')
    print(f"Uploading {filename} to s3")
    s3.Bucket(bucket).upload_file(path + filename, filename)
    print("File Upload Successful")


def ParseResponse(doc, path, ExcelFile):
    values = []
    i = 0
    for page in doc.pages:
        i += 1
        for field in page.form.fields:
            if field.value is None:
                continue
            else:
                temp = [i, field.key.text, field.value.text]
            values.append(temp)

    df = pd.DataFrame(data=values, columns=['Page Number', 'Key', 'Value'])
    df.to_csv(path + ExcelFile)


# Document
s3BucketName = "euw-textract-eia-dev"
'''path = 'Z:/Use_Case_Data/FISS_G_Internal_Order_Creation/AWStesting/'
filename = 'EIAtest.pdf'
ExcelFile = 'Forms.csv'
PageNo = 0'''

if PageNo>0:
    GetPdfPage(PageNo-1, path, filename)
    filename = 'SinglePage.pdf'
    print(filename)

UploadToS3(s3BucketName, path, filename)
jobId = startJob(s3BucketName, filename)
print("Started job with id: {}".format(jobId))
if isJobComplete(jobId):
    response = getJobResults(jobId)
    ParseResponse(response, path, ExcelFile)
    print("Script execution complete")

if PageNo>0:
    os.remove(path + filename)