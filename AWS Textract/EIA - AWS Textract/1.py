import boto3
import time
import pandas as pd
from trp import Document
from sys import argv
from PyPDF2 import PdfFileWriter, PdfFileReader
import os

scriptPath, path, filename, Option, PageNo = argv
Option = int(Option)
PageNo = int(PageNo)

def GetPdfPage(pageNum, folderPath, fileName):
    inputPDF = PdfFileReader(open(folderPath+fileName, "rb"))
    output = PdfFileWriter()
    output.addPage(inputPDF.getPage(pageNum))
    with open(folderPath+'SinglePage.pdf', "wb") as outputStream:
        output.write(outputStream)
    print("done making new page")


def UploadToS3(bucket, path, filename):
    s3 = boto3.resource('s3')
    print(f"Uploading {filename} to s3")
    s3.Bucket(bucket).upload_file(path + filename, filename)
    print("File Upload Successful")


def startJob(s3BucketName, objectName, Option):
    response = None
    client = boto3.client('textract')
    if Option<3:
        response = client.start_document_text_detection(DocumentLocation={'S3Object': {'Bucket': s3BucketName,'Name': objectName}})
    elif Option==3:
        response = client.start_document_analysis(DocumentLocation={'S3Object': {'Bucket': s3BucketName,'Name': objectName}}, FeatureTypes=["TABLES"])
    elif Option==4:
        response = client.start_document_analysis(DocumentLocation={'S3Object': {'Bucket': s3BucketName, 'Name': objectName}}, FeatureTypes=["FORMS"])


    return response["JobId"]


# Document
s3BucketName = "euw-textract-eia-dev"
if Option >4:
    print("Invalid Option selected")
    exit()

if PageNo>0:
    GetPdfPage(PageNo-1, path, filename)
    filename = 'SinglePage.pdf'
    print(filename)

UploadToS3(s3BucketName, path, filename)
jobId = startJob(s3BucketName, filename, Option)
print('Job ID = {'+str(jobId)+'}')

if PageNo>0:
    os.remove(path+filename)
