import boto3
import time
import pandas as pd
from trp import Document
from sys import argv
from PyPDF2 import PdfFileWriter, PdfFileReader
import os


scriptPath, path, ExcelFile, Option, jobId = argv
Option = int(Option)


def getJobResultsDetection(jobId):
    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    doc = Document(response)
    pages.append(response)
    print("Result set page recieved : {}".format(len(pages)))
    nextToken = None
    if ('NextToken' in response):
        nextToken = response['NextToken']

    while (nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if ('NextToken' in response):
            nextToken = response['NextToken']

    return doc


def getJobResultsAnalysis(jobId):
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


def ParseResponseWord(doc, path, ExcelFile):
    values = []
    i = 0
    for page in doc.pages:
        stream = ''
        i += 1
        for line in page.lines:
            for word in line.words:
                stream += word.text
                stream += ' '
        temp = [i, stream]
        values.append(temp)

    df = pd.DataFrame(data=values, columns=['Page Number', 'Words'])
    df.to_csv(path + ExcelFile)


def ParseResponseLine(doc, path, ExcelFile):
    values = []
    i = 0
    for page in doc.pages:
        i = i + 1
        for line in page.lines:
            temp = [i, line.text]
            values.append(temp)

    df = pd.DataFrame(data=values, columns=['Page Number', "Line"])
    df.to_csv(path + ExcelFile)


def ParseResponseTable(doc, path, ExcelFile):
    values = []
    i = 1
    for page in doc.pages:
        for table in page.tables:
            for r, row in enumerate(table.rows):
                stream = [i]
                for c, cell in enumerate(row.cells):
                    stream.append(cell.text)
                values.append(stream)
        i = i+1
    df = pd.DataFrame(data=values)
    df.to_csv(path + ExcelFile)


def ParseResponseForm(doc, path, ExcelFile):
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


if Option == 1:
    response = getJobResultsDetection(jobId)
    ParseResponseWord(response, path, ExcelFile)
    print("Script execution complete")

elif Option == 2:
    response = getJobResultsDetection(jobId)
    ParseResponseLine(response, path, ExcelFile)
    print("Script execution complete")

elif Option == 3:
    response = getJobResultsAnalysis(jobId)
    ParseResponseTable(response, path, ExcelFile)
    print("Script execution complete")

elif Option == 3:
    response = getJobResultsAnalysis(jobId)
    ParseResponseForm(response, path, ExcelFile)
    print("Script execution complete")


