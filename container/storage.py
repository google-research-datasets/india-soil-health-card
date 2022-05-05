import os
import utils
from google.cloud import storage
storage_client = storage.Client()

bucket = storage_client.bucket(os.getenv("GCS_BUCKET"))

file_prefix = 'shcs/'

def isFileDownloaded(file_path):
    return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)

def getContent(file_path):
    return storage.Blob(bucket=bucket, name=file_path).download_as_string(storage_client)

def getFileName( sample_text, srno_text):
    if srno_text:
        return sample_text.replace('/','-') + '_' + str(srno_text) + ').html'
    else:
        return sample_text.replace('/','-') + '.html'

def getFilePath(state, district, mandal, village, sample, srno):
    return file_prefix + str(state)+ "/"+ str(district) + "/"+ str(mandal) +"/"+ str(village) +"/"+ getFileName(sample, srno)

def uploadFile( file_path , content, metadata):
    if len(content) < 1000:
      utils.logText(f"Unable to download report {file_path}")
      return

    utils.logText("Writing SHC to "+file_path)
    blob = bucket.blob(file_path)
    blob.metadata = metadata
    blob.upload_from_string(content)