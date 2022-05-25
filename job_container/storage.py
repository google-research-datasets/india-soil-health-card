import os
import utils
import re
import shc_html_extractor
import analytics_storage
from google.cloud import storage
storage_client = storage.Client()

bucket = storage_client.bucket(os.getenv("GCS_BUCKET"))

file_prefix = 'shcs/'

def isFileDownloaded(file_path):
    return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)

def getContent(file_path):
    return storage.Blob(bucket=bucket, name=file_path).download_as_string(storage_client)

def getMetadata(file_path):
    return bucket.get_blob(file_path).metadata

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



if __name__ == "__main__":
  counter = 0
  objects = list(storage_client.list_blobs(bucket_or_name=os.getenv("GCS_BUCKET"), prefix="shcs"))
  for item in objects:
    result = re.match('shcs\/.*\/.*\/.*\/(.*)_(.*)\).html', item.name)

    if result:
        sample = result.groups()[0] 
        sr_no = int(result.groups()[1])
        metadata = item.metadata
        content = item.download_as_string(storage_client)
        extractor = shc_html_extractor.ShcHtmlExtractor(content)
        print(f'{counter} {sample}{sr_no}')
        counter = counter +1
        analytics_storage.insertCard(metadata['state'], metadata['district_code'], metadata['mandal_code'], metadata['village_code'], sample, sr_no, extractor.extract())
  