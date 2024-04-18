import requests
import s3fs
import os
import time
import json

from loguru import logger
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


sekarang = datetime.now()
format_ymd_hms = sekarang.strftime("%Y-%m-%d %H:%M:%S")



def download_pdf_s3(link):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(link, headers=headers, verify=False)
    client_kwargs = {
        'key': os.getenv('KEY'),
        'secret': os.getenv('SECRET_KEY'),
        'endpoint_url': os.getenv('ENDPOINT_URL'),
        'anon': False
    }

    file_name = link.split('/')[-1].lower()
    file_name_json = link.split('/')[-1].lower().replace('pdf','json')

    metadata = {
        'link': link,
        'tag': ['bnpb', 'inarisk'],
        'domain': 'inarisk.bnpb.go.id',
        'file_name': [file_name_json, file_name],
        'path_data_raw': [
            f's3://ai-pipeline-statistics/data/data_raw/Divtik/inarisk/publikasi/json/{file_name_json}',
            f's3://ai-pipeline-statistics/data/data_raw/Divtik/inarisk/publikasi/pdf/{file_name}'
        ],
        'path_data_clear': [
            f's3://ai-pipeline-statistics/data/data_clean/Divtik/inarisk/publikasi/json/{file_name_json}',
            f's3://ai-pipeline-statistics/data/data_clean/Divtik/inarisk/publikasi/pdf/{file_name}'
        ],
        "crawling_time": format_ymd_hms,
        "crawling_time_epoch": int(time.time())
    }


    s3 = s3fs.core.S3FileSystem(**client_kwargs)
    pdf_s3 = metadata['path_data_raw'][1]
    if response.status_code == 200:
        with s3.open(os.path.join(pdf_s3), "wb") as file:
            file.write(response.content)
        print(f"File successfully saved in {pdf_s3}.")

        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        json_s3 = str(metadata['path_data_raw'][0])
        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
        try:
            with s3.open(json_s3, 'w') as s3_file:
                s3_file.write(json_data)
            logger.success(f'File {file_name_json} berhasil diupload ke S3.')
        except Exception as e:
            logger.error(f'Gagal mengunggah file {file_name_json} ke S3: {e}')
    else:
        print(f"Failed to download file. Status Code: {response.status_code}")




data_link = [
    'https://inarisk.bnpb.go.id/PDF/BUKU%20IRBI%202021.PDF',
    'https://inarisk.bnpb.go.id/pdf/BUKU%20IRBI%202022.pdf',
    'https://inarisk.bnpb.go.id/pdf/BUKU%20IRBI%202020%20KP.pdf',
    'https://inarisk.bnpb.go.id/PDF/BUKU%20IRBI%202019.PDF',
]

for link in data_link:
    download_pdf_s3(link)
