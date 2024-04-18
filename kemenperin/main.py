from bs4 import BeautifulSoup
import re
from datetime import datetime
import time

import requests
import s3fs

import json
from loguru import logger


client_kwargs = {
        'key': 'GLZG2JTWDFFSCQVE7TSQ',
        'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
        'endpoint_url': 'http://192.168.180.9:8000',
        'anon': False
    }


sekarang = datetime.now()
format_ymd_hms = sekarang.strftime("%Y-%m-%d %H:%M:%S")



for ipage in range(1, 2):
    response : str = requests.get(f'https://kemenperin.go.id/direktori-perusahaan?what=&prov=35&hal={ipage}').text

    soup = BeautifulSoup(response, 'html.parser')
    date_update = soup.find(class_='text-muted').text.strip().split('tanggal ')[-1]

    table = soup.find('tbody')


    for i, tr in enumerate(table.find_all('tr'), start=1):
        tds = tr.find_all('td')

        kbli = tds[2].text.strip()

        data = str(tds[1])

        nama = data.split('<b>')[-1].split('</b>')[0]
        alamat = data.split('<br/>')[1]

        print({
            'name': nama,
            'addres': alamat
        })


        file_name = f'papua_barat_selatan{ipage}{i}.json'

        metadata = {
            'link': 'https://kemenperin.go.id/direktori-perusahaan?what=&prov=35',
            'tag': ['kemenperin', 'perusahaan'],
            'domain': 'kemenperin.go.id',
            'provinsi': 'Papua Barat Selatan',
            'update_date': date_update,
            'name': nama,
            'address': alamat,
            'kbli': kbli,
            'file_name': file_name,
            'path_data_raw': f's3://ai-pipeline-statistics/data/data_raw/Divtik/kemenperingoid/Daftar Perusahaan Industri di Papua Barat Selatan/json/{file_name}',
            'path_data_clear': f's3://ai-pipeline-statistics/data/data_clean/Divtik/kemenperingoid/Daftar Perusahaan Industri di Papua Barat Selatan/json/{file_name}',
            "crawling_time": format_ymd_hms,
            "crawling_time_epoch": int(time.time())
        }

        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        json_s3 = str(metadata['path_data_raw'])
        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
        try:
            with s3.open(json_s3, 'w') as s3_file:
                s3_file.write(json_data)
            logger.success(f'File {file_name} berhasil diupload ke S3.')
        except Exception as e:
            logger.error(f'Gagal mengunggah file {file_name} ke S3: {e}')