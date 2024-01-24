import os
import requests
import json
import csv
import time
import threading
from tqdm import tqdm
import requests


def download_chunk(url, start_byte, end_byte, file, pbar, max_retries=5):
    headers = {'Proxy-Connection': 'keep-alive', 'Range': f'bytes={start_byte}-{end_byte}'}
    
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, headers=headers, stream=True)
            r.raise_for_status()

            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=512):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            break  # 如果下载成功，则跳出循环

        except requests.exceptions.ChunkedEncodingError as e:
            print(f"ChunkedEncodingError: {e}")
            
            if attempt < max_retries:
                print(f"重试下载（第 {attempt + 1}/{max_retries + 1} 次）...")
                continue
            else:
                print("达到最大重试次数。无法完成下载。")
                raise

        except requests.exceptions.RequestException as e:
            print(f"下载错误: {e}")
            raise

def downloadFile_multithreaded(name, url, num_threads=4):
    r = requests.head(url)
    length = int(r.headers['content-length'])

    chunk_size = length // num_threads

    threads = []
    file_parts = []

    # Initialize the tqdm progress bar
    pbar = tqdm(total=length, unit='B', unit_scale=True, desc=f"Downloading {name}")

    for i in range(num_threads):
        start_byte = i * chunk_size
        end_byte = (i + 1) * chunk_size - 1 if i < num_threads - 1 else length - 1
        file_part = f"{name}.part{i + 1}"

        file_parts.append(file_part)
        thread = threading.Thread(target=download_chunk, args=(url, start_byte, end_byte, file_part, pbar))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Close the tqdm progress bar
    pbar.close()

    # Combine file parts into the final file
    with open(name, 'wb') as final_file:
        for part in file_parts:
            with open(part, 'rb') as f:
                final_file.write(f.read())

    # Remove temporary file parts
    for part in file_parts:
        try:
            os.remove(part)
        except FileNotFoundError:
            pass

    print(f"下载完成: {name}")


if __name__ == '__main__':
    base_url = "https://icbraindb.cytogen.ru/api-v2/"  

    endpoint = "human"
    id = ""
    url = f"{base_url}/{endpoint}/{id}"
    response = requests.get(url, params=None)
    if(response.status_code == 200):
        human_list = json.loads(response.content)
        for subject_json in human_list:

            base_url = "https://icbraindb.cytogen.ru"
            endpoint = subject_json["_url_files"]
            url = f"{base_url}{endpoint}"
            response = requests.get(url, params=None)
            

            if(response.status_code == 200):

                file_json = json.loads(response.content)

                for file in file_json:
                    base_url = "https://icbraindb.cytogen.ru/files"
                    url = base_url+file["path"]+"/"+file["filename"]
                    os.makedirs(file["path"],exist_ok=True)
                    name = file["path"]+"/"+file["filename"]
                    if not os.path.exists(name):
                        downloadFile_multithreaded(name,url,5)

            else:
                print(f"下载数据失败。HTTP 响应代码: {response.status_code}")
                print(url)
    else:
        print(f"下载数据失败。HTTP 响应代码: {response.status_code}")

     
