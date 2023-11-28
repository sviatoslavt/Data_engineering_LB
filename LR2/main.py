import shutil
import zipfile
import aiohttp
import asyncio
import aiofiles
import os

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async def download_and_extract(session, url):
    print("Processing: " + url)
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Не вдалося завантажити файл з URL: {url}")
            return

        file_name = os.path.basename(url)
        file_path = os.path.join('downloads', file_name)
        async with aiofiles.open(file_path, 'wb') as file:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                await file.write(chunk)

        print(f"Завантажено: {file_name}")

        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall('downloads')
        
        os.remove(file_path)
        print("Файли розархівовано, zip видалено")

async def main():
    if not os.path.exists('downloads'):
        os.makedirs('downloads')
    
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(download_and_extract(session, url)) for url in download_uris]
        await asyncio.gather(*tasks)
    
    if os.path.isdir("downloads"):
        print(os.listdir("downloads"))
        shutil.rmtree("downloads")
        print("Папку downloads видалено")

if __name__ == '__main__':
    asyncio.run(main())