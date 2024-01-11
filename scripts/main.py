import asyncio
import aiofiles
import time
import json
from aiolimiter import AsyncLimiter
import aiohttp_retry
import aiohttp

limiter = AsyncLimiter(1, 1 / 45)
key = os.getenv('TMDB_API_KEY')
headers = {'Authorization': 'Bearer ' + key}
discoverUrl = 'https://api.themoviedb.org/3/discover/movie?include_adult=true&include_video=true&sort_by=popularity.desc&vote_count.gte=1'
detailsUrl = 'https://api.themoviedb.org/3/movie/'


async def produce_date(queue):
    for year in range(2010, 2024):
        for month in range(1, 13):
            start_date = f'{year}-{str(month).zfill(2)}-01'
            end_date = f'{year}-{str(month).zfill(2)}-31'
            discover_url = f'{discoverUrl}&primary_release_date.gte={start_date}&primary_release_date.lte={end_date}'
            await queue.put(discover_url)
    await queue.put(None)


async def transform_page_urls(session, input_queue, output_queue):
    while True:
        url = await input_queue.get()
        if url is None:
            await output_queue.put(None)
            break
        async with limiter:
            async with session.get(url) as response:
                result = await response.json()
                if response.status != 200:
                    continue
        total_pages = result['total_pages']
        for i in range(1, total_pages + 1):
            page_url = f'{url}&page={i}'
            await output_queue.put(page_url)


async def transform_details_url(session, input_queue, output_queue):
    while True:
        page_url = await input_queue.get()
        if page_url is None:
            await output_queue.put(None)
            break
        async with limiter:
            async with session.get(page_url) as response:
                result = await response.json()
                if response.status != 200:
                    continue
        for movie in result['results']:
            details_url = f'{detailsUrl}{movie["id"]}?language=ko-KR'
            await output_queue.put(details_url)


async def transform_details(session, input_queue, output_queue):
    while True:
        url = await input_queue.get()
        if url is None:
            await output_queue.put(None)
            break
        async with limiter:
            async with session.get(url) as response:
                result = await response.json()
                if response.status != 200:
                    continue
        await output_queue.put(f'{json.dumps(result, ensure_ascii=False)}\n')


async def sink_file(queue):
    buffer = []
    i = 0
    start_time = time.time()
    chunk_time = time.time()
    while True:
        detail = await queue.get()
        if detail is None:
            async with aiofiles.open('../data/movie_full.json', 'a', encoding='UTF-8') as f:
                await f.writelines(buffer)
            print('task finished, total elapsed time:', time.time() - start_time)
            print('total', i, 'items processed')
            break
        buffer.append(detail)
        i += 1
        if len(buffer) >= 1000:
            print('writing chunk to files, elapsed time:', time.time() - chunk_time)
            print('total', i, 'items processed')
            async with aiofiles.open('../data/movie_full.json', 'a', encoding='UTF-8') as f:
                await f.writelines(buffer)
            buffer = []
            chunk_time = time.time()


async def main():
    date_queue = asyncio.Queue(100)
    page_queue = asyncio.Queue(100)
    details_url_queue = asyncio.Queue(5000)
    details_queue = asyncio.Queue(5000)
    file = open('../data/movie_full.json', 'w', encoding='UTF-8')
    file.close()

    async with aiohttp.ClientSession(headers=headers) as session:
        retry_options = aiohttp_retry.ExponentialRetry(attempts=5)
        client = aiohttp_retry.RetryClient(raise_for_status=False, retry_options=retry_options, headers=headers)
        tasks = [
            produce_date(date_queue),
            transform_page_urls(client, date_queue, page_queue),
            transform_details_url(client, page_queue, details_url_queue),
            *[transform_details(client, details_url_queue, details_queue) for _ in range(50)],
            sink_file(details_queue),
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
