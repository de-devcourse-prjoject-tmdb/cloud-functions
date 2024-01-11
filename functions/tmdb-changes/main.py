import asyncio
import os
import time
import json
from aiolimiter import AsyncLimiter
import aiohttp_retry
import aiohttp
from google.cloud import storage
import functions_framework

API_KEY = os.getenv('TMDB_API_KEY')
HEADERS = {'Authorization': 'Bearer ' + API_KEY}
DETAILS_URL = 'https://api.themoviedb.org/3/movie/'


@functions_framework.http
def tmdb_changes(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'date' in request_json:
        date = request_json['date']
    elif request_args and 'date' in request_args:
        date = request_args['date']
    else:
        return 'date must be given in YYYY-MM-DD format', 400

    return asyncio.run(main(date))


async def produce_change_url(queue, date):
    url = f'https://api.themoviedb.org/3/movie/changes?start_date={date}&end_date={date}'
    await queue.put(url)
    await queue.put(None)


async def transform_page_urls(session, input_queue, output_queue, limiter):
    while True:
        url = await input_queue.get()
        if url is None:
            await output_queue.put(None)
            break
        async with limiter:
            async with session.get(url) as response:
                result = await response.json()
                if response.status != 200:
                    print(result)
                    continue
        total_pages = result['total_pages']
        for i in range(1, total_pages + 1):
            page_url = f'{url}&page={i}'
            await output_queue.put(page_url)


async def transform_details_url(session, input_queue, output_queue, parallelism, limiter):
    while True:
        page_url = await input_queue.get()
        if page_url is None:
            for i in range(parallelism):
                await output_queue.put(None)
            break
        async with limiter:
            async with session.get(page_url) as response:
                result = await response.json()
                if response.status != 200:
                    print(result)
                    continue
        for movie in result['results']:
            details_url = f'{DETAILS_URL}{movie["id"]}?language=ko-KR'
            await output_queue.put(details_url)


async def transform_details(session, input_queue, output_queue, limiter):
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


async def sink_cloud_storage(queue, blob):
    buffer = []
    i = 0
    start_time = time.time()
    chunk_time = time.time()
    while True:
        detail = await queue.get()
        if detail is None:
            blob.writelines(buffer)
            print('task finished, total elapsed time:', time.time() - start_time)
            print('total', i, 'items processed')
            break
        buffer.append(detail)
        i += 1
        if len(buffer) >= 1000:
            print('writing chunk to files, elapsed time:', time.time() - chunk_time)
            print('total', i, 'items processed')
            blob.writelines(buffer)
            buffer = []
            chunk_time = time.time()


async def main(date, parallelism=50):
    storage_client = storage.Client()
    bucket_name = 'tmdb-movies-dl'
    bucket = storage_client.bucket(bucket_name)
    blob_name = f'changes-{date}.json'
    blob = bucket.blob(blob_name)
    limiter = AsyncLimiter(1, 1 / 45)
    changes_queue = asyncio.Queue(100)
    page_queue = asyncio.Queue(100)
    details_url_queue = asyncio.Queue(5000)
    details_queue = asyncio.Queue(5000)

    with blob.open(mode='w', encoding='UTF-8') as f:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            retry_options = aiohttp_retry.ExponentialRetry(attempts=5)
            async with aiohttp_retry.RetryClient(raise_for_status=False, retry_options=retry_options,
                                                 headers=HEADERS) as client:
                tasks = [
                    produce_change_url(changes_queue, date),
                    transform_page_urls(client, changes_queue, page_queue, limiter),
                    transform_details_url(client, page_queue, details_url_queue, parallelism, limiter),
                    *[transform_details(client, details_url_queue, details_queue, limiter) for _ in range(parallelism)],
                    sink_cloud_storage(details_queue, f),
                ]
                await asyncio.gather(*tasks)

    return f'gs://{bucket_name}/{blob_name}'
