#!/usr/bin/env python3

import concurrent
import csv
import hashlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, _base
from contextlib import closing
from urllib.parse import urlparse

import requests
from tqdm import tqdm


class SingleThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers=None, thread_name_prefix=''):
        self._same_thread_mode = max_workers < 1
        if len(thread_name_prefix) < 1:
            raise Exception('Thread pool with out name')
        if not self._same_thread_mode:
            super().__init__(max_workers, thread_name_prefix)

    def submit(self, fn, *args, **kwargs):
        if not self._same_thread_mode:
            return super().submit(fn, *args, **kwargs)

        f = _base.Future()
        try:
            result = fn(*args, **kwargs)
            f.set_result(result)
        except BaseException as exc:
            f.set_exception(exc)

        return f


class Downloader:
    def __init__(self, download_to, parallel_count):
        self._pool = SingleThreadPoolExecutor(parallel_count, 'DownloaderPool')
        self._destination_path = download_to

    @staticmethod
    def _transfer(url, destination, chunk_size):
        try:
            with requests.get(url, stream=True) as response:
                if response.ok is not True:
                    return
                expected_size = int(response.headers.get('Content-Length'))
                if os.path.exists(destination):
                    existing = os.path.getsize(destination)
                    if existing == expected_size:
                        return destination
                with closing(response), open(destination, 'wb') as file:
                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                downloaded_size = os.path.getsize(destination)
                if downloaded_size < expected_size or expected_size < 1:
                    os.remove(destination)
                    return
                return destination
        except BaseException:
            return

    @staticmethod
    def _download(item):
        max_retry = 5
        chunk_size = 1 << 15
        url, destination = item
        _try = 0
        while _try < max_retry:
            if _try > 0:
                print('Retrying to download {} to {} ({}/{})'.format(url, destination, _try, max_retry))
            _try = _try + 1
            result = Downloader._transfer(url, destination, chunk_size)
            if result is not None:
                return result
            time.sleep(1 * _try)

    def download_batch(self, urls):
        # return self._pool.map(self._download, )
        source = list(map(lambda url: (url, mk_dest_path(url, self._destination_path)), urls))
        return tqdm_parallel_map(self._pool, self._download, source)


def tqdm_parallel_map(executor, fn, *iterables, **kwargs):
    futures_list = []
    for iterable in iterables:
        futures_list += [executor.submit(fn, i) for i in iterable]
    for f in tqdm(concurrent.futures.as_completed(futures_list), total=len(futures_list), **kwargs):
        yield f.result()


def mk_dest_path(url, destination_path):
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    pu = urlparse(url)
    fn = os.path.basename(pu[2])

    if fn is None or fn is '':
        fn = url_hash

    return destination_path + '/' + fn


if len(sys.argv) < 2:
    raise Exception('Downloads file and download dir required')

threads_count = int(sys.argv[3]) if len(sys.argv) == 4 else 20
download_to = os.path.realpath(sys.argv[2])
downloads_file = os.path.realpath(sys.argv[1])

downloader = Downloader(download_to, 20)
with open(downloads_file, mode='r') as infile:
    reader = csv.reader(infile)
    for i in downloader.download_batch(map(lambda columns: columns[0], reader)):
        pass

