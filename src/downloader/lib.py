from __future__ import print_function
import concurrent
import hashlib
import os
import sys
import time
import re
import functools
from concurrent.futures import ThreadPoolExecutor, _base
from contextlib import closing
from urllib.parse import urlparse

import requests
from tqdm import tqdm


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


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
        self.real_names_map = {}

    @staticmethod
    def _transfer(url, destination, chunk_size):
        try:
            with requests.get(url, stream=True) as response:
                if response.ok is not True:
                    return
                expected_size = int(response.headers.get('Content-Length'))
                try:
                    if "Content-Disposition" in response.headers.keys():
                        fname = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
                    else:
                        fname = url.split("/")[-1]
                except:
                    fname = None
                if os.path.exists(destination):
                    existing = os.path.getsize(destination)
                    if existing == expected_size:
                        return url, destination, fname, None
                with closing(response), open(destination, 'wb') as file:
                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                downloaded_size = os.path.getsize(destination)
                if downloaded_size < expected_size or expected_size < 1:
                    os.remove(destination)
                    return
                return url, destination, fname, None
        except BaseException as e:
            return None, None, None, e

    @staticmethod
    def _download(item, callback=None):
        max_retry = 7
        chunk_size = 1 << 15
        url, destination = item
        _try = 0
        while _try < max_retry:
            _try = _try + 1
            result = Downloader._transfer(url, destination, chunk_size)
            if result is not None:
                if result[3] is not None:
                    eprint('Download error "{}" ({}/{}) {}'.format(url, _try, max_retry, result[3]))
                    if _try >= max_retry:
                        if callback is not None:
                            callback()
                        return result
                    continue
                if callback is not None:
                    callback()
                return result
            time.sleep(2 ** _try)
        if callback is not None:
            callback()

    def download_batch(self, urls):
        source = list(map(lambda url: (url, mk_dest_path(url, self._destination_path)), urls))
        t = tqdm(total=len(source))
        return self._pool.map(functools.partial(self._download, callback=t.update), source, chunksize=len(source))


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
