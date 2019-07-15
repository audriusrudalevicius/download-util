import functools
import hashlib
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, _base
from contextlib import closing
from urllib.parse import urlparse

import requests
from tqdm import tqdm

__all__ = ["Downloader"]

log = logging.getLogger("downloader")


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
                    log.debug('Non OK Response: %s', response.status_code)
                    return
                expected_size = int(response.headers.get('Content-Length'))
                try:
                    if "Content-Disposition" in response.headers.keys():
                        fname = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
                    else:
                        fname = url.split("/")[-1]
                except:
                    fname = None
                if os.path.exists(destination) and expected_size is not None:
                    existing = os.path.getsize(destination)
                    if existing == expected_size:
                        log.debug('File already exists on disk with same size "%s"', destination)
                        return url, destination, fname, None
                with closing(response), open(destination, 'wb') as file:
                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                downloaded_size = os.path.getsize(destination)
                if downloaded_size < expected_size or expected_size < 1:
                    log.debug('Removing old incomplete file "%s"', destination)
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
            log.debug('Downloading "%s" in %d try', url, _try)
            result = Downloader._transfer(url, destination, chunk_size)
            if result is not None:
                if result[3] is not None:
                    log.warning('Download error "%s" (%d/%d) %s', url, _try, max_retry, result[3])
                    if _try >= max_retry:
                        if callback is not None:
                            callback()
                        return result
                    continue
                else:
                    log.debug('Download complete "%s" in %d try', url, _try)
                    if callback is not None:
                        callback()
                    return result
            sleep_time = 2 ** _try
            log.debug('Wait for %d seconds', sleep_time)
            time.sleep(sleep_time)
        log.debug('Download failed "%s" in %d try', url, _try)
        if callback is not None:
            callback()

    def download_batch(self, urls, stream=False):
        log.info('Preparing download list')
        if stream is False:
            source = list(map(lambda url: (url, mk_dest_path(url, self._destination_path)), urls))
        else:
            source = map(lambda url: (url, mk_dest_path(url, self._destination_path)), urls)
        t = tqdm(total=len(source))
        return self._pool.map(functools.partial(self._download, callback=t.update), source, chunksize=len(source))


def mk_dest_path(url, destination_path):
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    pu = urlparse(url)
    fn = os.path.basename(pu[2])

    if fn is None or fn is '':
        fn = url_hash

    return destination_path + '/' + fn
