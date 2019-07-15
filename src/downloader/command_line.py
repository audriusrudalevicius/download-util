import sys
import os
import csv
from downloader.lib import Downloader
import logging
import argparse
__all__ = ["main"]


def filter_empty(line):
    return line is not None


def parse_csv(columns):
    if len(columns) < 1:
        return None
    return columns[0]


def main(fp=sys.stdout, argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description='Downloader')
    parser.add_argument('source', type=str)
    parser.add_argument('destination', type=str)
    parser.add_argument('--threads', type=int, default=20)
    parser.add_argument('--log', type=str, default='INFO')
    args = parser.parse_args(argv)

    is_stream = args.source.lower() is 'stdin'
    threads_count = args.threads
    download_to = os.path.realpath(args.destination)
    input_stream = sys.stdin if is_stream else open(os.path.realpath(args.source), mode='r')
    outcsv = csv.writer(fp)
    downloader = Downloader(download_to, threads_count)

    logging.basicConfig(
        level=getattr(logging, args.log),
        format="%(levelname)s:%(module)s:%(lineno)d:%(message)s")

    with input_stream as infile:
        reader = csv.reader(infile)
        for i in downloader.download_batch(filter(filter_empty, map(parse_csv, reader)), stream=is_stream):
            outcsv.writerow([i[0], i[1], i[3]])
            pass

