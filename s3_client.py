#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import copy
import logging
import logging.handlers
import math
import os
import re
import socket
import subprocess
import time
import traceback
from collections import deque
from datetime import datetime
from multiprocessing import Queue, Value, Process, Pipe, RLock
from multiprocessing.pool import ThreadPool
from posixpath import join as posix_join
from threading import BoundedSemaphore, Thread
from types import GeneratorType

import boto3
import botocore
from boto3.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError, ReadTimeoutError
from dateutil import tz
from six.moves import range
from six.moves.queue import Empty
from six.moves.urllib.parse import urlparse

EXCLUDE_ACTION = 'exclude'
INCLUDE_ACTION = 'include'

METRIC_UPDATE_PERIOD = 5
DEFAULT_S3_POOL_SIZE = 5

MB = 1024 * 1024
GB = 1024 * 1024 * 1024

DEFAULT_CHUNK_MB = 8
CHUNK_SIZE = DEFAULT_CHUNK_MB * MB  # type 8MB

MEM_LIMIT = 0.3 * GB  # 0.3GB
# https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0ae398fc54ea69ff85ec700722c9da773
MEM_FILE = '/proc/meminfo'
MEM_FACTOR = 3

MAX_RETRY = 5
DEFAULT_READ_TIMEOUT = 5 * 60  # 5m
DEFAULT_SOCKET_TIMEOUT = 1 * 60  # 1m

SYSLOG_SERVER = '10.126.83.22:1514'


class NotFoundError(Exception):
    pass


botocore_logger = logging.getLogger('botocore')
botocore_logger.setLevel(logging.ERROR)

logger = logging.getLogger('s3_client')
logger.setLevel(logging.DEBUG)


def init_logging(parsed_args):
    # type: (argparse.Namespace) -> None
    formatter = logging.Formatter(
        '%(asctime)s s3_client: [%(levelname)s] %(name)s %(message)s',
        datefmt='%b %d %H:%M:%S'
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    if parsed_args.verbose:
        stream_handler.setLevel(logging.DEBUG)
    elif parsed_args.quiet:
        stream_handler.setLevel(logging.ERROR)

    logger.addHandler(stream_handler)
    botocore_logger.addHandler(stream_handler)

    if parsed_args.remote_debug:
        syslog_handler = logging.handlers.SysLogHandler(
            address=parsed_args.remote_debug,
            socktype=socket.SOCK_DGRAM
        )
        syslog_handler.formatter = formatter
        syslog_handler.setLevel(logging.DEBUG)
        logger.addHandler(syslog_handler)
        botocore_logger.addHandler(syslog_handler)


def close_syslog_handlers():
    for handler in logger.handlers:
        if isinstance(handler, logging.handlers.SysLogHandler):
            handler.flush()
            handler.close()


def get_free_mem():
    with open(MEM_FILE, 'rb') as f:
        for line in f:
            if "MemFree" in str(line):
                return int(str(line).split()[1]) * 1024


def get_available_mem():
    with open(MEM_FILE, 'rb') as f:
        for line in f:
            if "MemAvailable" in str(line):
                return int(str(line).split()[1]) * 1024


def has_free_mem():
    if get_free_mem() > MEM_LIMIT:
        return True
    return False


def prestart_check(parsed_args):
    # type: (argparse.Namespace) -> None
    threads = parsed_args.processes * parsed_args.per_file_threads
    msg = 'This set of properties may cause MemoryError. ' \
          'Please, reduce chunk size or a number of threads|processes.'
    if get_available_mem() - threads * CHUNK_SIZE * MEM_FACTOR < MEM_LIMIT:
        if not parsed_args.force:
            raise RuntimeWarning(msg)
        logger.warning(msg)


__aws_session = None
session_lock = RLock()


def get_s3_client():
    # type: () -> Session.client
    with session_lock:
        global __aws_session
        if __aws_session is None:
            __aws_session = Session()
        return __aws_session.client(
            service_name='s3',
            config=Config(
                retries={'max_attempts': MAX_RETRY},
                read_timeout=DEFAULT_READ_TIMEOUT
            )
        )


class SafeProcess(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            if isinstance(e, BotoCoreError):
                logger.error(
                    'Got botocore error with next params: %s', e.kwargs
                )
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            raise

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


class FileStats(object):
    def __init__(self, size, mtime):
        # type: (int, float) -> None
        self.size = size
        self.mtime = mtime

    def is_newer(self, other):
        # type: (FileStats) -> bool
        if not isinstance(other, FileStats):
            raise RuntimeError(
                '%s is not instance of %s, cannot compare' %
                (other, self.__class__.__name__)
            )
        return self.mtime > other.mtime

    def size_ne(self, other):
        # type: (FileStats) -> bool
        if not isinstance(other, FileStats):
            raise RuntimeError(
                '%s is not instance of %s, cannot compare' %
                (other, self.__class__.__name__)
            )
        return self.size != other.size

    def __eq__(self, other):
        return all([
            self.size == other.size,
            self.mtime == other.mtime
        ])


class ObjectMeta(object):
    def __init__(self, relative_path, stats=None):
        # type: (str, FileStats) -> None
        self.relative_path = relative_path
        self.stats = stats


class DataManager(object):

    def list_objects(self):
        # type: () -> GeneratorType[ObjectMeta]
        raise NotImplementedError

    def get_object(self, path):
        # type: (str) -> GeneratorType[bytes]
        raise NotImplementedError

    def put_object(self, body, path, speed_metric):
        # type: (GeneratorType[bytes], str, Value) -> None
        raise NotImplementedError

    def update_file_stats(self, path, fstats):
        # type: (str, FileStats) -> None
        pass

    def get_file_stats(self, path):
        # type: (str) -> FileStats
        raise NotImplementedError


class DummyDataManager(object):

    def __init__(self, quantity, size):
        self._quantity = quantity
        self._size = size

    def list_objects(self):
        # type: () -> GeneratorType[ObjectMeta]
        for x in range(self._quantity):
            yield ObjectMeta(relative_path='dummy-%d' % x)

    def get_object(self, path):
        # type: (str) -> GeneratorType[bytes]
        if self._size < CHUNK_SIZE:
            yield b'0' * self._size
            return
        parts = self._size // CHUNK_SIZE
        for part in range(parts):
            yield b'0' * CHUNK_SIZE
        if self._size % CHUNK_SIZE:
            yield b'0' * (self._size % CHUNK_SIZE)

    def put_object(self, body, path, speed_metric):
        # type: (GeneratorType[bytes], str, Value) -> None
        for chunk in body:
            if speed_metric is not None:
                with speed_metric.get_lock():
                    speed_metric.value += len(chunk)

    def get_file_stats(self, path):
        # type: (str) -> FileStats
        return FileStats(
            size=self._size,
            mtime=time.mktime(datetime.now().timetuple())
        )


class FSDataManager(DataManager):
    DEFAULT_STREAM_PROCESSOR = 'cat - > %(path)s'

    def __init__(self, prefix, stream_processor=None):
        self._prefix = prefix
        self._stream_processor = stream_processor

    def list_objects(self):
        if os.path.isfile(self._prefix):
            prefix, file_name = os.path.split(self._prefix)
            yield ObjectMeta(
                relative_path=file_name,
                stats=self.get_file_stats(file_name)
            )
            return
        for root, _, files in os.walk(self._prefix):
            for file_name in files:
                relative_path = os.path.join(
                    root.replace(self._prefix, '').lstrip('/'),
                    file_name
                )
                yield ObjectMeta(
                    relative_path=relative_path,
                    stats=self.get_file_stats(relative_path)
                )

    def get_object(self, path):
        local_path = self._get_local_path(path)
        with open(local_path, 'rb') as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    @staticmethod
    def _prepare_dst(path):
        # type: (str) -> None
        dir_path = os.path.dirname(path)
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        except OSError as e:
            logger.debug(
                'Directory %s already exists, skipping dir creation: %s',
                path, e
            )
        # remove if already exists
        if os.path.exists(path):
            os.remove(path)
        # touch
        open(path, 'wb').close()

    def _write_to_stream_processor(self, local_path, body, speed_metric=None):
        logger.debug('Writing %s with stream processor: "%s"', local_path,
                     self._stream_processor)
        proc = subprocess.Popen(
            self._stream_processor % {'path': local_path},
            stdin=subprocess.PIPE,
            shell=True
        )
        for chunk in body:
            chunk_body = chunk.get('Body')
            proc.stdin.write(chunk_body)
            if speed_metric is not None:
                with speed_metric.get_lock():
                    speed_metric.value += len(chunk_body)
        proc.stdin.close()

    def _write_to_file(self, local_path, body, speed_metric=None):
        logger.debug('Writing %s with standard tools', local_path)
        with open(local_path, 'rb+') as f:
            for chunk in body:
                chunk_number = chunk.get('PartNumber')
                chunk_body = chunk.get('Body')
                f.seek(chunk_number * CHUNK_SIZE)
                f.write(chunk_body)
                f.flush()
                if speed_metric is not None:
                    with speed_metric.get_lock():
                        speed_metric.value += len(chunk_body)

    def put_object(self, body, path, speed_metric=None):
        if not isinstance(body, GeneratorType):
            logger.warning(
                '%s\'s method "put_object" works better when body argument is '
                'a generator',
                self.__class__.__name__
            )
        local_path = self._get_local_path(path)
        self._prepare_dst(local_path)
        if self._stream_processor:
            self._write_to_stream_processor(local_path, body, speed_metric)
        else:
            self._write_to_file(local_path, body, speed_metric)

    def update_file_stats(self, path, fstats):
        # type: (str, FileStats) -> None
        local_path = self._get_local_path(path)
        if not os.path.isfile(local_path):
            raise NotFoundError('File %s not found' % local_path)
        os.utime(local_path, (fstats.mtime, fstats.mtime))

    def get_file_stats(self, path):
        # type: (str) -> FileStats
        local_path = self._get_local_path(path)
        if not os.path.isfile(local_path):
            raise NotFoundError('File %s not found' % local_path)
        return FileStats(
            size=os.path.getsize(local_path),
            mtime=os.path.getmtime(local_path),
        )

    def _get_local_path(self, path):
        return os.path.join(self._prefix, path) if path else self._prefix

    def __repr__(self):
        return '%s (prefix=%s)' % (
            self.__class__.__name__,
            self._prefix
        )

    __str__ = __repr__


class S3SimpleDataManager(DataManager):

    def __init__(self, bucket, prefix, client=None):
        # type: (str, str, boto3.session.Session.client) -> None
        self._client = client or get_s3_client()
        self._bucket = bucket
        self._prefix = prefix

    def _get_key(self, path):
        if self._prefix.split('/')[-1] == path:
            try:
                self._client.head_object(Bucket=self._bucket, Key=self._prefix)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    return posix_join(self._prefix, path)
                raise
            return self._prefix
        return posix_join(self._prefix, path) if path else self._prefix

    def put_object(self, body, path, speed_metric):
        # type: (GeneratorType[bytes], str, Value) -> None
        if not isinstance(body, GeneratorType):
            logger.warning(
                'WARNING: %s\'s method "put_object" works better when body '
                'argument is a generator',
                self.__class__.__name__
            )
        key = self._get_key(path)
        mpu = self._client.create_multipart_upload(Bucket=self._bucket, Key=key)
        parts = []
        for idx, chunk in enumerate(body):
            upload = self._client.upload_part(
                Body=chunk,
                Bucket=self._bucket,
                Key=key,
                PartNumber=idx + 1,
                UploadId=mpu['UploadId']
            )
            parts.append({
                'ETag': upload['ETag'],
                'PartNumber': idx + 1

            })
            if speed_metric is not None:
                with speed_metric.get_lock():
                    speed_metric.value += len(chunk)

        # create key even body id empty
        if not parts:
            upload = self._client.upload_part(
                Bucket=self._bucket,
                Key=key,
                PartNumber=1,
                UploadId=mpu['UploadId']
            )
            parts.append({
                'ETag': upload['ETag'],
                'PartNumber': 1

            })

        self._client.complete_multipart_upload(
            Bucket=self._bucket,
            Key=key,
            UploadId=mpu['UploadId'],
            MultipartUpload={'Parts': parts}
        )

    def _get_object_from_s3(self, key):
        # type: (str) -> botocore.response
        try:
            return self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error('The object %s does not exist.', key)
            raise

    def get_object(self, path):
        # type: (str) -> botocore.response.StreamingBody
        key = self._get_key(path)
        body_stream = self._get_object_from_s3(key)['Body']
        while True:
            chunk = body_stream.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    def list_objects(self):
        # type: () -> list[ObjectMeta]
        paginator = self._client.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=self._bucket,
                                           Prefix=self._prefix):
                for item in page.get('Contents', []):
                    relative_path = item['Key'].replace(self._prefix, '')
                    relative_path.lstrip('/')
                    if not relative_path:
                        relative_path = self._prefix.split('/').pop()
                    mtime = item['LastModified']
                    yield ObjectMeta(
                        relative_path=relative_path,
                        stats=FileStats(
                            size=item['Size'],
                            mtime=time.mktime(
                                mtime.astimezone(tz.tzlocal()).timetuple()
                            )
                        )
                    )
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error(
                    'Prefix %s not found in %s bucket.',
                    self._prefix, self._bucket
                )
            raise

    def get_file_stats(self, path):
        # type: (str) -> FileStats
        key = self._get_key(path)
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise NotFoundError('Key %s not found' % key)
            else:
                raise
        mtime = response['LastModified']
        return FileStats(
            size=response['ContentLength'],
            mtime=time.mktime(mtime.astimezone(tz.tzlocal()).timetuple()),
        )

    def __repr__(self):
        return '%s (bucket=%s; prefix=%s)' % (
            self.__class__.__name__,
            self._bucket,
            self._prefix,
        )

    __str__ = __repr__


class S3ThreadsDataManager(DataManager):
    DEFAULT_THREADS = 1

    def __init__(self, bucket, prefix, client=None, threads=None,
                 preserve_order=False):
        # type: (str, str, boto3.session.Session.client, int, bool) -> None
        self._client = client or get_s3_client()
        self._bucket = bucket
        self._prefix = prefix
        self._threads = threads or self.DEFAULT_THREADS
        self._preserve_order = preserve_order
        self._results_lock = RLock()

    def _get_key(self, path):
        if self._prefix.split('/')[-1] == path:
            try:
                self._client.head_object(Bucket=self._bucket, Key=self._prefix)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    return posix_join(self._prefix, path)
                raise
            return self._prefix
        return posix_join(self._prefix, path) if path else self._prefix

    def _head_object(self, key):
        # type: (str) -> botocore.response
        try:
            return self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error('The object %s does not exist.', key)
            raise

    def _init_downloading_threads(self, key, parts, results, semaphore):
        # type: (str, int, deque, BoundedSemaphore) -> None
        download_pool = ThreadPool(processes=self._threads)
        logger.debug('Download thread pool for %s is initiated', key)
        for idx in range(parts):
            semaphore.acquire()
            with self._results_lock:
                results.append(
                    download_pool.apply_async(
                        func=_download_chunk_from_s3,
                        args=(self._bucket, key, idx,)
                    )
                )
        download_pool.close()
        download_pool.join()
        logger.debug('Download thread pool for %s is closed', key)

    def get_object(self, path):
        # type: (str) -> botocore.response.StreamingBody
        key = self._get_key(path)
        obj_size = int(self._head_object(key).get('ContentLength'))

        # In case of empty object
        if obj_size == 0:
            yield {'PartNumber': 0, 'Body': ''}
            return

        # Start downloading thread
        parts = int(math.ceil(float(obj_size) / CHUNK_SIZE))
        results = deque()  # type: deque[AsyncResult]
        # For cases when Network is faster than FS
        download_semaphore = BoundedSemaphore(self._threads * MEM_FACTOR)
        downloading_thread = Thread(
            target=self._init_downloading_threads,
            kwargs={
                'key': key,
                'parts': parts,
                'results': results,
                'semaphore': download_semaphore,
            }
        )
        downloading_thread.daemon = True
        downloading_thread.start()

        logger.debug('File %s of size %d will be downloaded in %d parts', key,
                     obj_size, parts)
        # Cannot relay on len(results) because there could be no results YET
        parts_processed = 0
        while parts_processed < parts:
            try:
                with self._results_lock:
                    result = results.popleft()
            except IndexError:
                time.sleep(0.1)
                continue
            if result.ready():
                yield result.get()
                parts_processed += 1
                download_semaphore.release()
            elif self._preserve_order:
                with self._results_lock:
                    results.appendleft(result)
                time.sleep(0.1)
            else:
                with self._results_lock:
                    results.append(result)
                # Just to prevent CPU usage
                time.sleep(0.005)
        logger.debug('File %s: downloading finished', key)
        downloading_thread.join()

    def put_object(self, body, path, speed_metric=None):
        # type: (GeneratorType[bytes], str, Value) -> None

        if not isinstance(body, GeneratorType):
            logger.warning(
                '%s\'s method "put_object" works better when body argument is '
                'a generator',
                self.__class__.__name__
            )

        key = self._get_key(path)
        upload_id = self._client.create_multipart_upload(
            Bucket=self._bucket,
            Key=key
        ).get('UploadId')

        # We have to use semaphore to prevent MemoryError.
        # Client will read data to memory much faster than it will be sent to
        # S3, so semaphore will block reading until there will be free slots
        # in the Pool to send data
        p_in, p_out = Pipe()
        upload_semaphore = BoundedSemaphore(self._threads)
        upload_pool = ThreadPool(processes=self._threads)
        results = []
        for idx, chunk in enumerate(body):
            upload_semaphore.acquire()
            results.append(
                upload_pool.apply_async(
                    func=_upload_chunk_to_s3,
                    args=(chunk, self._bucket, key, idx + 1, upload_id,
                          upload_semaphore, p_in, speed_metric,)
                )
            )
            if p_out.poll():
                upload_pool.terminate()
                break
        else:
            upload_pool.close()
        upload_pool.join()

        parts = [r.get() for r in results]
        # create key even body is empty
        if not parts:
            result = self._client.upload_part(
                Bucket=self._bucket,
                Key=key,
                PartNumber=1,
                UploadId=upload_id
            )
            # single empty part
            parts = [{
                'ETag': result['ETag'],
                'PartNumber': 1

            }]

        self._client.complete_multipart_upload(
            Bucket=self._bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

    def list_objects(self):
        # type: () -> list[ObjectMeta]
        paginator = self._client.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(
                    Bucket=self._bucket,
                    Prefix=self._prefix
            ):
                for item in page.get('Contents', []):
                    relative_path = item['Key'].replace(self._prefix, '')
                    relative_path = relative_path.lstrip('/')
                    if not relative_path:
                        relative_path = self._prefix.split('/').pop()
                    mtime = item['LastModified']
                    yield ObjectMeta(
                        relative_path=relative_path,
                        stats=FileStats(
                            size=item['Size'],
                            mtime=time.mktime(
                                mtime.astimezone(tz.tzlocal()).timetuple()
                            )
                        )
                    )
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error(
                    'Prefix %s not found in %s bucket.',
                    self._prefix,
                    self._bucket
                )
            raise

    def get_file_stats(self, path):
        # type: (str) -> FileStats
        key = self._get_key(path)
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise NotFoundError('Key %s not found' % key)
            else:
                raise
        mtime = response['LastModified']
        return FileStats(
            size=response['ContentLength'],
            mtime=time.mktime(mtime.astimezone(tz.tzlocal()).timetuple()),
        )

    def __repr__(self):
        return '%s (bucket=%s; prefix=%s; threads=%s)' % (
            self.__class__.__name__,
            self._bucket,
            self._prefix,
            self._threads
        )

    __str__ = __repr__


def _upload_chunk_to_s3(body, bucket, key, part_number, upload_id, semaphore,
                        err_pipe, speed_metric):
    # type: (bytes, str, str, int, str, BoundedSemaphore, Pipe, Value) -> dict
    try:
        result = get_s3_client().upload_part(
            Bucket=bucket,
            Key=key,
            Body=body,
            PartNumber=part_number,
            UploadId=upload_id
        )
        etag = result.get('ETag')
    except Exception as e:
        err_pipe.send(e)
        raise e
    finally:
        semaphore.release()

    if speed_metric is not None:
        with speed_metric.get_lock():
            speed_metric.value += len(body)

    # free mem
    del body
    del result

    return {
        'PartNumber': part_number,
        'ETag': etag
    }


def _download_chunk_from_s3(bucket, key, part_number):
    # type: (str, str, int) -> dict
    # start downloading
    bytes_range = 'bytes=%d-%d' % (
        part_number * CHUNK_SIZE,
        (part_number + 1) * CHUNK_SIZE - 1
    )

    for i in range(MAX_RETRY):
        try:
            result = get_s3_client().get_object(
                Bucket=bucket,
                Key=key,
                Range=bytes_range
            )
            body_stream = result['Body']
            body_stream.set_socket_timeout(DEFAULT_SOCKET_TIMEOUT)
            return {
                'PartNumber': part_number,
                'Body': body_stream.read(),
            }
        except ReadTimeoutError as e:
            logger.warning('Got exception on reading Key=%s Bucket=%s from '
                           's3: %s', key, bucket, e)
            continue
    else:
        raise RuntimeError(
            'Was not able to read Key=%s Bucket=%s from s3. Give up after '
            '%s tries' % (key, bucket, MAX_RETRY)
        )


class FilterAction(argparse.Action):
    """
    Action objects will be a lists of pairs: tuple(self.const, values)
    """

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is None:
            setattr(namespace, self.dest, [])

        # A bit of magic from argparse lib
        items = copy.copy(getattr(namespace, self.dest))

        items.append((self.const, values,))
        setattr(namespace, self.dest, items)


def parse_address(address):
    try:
        netloc, port = address.split(':')
    except ValueError:
        raise RuntimeError('Expected <address>:<port>, got %s' % address)
    return netloc, int(port)


def parse_args():
    parser = argparse.ArgumentParser(description='AWS S3 multithreaded client')
    parser.add_argument('src', help='Source data path: local or s3')
    parser.add_argument('dst', help='Destination data path: local or s3')
    parser.add_argument('--verbose', '-v', action='store_true',
                        required=False, help='Verbose output')
    parser.add_argument('--quiet', '-q', action='store_true', required=False,
                        help='No output except errors')
    parser.add_argument('--remote-debug', action='store', type=parse_address,
                        nargs='?', required=False, const=SYSLOG_SERVER,
                        help='Store debug info on syslog server')
    parser.add_argument(
        '--sync', action='store_true',
        help='Sync src and dst (compare by name, size and mtime)'
    )
    parser.add_argument('--processes', '-p', action='store', default=10,
                        type=int,
                        help='Number of copying processes. Default 10')
    parser.add_argument('--per-file-threads', '-t', action='store', default=5,
                        type=int,
                        help='Threads number per file. Default 5')
    parser.add_argument('--chunk-size', '-s', action='store',
                        default=DEFAULT_CHUNK_MB, type=int,
                        help='Chunk size in MB. default 8MB')
    parser.add_argument(
        '--stream-processor', action='store',
        help='Stream processor. Default: "cat - > %%(path)s". Use placeholder'
             'for path "%%(path)s"'
    )
    parser.add_argument('--force', '-f', action='store_true',
                        help='Ignore warnings')
    parser.add_argument('--retry', action='store', type=int, default=MAX_RETRY,
                        help='Max retry times')
    # Parsed args will be stored in a list of tuples: [(const, value,), ...]
    # for example: --exclude-files ".*" will be stored as [('exclude', '.*')]
    parser.add_argument('--exclude-files', '-e', dest='filters',
                        action=FilterAction,
                        help='Exclude regex pattern', const=EXCLUDE_ACTION)
    parser.add_argument('--include-files', '-i', dest='filters',
                        action=FilterAction,
                        help='Include regex pattern', const=INCLUDE_ACTION)

    return parser.parse_args()


def get_data_manager(path, parsed_args):
    # type: (str, argparse.Namespace) -> DataManager
    if path.startswith('s3://'):
        url = urlparse(path)
        return S3ThreadsDataManager(
            bucket=url.netloc,
            prefix=url.path.lstrip('/'),
            threads=parsed_args.per_file_threads,
            preserve_order=True if parsed_args.stream_processor else False
        )
    return FSDataManager(
        prefix=os.path.abspath(os.path.expanduser(path)),
        stream_processor=parsed_args.stream_processor
    )


def _should_not_sync(src_stats, dst_stats):
    # type: (FileStats, FileStats) -> bool
    # If files has different size or dst file is older than src than copy
    if src_stats.is_newer(dst_stats) or src_stats.size_ne(dst_stats):
        return False
    return True


def copy_object(src_queue, quit_signal, speed_metric, parsed_args):
    # type: (Queue, Value, Value, argparse.Namespace) -> None
    dst_dm = get_data_manager(parsed_args.dst, parsed_args)
    src_dm = get_data_manager(parsed_args.src, parsed_args)
    logger.debug('Starting copying process; src data manager: %s; dst data '
                 'manager: %s', src_dm, dst_dm)
    while True:
        if src_queue.empty() and quit_signal.value:
            logger.debug('src queue is empty and closed, exiting')
            break
        try:
            src_obj_meta = src_queue.get(timeout=1)  # type: ObjectMeta
        except Empty as e:
            logger.debug('tried to get from src queue but failed: %s', e)
            continue

        relative_path = src_obj_meta.relative_path

        logger.debug('Trying to copy %s', relative_path)
        # Sync check
        try:
            if args.sync and _should_not_sync(
                    src_obj_meta.stats,
                    dst_dm.get_file_stats(relative_path)
            ):
                logger.debug('%s was found in dst. Will not be copied',
                             relative_path)
                continue
        except NotFoundError:
            logger.debug('%s was not found in dst. Will be copied',
                         relative_path)

        dst_dm.put_object(
            body=src_dm.get_object(relative_path),
            path=relative_path,
            speed_metric=speed_metric
        )

        if args.sync:
            dst_dm.update_file_stats(
                path=relative_path,
                fstats=src_obj_meta.stats,
            )


def satisfies_filters_conditions(src_meta, filters_list):
    # type: (ObjectMeta, list[tuple]) -> bool
    for filter_type, regex in filters_list:
        if filter_type == INCLUDE_ACTION and regex.search(
                src_meta.relative_path):
            return True
        if filter_type == EXCLUDE_ACTION and regex.search(
                src_meta.relative_path):
            return False
    # by default all files are included
    return True


def filter_src_list(src_list, filters_list):
    # type: (list[ObjectMeta], list[tuple[str]]) -> list[ObjectMeta]
    compiled_filters_list = [
        (filter_type, re.compile(regex_str))
        for filter_type, regex_str in reversed(filters_list)
    ]

    for src_obj in src_list:
        if satisfies_filters_conditions(src_obj, compiled_filters_list):
            yield src_obj


def print_current_speed(speed_metric):
    # type: (Value) -> None

    if speed_metric is None:
        return

    total_size = 0
    loop_counter = 0
    while True:
        time.sleep(METRIC_UPDATE_PERIOD)
        with speed_metric.get_lock():
            current_size = speed_metric.value
            speed_metric.value = 0

        total_size += current_size
        loop_counter += 1
        print(
            'Current speed is: %.2f MB/s; Average speed is: %.2fMB/s; '
            'Processed: %.2fMB \r' %
            (float(current_size) / (MB * METRIC_UPDATE_PERIOD),
             float(total_size) / (MB * METRIC_UPDATE_PERIOD * loop_counter),
             float(total_size) / MB)
        )


def init_src_queue(src_dm, filters, src_queue, quit_signal):
    # type: (DataManager, list, Queue, Value) -> None
    try:
        for src_obj_meta in filter_src_list(src_dm.list_objects(), filters):
            src_queue.put(src_obj_meta)
    except Exception as e:
        logger.error('Failed to get src list: %s', e)
        raise e
    finally:
        quit_signal.value = True
        src_queue.close()
        logger.debug('src queue is closed, signal sent')


if __name__ == '__main__':
    args = parse_args()
    init_logging(args)

    CHUNK_SIZE = args.chunk_size * MB
    MAX_RETRY = args.retry

    prestart_check(args)

    src_dm = get_data_manager(args.src, args)
    dst_dm = get_data_manager(args.dst, args)
    if all([
        isinstance(src_dm, (S3SimpleDataManager, S3ThreadsDataManager)),
        isinstance(dst_dm, (S3SimpleDataManager, S3ThreadsDataManager))
    ]):
        raise RuntimeError('Copying from s3 to s3 is not supported yet')

    done_reading_src_list = Value('b', False)
    src_queue = Queue()
    init_src_queue_process = SafeProcess(
        target=init_src_queue,
        kwargs={
            'quit_signal': done_reading_src_list,
            'filters': args.filters or [],
            'src_dm': src_dm,
            'src_queue': src_queue
        }
    )
    init_src_queue_process.daemon = True
    init_src_queue_process.start()

    # Set speed metric
    speed_metric = None if args.quiet else Value('d', 0)
    speed_metric_process = SafeProcess(
        target=print_current_speed,
        kwargs={'speed_metric': speed_metric}
    )
    speed_metric_process.daemon = True
    speed_metric_process.start()

    # Start copying processes
    copy_procs = []
    for _ in range(args.processes):
        proc = SafeProcess(
            target=copy_object,
            kwargs={
                'speed_metric': speed_metric,
                'quit_signal': done_reading_src_list,
                'src_queue': src_queue,
                'parsed_args': args,
            }
        )
        proc.daemon = True
        proc.start()
        copy_procs.append(proc)

    # Wait until all copying processes finish
    errors = []
    for proc in copy_procs:
        proc.join()
        if proc.exception:
            err, traceback = proc.exception
            errors.append(err)
            logger.error(traceback)

    init_src_queue_process.join()
    if init_src_queue_process.exception:
        err, traceback = init_src_queue_process.exception
        logger.error(traceback)
        errors.append(err)

    close_syslog_handlers()

    if errors:
        raise errors[0]
