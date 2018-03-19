#!/usr/bin/env python
import fnmatch
import os
import sys
import time
from urllib.request import urlopen
from urllib.error import HTTPError


__version__ = '1.0.0'


class PipelineSpecError(Exception):
    pass


class PipelineSpec:
    def __init__(self, filename):
        self.filename = filename
        self.data = list()
        self._read()

    def replace(self, old, new):
        for idx, record in enumerate(self.data):
            parts = record.split('/')
            for part in parts:
                if part == old:
                    self.data[idx] = record.replace(old, new)
                    break

    def search(self, pattern):
        for record in self.data:
            if fnmatch.fnmatch(record, pattern):
                yield record

    def verify(self):
        with open(self.filename, 'r') as fp:
            for line in fp:
                if line.startswith('@EXPLICIT'):
                    return True
        return False

    def _read(self):
        if not self.verify():
            raise PipelineSpecError('Invalid spec file: {}'.format(self.filename))

        with open(self.filename, 'r') as fp:
            data = list()
            for line in fp:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('@'):
                    continue
                data.append(line)
            self.data = data


class Backup:
    def __init__(self, data, destination):
        assert isinstance(data, list)
        assert isinstance(destination, str)
        self.block_size = 0xFFFF
        self.destination = os.path.normpath(destination)
        self.data = data
        self.stats = dict(
            read=0,
            written=0,
            success=0,
            skipped=0,
            fatal=list(),
            fail=list(),
        )

    def run(self):
        for url in self.data:
            self._download(url)

    def show_stats(self):
        print("### Statistics ###")
        for key, value in self.stats.items():
            fmt = '{:<10s}: {:<20d}'
            if isinstance(value, list):
                if len(value) != 0:
                    fmt += '\n=>'
                print(fmt.format(key, len(value)))
                for url, reason in value:
                    print('  [{}] {}'.format(reason, url))
                continue

            if key == 'read' or key == 'written':
                value = value / (1024 ** 2)
                fmt = '{:<10s}: {:<.02f}MB'

            print(fmt.format(key, value))

    def _download(self, url):
        path, filename = self._determine_local_path(url)
        dirpath = os.path.join(self.destination, path)
        fullpath = os.path.join(dirpath, filename)
        block_size = self.block_size

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        elif os.path.exists(fullpath):
            self.stats['skipped'] += 1
            return

        try:
            with urlopen(url) as data:
                with open(fullpath, 'w+b') as fp:
                    if self.verbose:
                        print("Writing: {}".format(fullpath))

                    chunk = data.read(block_size)
                    self.stats['read'] += len(chunk)
                    while chunk:
                        fp.write(chunk)
                        self.stats['written'] += len(chunk)
                        chunk = data.read(block_size)
                        self.stats['read'] += len(chunk)
        except HTTPError as reason:
            self.stats['fail'].append([url, reason])
            return
        except Exception as reason:
            self.stats['fatal'].append([url, reason])

        self.stats['success'] += 1

    def _determine_local_path(self, record):
        assert isinstance(record, str)
        filename = os.path.basename(record)
        markers = []

        for i, ch in enumerate(record):
            if ch == '/':
                markers.append(i)

        markers_len = len(markers)
        if markers_len < 3:
            raise ValueError('Invalid URL part length')

        begin = markers[markers_len - 3] + 1  # start after leading slash
        end = markers[markers_len - 1]

        local_path = os.path.normpath(os.path.join(
            self.destination, record[begin:end]))
        return local_path, filename


def find_specs(search_path, pattern):
    """ Compile list of spec file paths
    """
    for root, dirs, files in os.walk(search_path):
        for filename in files:
            filename = os.path.join(root, filename)
            if fnmatch.fnmatch(filename, pattern):
                yield filename


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--basedir', '-d', default='.',
                        help='Search for spec files under this path')

    parser.add_argument('--extension', '-e', default='*-py*.txt',
                        help='Match spec file extension by glob')

    parser.add_argument('--search-pattern', '-s', default='*',
                        help='Return packages from spec files matching glob pattern')

    parser.add_argument('--replace-pattern', '-r', action='append',
                        default=list(), nargs='*', help='Replace pattern in package output strings')

    parser.add_argument(
        '--backup', '-b', help='Backup packages to root directory (preserve relative tree)')
    parser.add_argument('--version', action='store_true')

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit(0)

    info = list()
    for spec in find_specs(args.basedir, args.extension):
        pspec = PipelineSpec(spec)

        for pattern in args.replace_pattern:
            old, new = pattern
            pspec.replace(old, new)

        if args.search_pattern:
            info += pspec.search(args.search_pattern)
        else:
            info += pspec.data

    if not info:
        print("No spec files found (extension: '{}')".format(args.extension), file=sys.stderr)
        exit(0)

    info = sorted(set(info))

    if args.backup:
        if not os.path.exists(args.backup):
            os.makedirs(args.backup)

        backup = Backup(info, args.backup)
        backup.verbose = True
        backup.run()
        backup.show_stats()

    else:
        for x in info:
            print(x)
