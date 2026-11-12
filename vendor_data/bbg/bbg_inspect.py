"""
数据规模与机器资源摸底（只读 parquet 元数据 + 读最大的一个文件测内存），用于决定并行度。

  python bbg_inspect.py equityNamrV2
"""
import os
import sys
import time

import numpy as np
import pyarrow.parquet as pq

from bbg_common import list_files, read_parquet


def _meminfo_gb():
    try:
        info = dict(line.split(':', 1) for line in open('/proc/meminfo'))
        kb = lambda k: int(info[k].strip().split()[0])
        return kb('MemTotal') / 2 ** 20, kb('MemAvailable') / 2 ** 20
    except (OSError, KeyError, ValueError):
        return float('nan'), float('nan')


def main(file_name):
    files = list_files(file_name)
    rows = np.array([pq.read_metadata(f).num_rows for f in files])
    disk = np.array([os.path.getsize(f) for f in files]) / 2 ** 20
    total, avail = _meminfo_gb()
    print(f'[{file_name}] 文件数 {len(files)}，总行数 {rows.sum():,}')
    print(f'每个文件行数: min {rows.min():,} / 中位数 {int(np.median(rows)):,} / max {rows.max():,}')
    print(f'每个文件磁盘大小(MB): min {disk.min():.0f} / 中位数 {np.median(disk):.0f} / max {disk.max():.0f}，合计 {disk.sum() / 1024:.1f} GB')
    print(f'机器: CPU {os.cpu_count()} 核，内存总量 {total:.0f} GB，当前可用 {avail:.0f} GB')

    big = files[int(rows.argmax())]
    t0 = time.time()
    df = read_parquet(big)
    mem = df.memory_usage(deep=True).sum() / 2 ** 30
    print(f'最大文件 {os.path.basename(big)} 读成 pandas 后占内存 {mem:.2f} GB（{len(df):,} 行 × {df.shape[1]} 列），读取用时 {time.time() - t0:.0f}s')


if __name__ == '__main__':
    main(sys.argv[1])
