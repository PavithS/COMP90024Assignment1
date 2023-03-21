# coding=utf-8
from mpi4py import MPI
import os
import io
import time
import sys
sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding='utf8')  # 改变标准输出的默认编码
# 读取大型文件的函数


def handle_buffer(buff, last_line):
    str_data = buff
    lines = str_data.split("\n")
    lines = list(map(lambda x: x.strip(), lines))
    if last_line:
        lines[0] = last_line + lines[0]
    if '\n' not in lines[-1]:
        last_line = lines[-1]
        lines = lines[:-1]
    else:
        last_line = None
    return lines, last_line


def read_big_file(filename, start, end):
    with open(filename, "r", encoding='utf-8', errors="ignore") as f:
        f.seek(start)
        buff_size = 1
        first_buff = ""
        tell = start
        if comm.Get_rank() != 0:
            while True:
                tmp = f.read(1)
                tell += 1
                first_buff += tmp
                if tmp == '\n':
                    break
            print("rank:", rank, "first_buff:", first_buff)
            comm.send(first_buff.strip(), dest=rank-1)
        last_line = None
        while tell < end and end - tell>0:

            str_buff = f.read(min(buff_size, end - tell))
            print("rank:", rank, "end - tell:", end - tell)
            print("rank:", rank, "str_buff:", str_buff,min(buff_size, end - tell))
            tell += min(buff_size, end - tell)
            # print("rank:", rank, "f.tell():", end - f.tell())
            lines, last_line = handle_buffer(str_buff, last_line)

        print("rank:", rank, "last line:", last_line)
        # if rank != size-1:
        #     t = comm.recv(source=rank+1)
            # last_line_2 = last_line + t
            # print("rank:", rank, ":",
            #       last_line_2)


if __name__ == "__main__":
    # 初始化MPI
    t1 = time.time()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # 大文件的文件名和大小
    # filename = "D://bigTwitter.json"
    filename = "./twitter-data-small.json"
    filename = "./tiny.json"

    filesize = os.path.getsize(filename)

    # 计算每个进程读取的文件块大小
    blocksize = filesize // size
    start = rank * blocksize
    end = start + blocksize
    if rank == size - 1:
        end = filesize
    print("rank:", rank, "start:", start, "end:", end)

    # 在每个进程中读取文件块
    chunks = read_big_file(filename, start, end)
    if rank == 0:
        t2 = time.time()
        # print(t2-t1)
    # 将读取的结果汇总
    # result = b"".join(chunks)
    # results = comm.gather(result, root=0)

    # if rank == 0:
    #     # 将所有文件块合并成一个字符串
    #     final_result = b"".join(results)
    #     print(final_result[:100])
    MPI.Finalize()
