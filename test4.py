from mpi4py import MPI
import os
import time
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
t1 = time.time()
# filename = './smallTwitter.json'
filename = 'D://bigTwitter.json'

filesize = os.path.getsize(filename)

chunk_size = 1024*1024   # 每次读取1MB
num_chunks = (filesize + chunk_size - 1) // chunk_size
chunk_offset = rank * num_chunks // size
chunk_count = (num_chunks + size - 1) // size

with open(filename, 'rb') as f:
    f.seek(chunk_offset * chunk_size)
    data = f.read(chunk_count * chunk_size)

data_chunks = []
for i in range(size):
    chunk_offset_i = i * num_chunks // size
    chunk_count_i = (num_chunks + size - 1) // size if i == size - \
        1 else num_chunks // size
    offset_i = chunk_offset_i * chunk_size
    chunk_i = data[offset_i: offset_i + chunk_count_i * chunk_size]
    data_chunks.append(chunk_i)

chunk = comm.scatter(data_chunks, root=0)

result = comm.gather(chunk, root=0)

if rank == 0:
    t2 = time.time()
    print(t2-t1)

    # result = b''.join(result)
