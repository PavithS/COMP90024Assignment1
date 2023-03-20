from mpi4py import MPI
import numpy as np

def parallel_quick_sort(data, comm):
    size = comm.Get_size()
    rank = comm.Get_rank()

    # Scatter data to each process
    local_data = np.zeros(len(data) // size, dtype='int')
    comm.Scatter(data, local_data, root=0)

    # Perform quicksort on local data
    local_data.sort()

    # Gather data to the root process
    sorted_data = np.zeros(len(data), dtype='int')
    comm.Gather(local_data, sorted_data, root=0)

    if rank == 0:
        # Perform final merge sort on the sorted data
        sorted_data = merge_sort(sorted_data)

    return sorted_data

def merge_sort(data):
    if len(data) <= 1:
        return data

    # Split the data into two halves
    mid = len(data) // 2
    left = merge_sort(data[:mid])
    right = merge_sort(data[mid:])

    # Merge the two halves
    merged = []
    i, j = 0, 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            merged.append(left[i])
            i += 1
        else:
            merged.append(right[j])
            j += 1
    merged += left[i:]
    merged += right[j:]
    return merged

if __name__ == '__main__':
    # Initialize MPI environment
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # Generate random data on the root process
    if rank == 0:
        data = np.random.randint(0, 100, size=100)
    else:
        data = None

    # Call parallel quicksort function
    sorted_data = parallel_quick_sort(data, comm)

    # Print sorted data on the root process
    if rank == 0:
        print(sorted_data)
