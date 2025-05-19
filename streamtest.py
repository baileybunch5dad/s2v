import numpy as np
import pickle
from DynamicDist import DynamicDist
import time
import traceback

def perftest(size):
    print(f"perftest {size=}")
    krmdata = {}
    for scenario in range(3000):
        some_data = np.random.uniform(low=100,high=200,size=size)
        dd = DynamicDist()
        dd.add_many(some_data)
        krmdata[scenario] = dd
    t1 = time.perf_counter()
    try:
        print(f"dumps 3k x {size}")
        b = pickle.dumps(krmdata)
        print(f"loads 3k x {size}")
        o = pickle.loads(b)
    except MemoryError as e:
        print("pickle was slow and ran out of memory")
        stack_trace = traceback.format_exc()
        print(stack_trace)
        exit(1)
    t2 = time.perf_counter()
    print(f"Pickle dictionary of 3k dds of size={size} took {t2-t1:.2f} seconds")
    
    krmdata3 = {}
    for k,dd in krmdata.items():
        dd.load_bins()
        mylist = [np.array(list(dd.bins.keys()), dtype=np.int32), np.array(list(dd.bins.values()), dtype=np.int32), dd.bin_offset, dd.bin_size]
        krmdata3[k] = mylist
    t1 = time.perf_counter()
    krmdata4 = pickle.loads(pickle.dumps(krmdata3))
    t2 = time.perf_counter()
    print(f"Pickle dictionary of internals 3k dds of size={size} took {t2-t1:.2f} seconds")
    
    
if __name__ == "__main__":
    for size in [500,5000, 50000, 500000]:
        perftest(size)