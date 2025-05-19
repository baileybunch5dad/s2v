import numpy as np
import pickle
from DynamicDist import DynamicDist
import time
import traceback

def perftest(size):
    krmdata = {scenario:DynamicDist() for scenario in range(3000)}
    for k,dd in krmdata.items():
        dd.add_many(np.random.uniform(low=100,high=200,size=size))
    t1 = time.perf_counter()
    b = pickle.loads(pickle.dumps(krmdata))
    t2 = time.perf_counter()
    print(f"Pickle dictionary of 3k dds of size={size} took {t2-t1:.2f} seconds")
    
if __name__ == "__main__":
    sizes = np.array([10 ** i for i in range(2,7)], dtype=np.int32) * 5
    for size in sizes:
        perftest(size)