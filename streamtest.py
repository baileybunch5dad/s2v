import math
import numpy as np
import pickle
from DynamicDist import DynamicDist
import time
import traceback


def singlesize():
    dd =  DynamicDist()
    dd.add_many(np.random.uniform(low=100,high=200,size=99_999))
    b = pickle.dumps(dd)
    print(len(b))
    print(f"{len(b) * 3000:,d}")
    
def perftest(size):
    t0 = time.perf_counter()
    krmdata = {scenario:DynamicDist() for scenario in range(1000)}
    for k,dd in krmdata.items():
        dd.add_many(np.random.uniform(low=100,high=200,size=size))
        
    print("getting ready to pickly")
    t1 = time.perf_counter()
    # b = CHROME[zipped(pickle.dumps(dd)) for dd in krmdata.values()]
    b = pickle.dumps(krmdata)
    krmdata2 = pickle.loads(b)
    for k,dd1 in krmdata.items():
        dd2 = krmdata2[k]
        h1,b1,n1 = dd1.get_merge_data()
        h2,b2,n2 = dd2.get_merge_data()
        print(h1 == h2, b1==b2)
    t2 = time.perf_counter()
    print(f"add_many {t1-t0:.2f} seconds pickle size={size} took {t2-t1:.2f} seconds")

def compare_single_attr(o1,o2):
    if o1 is None and o2 is None:
        return True
    if isinstance(o1,float) and isinstance(o2,float):
        if math.isnan(o1) and math.isnan(o2):
            return True
    if isinstance(o1, np.ndarray) and isinstance(o2, np.ndarray):
        return np.array_equal(o1,o2)
    if not isinstance(o1, type(o2)):
        return False
    return o1 == o2

def is_primitive(obj):
    return isinstance(obj, (int, float, str, bool, np.int64, np.float64, type(None)))

def print_singleton(a):
    print(f"   Type={type(a)}",end='')
    if is_primitive(a):
        print(f" Value={a}")
    else:
        print()    

def compare_objects(obj1,obj2):
    for key in obj1.__dict__.keys() | obj2.__dict__.keys():  # Union of keys
        val1 = getattr(obj1, key, None)
        val2 = getattr(obj2, key, None)
        if compare_single_attr(val1,val2) == False:
            print(f"{key}")
            print("   Before ",end='') 
            print_singleton(val1)
            print("   After  ", end='')
            print_singleton(val2)

#print(np.array_equal(a, b))  # Output: False
    
def before_after(size):
    print(f"Comparing DynamicDist before after pickle with size={size}")
    dd1 = DynamicDist()
    dd1.add_many(np.random.uniform(low=100,high=200,size=size))    
    b = pickle.dumps(dd1)
    dd2 = pickle.loads(b)
    compare_objects(dd1, dd2)
    

    
if __name__ == "__main__":
    before_after(101)
    before_after(101000)
    
    # singlesize()
    # # sizes = np.array([10 ** i for i in range(2,7)], dtype=np.int32) * 5
    # sizes = [99_999]
    # for size in sizes:
    #     perftest(size)