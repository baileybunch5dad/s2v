
import pyarrow as pa
import numpy as np

# Create a PyArrow Table
table = pa.table({
    # "category": ["A", "A", "B", "B", "C"],
    "category": [1,1,2,2,3],
    "values": [1, 2, 3, 4, 5]
})

# Group by 'category' and collect values into lists
grouped = table.group_by("category").aggregate([("values", "list")])

# Convert lists to NumPy arrays
numpy_arrays = {key.as_py(): np.array(values.as_py()) for key, values in zip(grouped["category"], grouped["values_list"])}

print(numpy_arrays)

table = pa.table({
    "category1": ["A", "A", "B", "B", "C", "A", "A", "B", "B", "C"],
    "category2": ["X", "Y", "X", "Y", "X", "X", "Y", "X", "Y", "X"],
    "values1": [1, 2, 3, 4, 5, 6, 7, 8, 9 ,10 ],
    "values2": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100 ]
})

# Perform groupby on multiple columns and aggregate values into lists
grouped = table.group_by(["category1", "category2"]).aggregate([("values1", "list"), ("values2", "list")])

compound_dict = {
    (cat1.as_py(), cat2.as_py()): {
        "values1": np.array(values1.as_py()),
        "values2": np.array(values2.as_py())
    }
    for cat1, cat2, values1, values2 in zip(grouped["category1"], grouped["category2"], grouped["values1_list"], grouped["values2_list"])
}



print(compound_dict)



{('A', 'X'): {'values1': array([1, 6]), 'values2': array([10, 60])}, 
 ('A', 'Y'): {'values1': array([2, 7]), 'values2': array([20, 70])}, 
 ('B', 'X'): {'values1': array([3, 8]), 'values2': array([30, 80])}, 
 ('B', 'Y'): {'values1': array([4, 9]), 'values2': array([40, 90])}, 
 ('C', 'X'): {'values1': array([ 5, 10]), 'values2': array([ 50, 100])}}


print(grouped)


{
    'A': array([1, 2]),
    'B': array([3, 4]),
    'C': array([5])
}



from nltk.corpus import words 
import itertools
 
# clone =  copy.deepcopy(list)
def recurse(inlist, outlist):
    if len(inlist) == 0:
        l1 = outlist[0:4]
        l2 = outlist[4:]
        w1 = ''.join(l1)
        w2 = ''.join(l2)
        if w1 in words.words() or w2 in words.words():
            print(w1,w2)
            exit()
    for t in inlist:
        listcopy = [x for x in inlist if x != t]
        outlist.append(t)
        recurse(listcopy, outlist)
        outlist.pop()

# recurse(['mer','li','apo','ito','ti','ri','cal','ous'],[])

orig = ['mer','li','apo','ito','ti','ri','cal','ous']

while len(orig) > 0:
    n = 4
    all_combinations = list(itertools.combinations(orig, n))
    for combo in all_combinations:
        w = ''.join(combo)
        if w in words.words():
            print(w)
            for tok in combo:
                orig.remove(tok)
            print(orig)
            break


