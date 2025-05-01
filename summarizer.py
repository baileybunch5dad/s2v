import pandas as pd

# df = pd.read_csv('foo.txt', usecols=[0,1,2,3,4,5], names=['ncalls','tottime','percall','cumtime','percall','fwhere'], header=None)

df = pd.read_table('foo.txt', sep=' ', usecols=[0,1,2,3,4,5], header=None)

df.columns = ['ncalls','tottime','percall','cumtime','percall','fwhere']
print(df)

