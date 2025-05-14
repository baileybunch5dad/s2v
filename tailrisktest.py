import tail_risk
import numpy as np

data = np.random.uniform(low=100, high=150, size=1000)
freqs = np.random.randint(low=7, high=100, size=1000)
dist = "GPD"
alpha = 0.95
right_side = True
# df = 3
# trsh_pct = 0.9
# def tail_risk(data, freqs, alpha = 0.95, right_side = True, dist = None, df = None, trsh_pct = 0.9):
taildf = tail_risk.tail_risk(data, freqs, alpha, right_side, dist)
print(taildf)
print(taildf.columns.tolist())
print(taildf.index.tolist())
print("ES=", taildf.at[0.95,'ES'])
print("VaR=", taildf.at[0.95,'VaR'])