import numpy as np

n = 10000
nos = (np.random.poisson(1, n) + np.random.uniform(n) + np.random.beta(1000, 4, n))

print(np.median(nos))

with open('numbers_random.txt', 'w') as f:
    str_nos = [str(n) + '\n' for n in nos]
    str_nos[-1] = str_nos[-1][:-1]
    f.writelines(str_nos)
