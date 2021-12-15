import numpy as np

matrix = np.random.uniform(1, 100, (10, 30))

print(matrix)

print(matrix @ matrix.T)

a = matrix @ matrix.T
b = matrix.T @ matrix

c = a @ matrix
d = matrix.T @ a

print(np.all(c == d))

with open('random_matrix.txt', 'w') as f:
    for row in matrix:
        f.write(' '.join([str(n) for n in row]))
        f.write('\n')
