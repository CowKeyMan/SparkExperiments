def file_to_matrix(file_name):
    matrix = []
    with open(file_name, 'r') as f:
        for line in f:
            line = line[:-1]
            line = line.split()
            line = [float(x) for x in line]
            matrix.append(line)
    return matrix


def matrix_transpose(a):
    result = []
    for i in range(len(a[0])):
        result.append([r[i] for r in a])
    return result


def matrix_multiply(a, b):
    result = []
    for row in a:
        new_row = []
        for i in range(len(b[0])):
            new_row.append(
                sum([a_elem * b[j][i] for j, a_elem in enumerate(row)])
            )
        result.append(new_row)
    return result


def check(file_name, expected):
    matrix = file_to_matrix(file_name)
    actual = matrix_multiply(matrix, matrix_transpose(matrix))
    actual = matrix_multiply(actual, matrix)
    truths = []
    for a, e in zip(actual, expected):
        truths += [round(x, 5) == round(y, 5) for x, y in zip(a, e)]
    assert sum(truths) == len(truths)
