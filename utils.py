import numpy as np


def get_pivot(total_count, left_count, right_count, hist):
    true_mid = total_count / 2
    bucket_points = np.array(hist[0])
    bucket_counts = np.array(hist[1])
    sample_count = sum(bucket_counts)
    if sample_count == 0:
        return bucket_points[round(len(bucket_points) / 2)], "left"
    bucket_counts = \
        bucket_counts / sample_count \
        * (total_count - left_count - right_count)
    predicted_counts = left_count
    for i, bc in enumerate(bucket_counts):
        next_count = predicted_counts + bucket_counts[i]
        if next_count == true_mid:
            if sum(bucket_counts[:i + 1]) > sum(bucket_counts[i + 1:]):
                return bucket_points[i + 1], "right"
            else:
                return bucket_points[i + 1], "left"
        if next_count > true_mid:
            if sum(bucket_counts[:i + 1]) > sum(bucket_counts[i:]):
                return bucket_points[i], "right"
            else:
                return bucket_points[i + 1], "left"
        predicted_counts = next_count
