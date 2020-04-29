import os
import numpy as np
import pandas as pd


def euclidean_distance(M, v):
    """
    Compute the Euclidean distance
    
    Args:
    =====
    M: matrix of observations
    v: data point as a vector

    Return:
    =======
    The distances to v for each observation
    """
    assert M.shape[1] == v.shape[0]
    distance = np.sqrt(np.sum((M - v) ** 2, axis=1))
    return distance


def kmeans(M, k, iterations=20):
    """
    Implement k-means clustering with Euclidean distance

    Args:
    =====
    M:          matrix of observations
    k:          number of clusters
    iterations: max number of iterations k-means algorithm to run

    Return:
    =======
    centoids:   the centroids of the clusters
    clusters:   cluster id for each observation
    distances:  distances to centroids for each observation
    """
    # initialise the centroids
    pos = np.random.choice(np.arange(M.shape[0]), size=k, replace=False)
    centroids = M[pos, :]

    # initialise the distances matrix
    distances = np.zeros((M.shape[0], centroids.shape[0]))
    prev_distances = distances.copy()

    for _ in range(iterations):
        # calculate the distance for each observation
        for i in range(centroids.shape[0]):
            distances[:, i] = euclidean_distance(M, centroids[i, :])
        if np.array_equal(distances, prev_distances):
            break
        del prev_distances
        prev_distances = distances.copy()

        # assign observation to the cluster
        clusters = np.argmin(distances, axis=1)

        # update the centroids
        for i in range(centroids.shape[0]):
            centroids[i, :] = np.mean(M[clusters==i, :], axis=0)

    return centroids, clusters, distances


if __name__ == '__main__':
    file_path = './k-means/data/ABCBank.csv'
    raw_df = pd.read_csv(file_path)
    input_matrix = raw_df[['Age', 'Income', 'CCAvg', 'Mortgage']].values
    centers, clusters, distances = kmeans(input_matrix, k=3)