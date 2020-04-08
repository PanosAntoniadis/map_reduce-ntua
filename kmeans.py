from pyspark import SparkConf, SparkContext
from math import cos, sin, atan2, sqrt
import numpy as np


def haversine(lon1, lat1, lon2, lat2):
    """
        Computes the haversine distance between two given points.

        Parameters:
            lon1 (float): longitude of 1st point
            lat1 (float): latitude of 1st point
            lon2 (float): longitude of 2nd point
            lat2 (float): latitude of 2nd point

        Returns:
            float: the haversine distance of the two given points

    """

    # Define dl, dlat and the Earth radius */
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    r = 6371

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return c * r


def closest_centroid(point, centroids):
    """
        Computes the closest centroid from a given point.

        Parameters:
            point (numpy.array): the coordinates of the given point
            centroids (list): list of numpy arrays that contains the coordinates of the centroids

        Returns:
            int: the index of the closest centroid

    """

    dists = []
    for centroid in centroids:
        dist = haversine(point[0], point[1], centroid[0], centroid[1])
        dists.append(dist)

    return dists.index(min(dists))


if __name__ == "__main__":
    # Set up a Spark context
    conf = SparkConf().setAppName("kmeans")
    sc = SparkContext(conf=conf)

    # Read data from hdfs
    data = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")

    # Keep only pickup locations and convert them in a numpy array of floats
    data = data.map(lambda line: np.array(
        [float(coord) for coord in line.split(",")[3:5]]))

    # Clean the dataset from 'dirty' 0s and cache data to improve performance
    data = data.filter(lambda line: line[0] != 0 and line[1] != 0).cache()

    # Define parameters
    k = 5
    MAX_ITERATIONS = 3
    iterations = 1

    # Initialize centroids
    centroids = data.take(5)

    while (iterations <= MAX_ITERATIONS):
        # Map each point to the closest centroid
        new_centroids = data.map(lambda point: (
            closest_centroid(point, centroids), (point, 1)))

        # Sum the coordinates and the size of the points in each centroid
        new_centroids = new_centroids.reduceByKey(
            lambda x, y: (x[0] + y[0], x[1] + y[1]))

        # Compute the new centroids as the mean value of the coordinates of the points in each centroid
        new_centroids = new_centroids.map(
            lambda line: (line[0], line[1][0] / line[1][1]))

        # Update the centroids
        for (idx, new_centroid) in new_centroids.collect():
            centroids[idx] = new_centroid

            iterations += 1

# Save result in hdfs
centroids = sc.parallelize(centroids)
centroids.saveAsTextFile("hdfs://master:9000/kmeans.res")
