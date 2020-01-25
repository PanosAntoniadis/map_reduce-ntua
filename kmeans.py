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
            float: the haversine distance of the two points 
  
    """
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = atan2(sqrt(a), sqrt(1-a))
    r = 6371 
    return c * r

def closest_centroid(point, centroids):
    """ 
        Computes the closest centroid from a given point.
 
        Parameters: 
            point (numpy.array): the coordinates of the given point
            centroids (list): contrains numpy arrays with the coordinates of the centroids

        Returns: 
            int: the index of the closest centroid
  
    """
    dists = []

    for centroid in centroids:
        dist = haversine(point[0], point[1], centroid[0], centroid[1])
        dists.append(dist)
	
    return dists.index(min(dists))


if __name__== "__main__":
    # Set up a Spark context
    conf = SparkConf().setAppName("k-means")
    sc = SparkContext(conf = conf)

    # Read data from hdfs
    #data = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
    data = sc.textFile("hdfs://master:9000/sample.csv")

    # Keap only pickup locations and convert them in a tuple of floats
    data = data.map(lambda line : np.array([float(coord) for coord in line.split(",")[3:5]]))


    for i in data.collect():
        print(i)

    k = 5
    MAX_ITERATIONS = 3

    # Initialize centroids
    centroids = data.take(5)
    iterations = 1
    print(centroids)

    while (iterations <= MAX_ITERATIONS):
        new_centroids = data.map(lambda point : (closest_centroid(point, centroids) , (point, 1)))

        new_centroids = new_centroids.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

        new_centroids = new_centroids.map(lambda line : (line[0], line[1][0]/line[1][1]))


        for (idx, new_centroid) in new_centroids.collect():
            centroids[idx] = new_centroid

        print(centroids)
        iterations += 1



