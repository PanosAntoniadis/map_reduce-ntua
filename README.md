# Clustering of Taxi Trip Data using Spark


## Dataset

The dataset is the [2015 Yellow Taxi Trip Data](https://data.cityofnewyork.us/Transportation/2015-Yellow-Taxi-Trip-Data/ba8s-jw6u). It includes trip records from all trips completed in yellow taxis from in NYC from January to June in 2015. Due to limited resources, we used only a 2 GB subset of the dataset. This subset contains 13m trip records and are available [here](http://www.cslab.ntua.gr/courses/atds/yellow_trip_data.zip). In this file, two comma-delimited text files (.csv) are available. The first contains all the necessary information about a route and the second contains information about the taxi vendors.


## Algorithm

The scope of the project is to find the coordinates of the top 5 pickup locations. In order to achieve this, we implemented the [K-means](https://en.wikipedia.org/wiki/K-means_clustering) with k=5, that clusters the pickup locations in five regions.
