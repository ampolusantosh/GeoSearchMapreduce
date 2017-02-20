# geo search mapreduce
Map Reduce code to map given latitude longitude values to nearest latitude longitude values in a given set using Map side join of two data sets.

This takes two sets of data as input. A master set of latitude longitude values, and a set of latitude longitudes which are to be searched and mapped to this master set.

Assumption: The master data set can be cached in memory, thus not very large. This data set is already sorted based on latitudes and then optionally on longitudes.
The master set has all unique coordinates.

The two sets of coordinates are mapped based on the distance calculated by using the Haversine formula. The max limits considered as near can be specified in the Job Conf as follows:
maxDistMiles - maximum distance that is considered as near (in miles) [used 30 here]
latLimit, lonLimit - To increase efficiency, the area in which the actual distances are calculated is narrowed down by using these limits. Eg: 0.5 latLimit means the distance of points beyond searchLat+-0.5 are ignored, since 0.5 degree means 34.5 miles approx, the mapped lat lon values would not lie beyond this range. Similarly for longitude, till 83 deg latitude the values for 30 miles is 3.9 degree


Geosearch in databases and SQL queries (if written in Hive) use theta joins which are not supported in Hive, thus forcing cross products. This map reduce code reduces the need for a cross product using a map side join.

