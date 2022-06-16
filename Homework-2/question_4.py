import sys
import os
import json
from pyspark import SparkContext
from point import Point
import time

def init_centroids(path_to_centroids):
    initial_centroids = sc.textFile(path_to_centroids).map(Point).cache()
    return initial_centroids

def assign_point_to_centroid(p):
    min_dist = float("inf")
    centroids = centroids_broadcast.value # list of centroids. type: python list
    nearest_centroid = 0
    for i in range(len(centroids)):
        distance = p.distance(centroids[i])
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = i
    return (nearest_centroid, p)

def calculate_cost_func(centroid_assignment_rdd):
    centroids = centroids_broadcast.value
    centroid_assignment_list = centroid_assignment_rdd.collect()
    cost = 0
    for nearest_centroid,p in centroid_assignment_list:
        cost += p.distance(centroids[nearest_centroid])**2
    return cost

if __name__ == "__main__":
   
    # Set parameters 
    max_iterations = 20
    # read params
    with open('./conf') as config:
        params = json.load(config)["configuration"]

    # see conf file
    INPUT_PATH = params["input_path"]
    OUTPUT_COSTS_PATH = params["output_path"][params["initial_centers_option"]]["output_path_cost_values"]
    OUTPUT_CENTROIDS_PATH = params["output_path"][params["initial_centers_option"]]["output_path_centroids"]
    CENTROIDS_PATH = params["initial_centers_file_path"][params["initial_centers_option"]]
    
    sc = SparkContext(appName="hw2-k-means")
    sc.setLogLevel("ERROR")
    sc.addPyFile("./point.py") ## It's necessary, otherwise the spark framework doesn't see point.py

    print("="*50,"\nStarting K Means\n","="*50)

    # read points
    points = sc.textFile(INPUT_PATH).map(Point).cache()
    # read centroids
    initial_centroids = init_centroids(CENTROIDS_PATH)
    # Broadcast centroids for further use
    centroids_broadcast = sc.broadcast(initial_centroids.collect())
    cost_func_values = []

    for i in range(max_iterations):

        centroid_assignment_rdd = points.map(assign_point_to_centroid)
        cost_func_values.append(calculate_cost_func(centroid_assignment_rdd))
        centroid_sum_rdd = centroid_assignment_rdd.reduceByKey(lambda x, y: x.sum(y))
        centroids_rdd = centroid_sum_rdd.mapValues(lambda x: x.get_average_point()).sortByKey(ascending=True)
        new_centroids = [item[1] for item in centroids_rdd.collect()]
        centroids_broadcast = sc.broadcast(new_centroids)


    print("Iterations DONE. Writing final centroids and cost values at: ", OUTPUT_CENTROIDS_PATH, OUTPUT_COSTS_PATH)
    
    # Save final centroids and cost values in output folder
    with open(OUTPUT_CENTROIDS_PATH, "w") as f:
        for centroid in new_centroids:
            f.write(str(centroid) + "\n")
        f.close()
    with open(OUTPUT_COSTS_PATH,"w") as f:
        for cost in cost_func_values:
            f.write(str(cost)+"\n")
        f.close()
    print("DONE")