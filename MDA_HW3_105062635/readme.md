# Hadoop Implementation of K-means Clustering

### File Description

  * MDA_HW3.pdf: Problem Description
  * KMeansM.java: K-means using Manhattan distance
  * KMeansE.java: K-means using Euclidean distance
  * result.txt: **cost** for each round and **the distance matrix** for cluster centroids in the last round
  * Euclidean_c1, Euclidean_c2, Manhattan_c1, Manhattan_c2：Resulted 10 cluster centroids for each case
  * Report: Summary of experimental result and conclusion
  
※c1: 10 randon initial centroids

※c2: 10 initial centroids as far as possible

### Compilation
  ```shell
  yarn jar target/wordcountjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.KMeansE /user/root/data/data.txt /user/root/data/c2.txt output/out1
  ```
The last 3 arguments are input data points, initial centroids and output path, respectively.
