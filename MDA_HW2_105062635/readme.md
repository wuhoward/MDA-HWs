# Hadoop Implementation of Pagerank

### File Description
  
  * **MDA_HW2_v2.pdf:** Problem Description
  * **PageRank.java:** Main program
  * **p2p-Gnutella04.txt:** Input file
  * **output20, output36：** Results for for 20 iterations and 36 iterations (reaching convergence)
  * **Report:** Summary of experimental result and conclusion

### Parameters in the Main Program

  * **NODES:** Total number of nodes (default: 10876)
  * **BET:** β (default: 0.8)
  * **TOPN:** Keep Top-N vertices sorted by rank (default: 10)
  * **MAXROUND:** max iterations to run (default: 100)
  * **PRECISION:** number of fractional digits;enter 0 for double precision (default: 0)

### Compilation
  ```shell
  yarn jar target/wordcountjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.PageRank /user/root/data/p2p-Gnutella04.txt output/out1
  ```

