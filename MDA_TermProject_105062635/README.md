# Hadoop Implementation for SON (Savasere, Omiecinski, Navathe) Algorithm

### Introduction
We all have our secret shopping lists. If you were a store owner, you would definitely have a strong desire to know everyone's. In that case, you can come up with many ways to boost sales. E.g., putting those products customers frequently buy together on the same shelf at the very entrance of the store.
View the items that a customer buy in a day as they are in a *basket*, so each basket consists of a set of items (an *itemset*). The goal of a store owner is to find the *associate rules* between items. E.g. association rule ```{x, y, z} → a``` means someone buying **x**, **y** and **z** are very likely to buy **a** as well. These rules can be generated directly from *frequent itemsets*, where items appear in more than a **s** (*support*) baskets. We usually assume the number of items in a basket is much smaller than the total number of items, and the number of baskets is too large to fit in main memory.
In this project, I implemented SON algorithm in Hadoop's MapReduce framework to find the association rules in Extended Bakery Dataset collected by Cal Poly.

### File Description

   * **input/**: Input files with different dataset sizes
   * **output/**: Output files of frequent itemsets and association rules
   * **SON.java**: Main Program
   * **MultiLineInputFormat.java**: A customized class for file input format inherited from NLineInputFormat
   * **MDA_TermProject_team29.pdf**: Report for this project
   
   
### Apriori Algorithm

   1. **Pass 1**: Read baskets and count in main memory the occurrences of each individual item, where items that appear **≥s** times are the frequent items. 
   2. **Pass 2**: Read baskets again and count in main memory only those pairs where both elements are frequent.
   3. Iteratively repeat step 2. to construct frequent itemset of size **k** from the information from the pass for **k-1**.

### SON (Savasere, Omiecinski, Navathe) Algorithm

   1. **1st Map()**: Take the assigned subset of the baskets and find the itemsets frequent in the subset. Lower the support threshold from **s** to **ps**, where **p** is the threshold frequency in **\[0, 1]**.
   2. **1st Reduce()**: Simply produce the summary of candidate itemsets.
   3. **2nd Map()**: Counts the number of occurrences of the candidate itemsets among the portion of baskets. 
   4. **2nd Reduce()**: Sum up the associated values for each candidate itemset. The result is the total support. 

### Implementation Techniques I used

   * Used TreeSet, TreeMap instead of HashSet, HashMap to speed up the indexing.
   * Customized input format for map() function, so that it can process multiple lines at once instead of 1 line.
   * Used a efficient intermediate representation between two mapreduce jobs.
   * Overrode the setup() of the 2nd map() to preprocess the intermediate candidate itemsets.

### Usage

   Generate all assoication rules with itemsets appear more frequent than **threshold**, with a set size smaller than **tuplesize**. **filesize** denotes how many lines (baskets) are in the input files. Each Hadoop node is resposible for **chunksize** lines (baskets).
   ```shell
   ./SON <in> <out> <threshold> <filesize> <chunksize> <tuplesize>
   ```
Example:

   ```shell
   ./SON /user/root/data/data.txt output/out1 0.45 14 5 5
   ```
