# Hadoop Implementation for SON (Savasere, Omiecinski, Navathe) Algorithm

### Introduction
We all have our secret shopping lists. If you were a store owner, you would definitely have a strong desire to knowi every customer's list. In that case, you can come up with many ways to boost sales. E.g., putting those products customers frequently buy together on the same shelf at the very entrance of the store.
Picture the items that a customer buy on one day are in a *basket*, so each basket consists of a set of *items* (an itemset). We want to find the *associate rules* between items. E.g. association rule ```{x, y, z} → a``` means someone buying **x**, **y** and **z** are very likely to buy **a** as well. These rules can actually be generated directly from *frequent itemsets*, where items appear in more than a certain number (*support*) of baskets. We usually assume the number of items in a basket is much smaller than the total number of items, and the number of baskets is too large to fit in main memory.
In this project, I implemented SON algorithm in Hadoop's MapReduce framework to find the association rules in Extended Bakery Dataset collected by Cal Poly.

### File Description


### SON (Savasere, Omiecinski, Navathe) Algorithm


### Implementation Techniques I used


### Usage

   ```shell
   ./SON <in> <out> <threshold> <filesize> <chunksize> <tuplesize>
   ```
Example:

   ```shell
   ./SON /user/root/data/data.txt output/out1 0.45 14 5 5
   ```
