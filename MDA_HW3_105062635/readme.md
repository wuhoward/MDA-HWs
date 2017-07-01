KMeansM.java：使用Manhattan Distance計算的主程式
KMeansE.java：使用Euclidean Distance計算的主程式
result.txt：每種組合每一輪的cost，和最後10個cluster centroids兩兩之間的距離
Euclidean_c1, Euclidean_c2, Manhattan_c1, Manhattan_c2：各種組合最後得到的10個cluster centroids。
執行時使用如下指令：
yarn jar target/wordcountjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.KMeansE /user/root/data/data.txt /user/root/data/c2.txt output/out1
最後三個參數分別為所有資料點檔案、初始centroids檔案、輸出路徑