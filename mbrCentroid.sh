# execute (<namenode name> <hdfs dir name> <query dataset> <hdfs GNN dir name> <step> <mindist> <counter_limit> <pointdist>)

hadoop jar ./target/gknn-hadoop-prepartitioning-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop_prepartitioning.preliminary.MbrCentroid \
nameNode=panagiotis-lubuntu \
queryDir=input \
queryDataset=query-dataset.txt \
gnnDir=gnn \
step=0.000001 \
minDist=10 \
counter=100 \
diff=0.000001
