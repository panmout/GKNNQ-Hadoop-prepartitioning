# execute (<namenode name> <hdfs dir name> <query dataset> <hdfs GNN dir name> <step> <mindist> <counter_limit> <pointdist>)

hadoop jar ./target/hadoop-prepartitioning-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop_prepartitioning.preliminary.MbrCentroid \
nameNode=Hadoopmaster \
queryDir=input \
queryDataset=linearwaterNNew_sub_2.8M.txt \
gnnDir=gnn \
step=0.000001 \
minDist=10 \
counter=100 \
diff=0.000001
