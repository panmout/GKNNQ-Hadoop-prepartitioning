###########################################################################
#                             PARAMETERS                                  #
###########################################################################

partitioning=qt	    # gd or qt
mode=bf                       # bf or ps
phase15=centroid                    # mbr or centroid
heuristics=true                # true or false
fastSums=true                 # true or false
K=10
reducers=2
nameNode=panagiotis-lubuntu
N=100
treeFile=qtree.ser
treeDir=sampletree
trainingDir=input
queryDir=input
queryDataset=query-dataset.txt
sortedQueryFile=qpoints_sorted.ser
trainingDataset=NApppointNNew.txt
mbrCentroidFile=mbrcentroid.txt
overlapsFile=overlaps.txt
gnnDir=gnn
gnn25File=gnn2_5.txt
mr_partition=mr_partition
mr1outputPath=mapreduce1
mr2outputPath=mapreduce2
mr3outputPath=mapreduce3

###########################################################################
#                                    EXECUTE                              #
###########################################################################

hadoop jar ./target/gknn-hadoop-prepartitioning-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop_prepartitioning.main.Gnn \
partitioning=$partitioning \
mode=$mode \
phase15=$phase15 \
heuristics=$heuristics \
fastSums=$fastSums \
K=$K \
reducers=$reducers \
nameNode=$nameNode \
N=$N \
treeFile=$treeFile \
treeDir=$treeDir \
trainingDir=$trainingDir \
queryDir=$queryDir \
queryDataset=$queryDataset \
sortedQueryFile=$sortedQueryFile \
trainingDataset=$trainingDataset \
mbrCentroidFile=$mbrCentroidFile \
overlapsFile=$overlapsFile \
gnnDir=$gnnDir \
gnn25File=$gnn25File \
mr_partition=$mr_partition \
mr1outputPath=$mr1outputPath \
mr2outputPath=$mr2outputPath \
mr3outputPath=$mr3outputPath \
