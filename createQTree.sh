###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=panagiotis-lubuntu
trainingDir=input
treeDir=sampletree
trainingDataset=NApppointNNew.txt
samplerate=100
capacity=10
type=1 # 1 for simple capacity based quadtree, 2 for all children split method, 3 for average width method

###########################################################################
#                                    EXECUTE                              # ###########################################################################

hadoop jar ./target/gknn-hadoop-prepartitioning-0.0.1-SNAPSHOT.jar \
gr.uth.ece.dsel.hadoop_prepartitioning.preliminary.Qtree \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity \
type=$type
