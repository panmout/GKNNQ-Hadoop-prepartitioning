###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=panagiotis-lubuntu
trainingDir=input
treeDir=sampletree
trainingDataset=NApppointNNew.txt
samplerate=10
capacity=10

###########################################################################
#                                    EXECUTE                              # ###########################################################################

hadoop jar ./target/gknn-hadoop-prepartitioning-0.0.1-SNAPSHOT.jar \
gr.uth.ece.dsel.hadoop_prepartitioning.preliminary.QuadtreeArray \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity
