datasetDir=input
datasetName=NApppointNNew.txt
outputPath=datasetSample
samplerate=20
reducers=1

hadoop jar ./target/gknn-hadoop-prepartitioning-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.hadoop_prepartitioning.util.takeSample.SampleDriver $datasetDir/$datasetName $outputPath $samplerate $reducers
