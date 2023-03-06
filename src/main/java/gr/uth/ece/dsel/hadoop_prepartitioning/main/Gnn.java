package gr.uth.ece.dsel.hadoop_prepartitioning.main;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Formatter;
import java.util.FormatterClosedException;

public class Gnn
{
	private static Formatter outputTextFile; // local output text file
	
	private static String partitioning; // grid or quadtree
	private static String mode; // bf or ps
	private static String phase15; // mbr or centroid
	private static String heuristics; // true or false
	private static String fastSums; // true or false
	private static String K;
	private static String reducers;
	private static String nameNode;
	private static String N;
	private static String treeFile;
	private static String treeDir;
	private static String trainingDir;
	private static String queryDir;
	private static String queryDataset;
	private static String sortedQueryFile;
	private static String trainingDataset;
	private static String mbrCentroidFile;
	private static String overlapsFile;
	private static String gnnDir;
	private static String gnn25File;
	private static String mr_partition;
	private static String mr1outputPath;
	private static String mr2outputPath;
	private static String mr3outputPath;
	
	public static void main(String[] args) throws Exception
	{
		try
		{
			outputTextFile = new Formatter(new FileWriter("results.txt", true)); // appendable file
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
		
		for (String arg: args)
		{
			String[] newarg;
			if (arg.contains("="))
			{
				newarg = arg.split("=");
				
				if (newarg[0].equals("partitioning"))
					partitioning = newarg[1];
				if (newarg[0].equals("mode"))
					mode = newarg[1];
				if (newarg[0].equals("phase15"))
					phase15 = newarg[1];
				if (newarg[0].equals("heuristics"))
					heuristics = newarg[1];
				if (newarg[0].equals("fastSums"))
					fastSums = newarg[1];
				if (newarg[0].equals("K"))
					K = newarg[1];
				if (newarg[0].equals("reducers"))
					reducers = newarg[1];
				if (newarg[0].equals("nameNode"))
					nameNode = newarg[1];
				if (newarg[0].equals("N"))
					N = newarg[1];
				if (newarg[0].equals("treeFile"))
					treeFile = newarg[1];
				if (newarg[0].equals("treeDir"))
					treeDir = newarg[1];
				if (newarg[0].equals("trainingDir"))
					trainingDir = newarg[1];
				if (newarg[0].equals("queryDir"))
					queryDir = newarg[1];
				if (newarg[0].equals("queryDataset"))
					queryDataset = newarg[1];
				if (newarg[0].equals("sortedQueryFile"))
					sortedQueryFile = newarg[1];
				if (newarg[0].equals("trainingDataset"))
					trainingDataset = newarg[1];
				if (newarg[0].equals("mbrCentroidFile"))
					mbrCentroidFile = newarg[1];
				if (newarg[0].equals("overlapsFile"))
					overlapsFile = newarg[1];
				if (newarg[0].equals("gnnDir"))
					gnnDir = newarg[1];
				if (newarg[0].equals("gnn25File"))
					gnn25File = newarg[1];
				if (newarg[0].equals("mr_partition"))
					mr_partition = newarg[1];
				if (newarg[0].equals("mr1outputPath"))
					mr1outputPath = newarg[1];
				if (newarg[0].equals("mr2outputPath"))
					mr2outputPath = newarg[1];
				if (newarg[0].equals("mr3outputPath"))
					mr3outputPath = newarg[1];
			}
			else
				throw new IllegalArgumentException("not a valid argument, must be \"name=arg\", : " + arg);
		}

		String trainingFile = String.format("%s/%s", trainingDir, trainingDataset);
		
		String queryFile; // text file query (bf) or sorted query file (ps)
		
		if (mode.equals("bf"))
			queryFile = queryDataset;
		else if (mode.equals("ps"))
			queryFile = sortedQueryFile;
		else
			throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
		
		if (!partitioning.equals("qt") && !partitioning.equals("gd"))
			throw new IllegalArgumentException("partitoning arg must be 'qt' or 'gd'");
		
		if (!phase15.equals("mbr") && !phase15.equals("centroid"))
			throw new IllegalArgumentException("phase15 args must be 'mbr' or 'centroid'");
		
		// execution starts
		long tstart = System.currentTimeMillis();
		
		String startMessage = String.format("GNN %s-%s using {%s method, heuristics:%s, fast sums:%s} starts\n", partitioning.toUpperCase(), mode.toUpperCase(), phase15, heuristics, fastSums);
		System.out.println(startMessage);
		writeToFile(outputTextFile, startMessage);
		
		// Phase 0
		// execute (<input path> <output path> <namenode name> <treeDir> <treeFileName> <N> <partitioning> <reducers>)
		String[] driver_partition_args = new String[] {trainingFile, mr_partition, nameNode, treeDir, treeFile, N, partitioning, reducers};
		new gr.uth.ece.dsel.hadoop_prepartitioning.partition.Driver_partition().run(driver_partition_args);
		
		long t0 = System.currentTimeMillis();
		String phase0Message = String.format("Phase 0 time: %d millis\n", t0 - tstart);
		System.out.println(phase0Message);
		writeToFile(outputTextFile, phase0Message);
		
		// Phase 1
		// execute (<input path> <output path>)
		String[] driver1args = new String[] {mr_partition, mr1outputPath, reducers};
		new gr.uth.ece.dsel.hadoop_prepartitioning.phase1.Driver1().run(driver1args);
		
		long t1 = System.currentTimeMillis();
		String phase1Message = String.format("Phase 1 time: %d millis\n", t1 - t0);
		System.out.println(phase1Message);
		writeToFile(outputTextFile, phase1Message);
		
		// Phase 1.5
		// execute (<namenode name> <MR1 hdfs dir name> <mbrCentroid hdfs dir> <mbrCentroid filename> <treeDir> <treeFileName> <N> <K> <phase15> <partitioning>)			
		String[] phase15args = new String[] {nameNode, mr1outputPath, gnnDir, mbrCentroidFile, treeDir, treeFile, N, K, phase15, partitioning};
		gr.uth.ece.dsel.hadoop_prepartitioning.phase1_5.Phase15.main(phase15args);
		
		long t15 = System.currentTimeMillis();
		String phase15Message = String.format("Phase 1.5 time: %d millis\n", t15 - t1);
		System.out.println(phase15Message);
		writeToFile(outputTextFile, phase15Message);
		
		// Phase 2
		// execute (<input path> <output path> <namenode name> <treeDir> <treeFileName> <overlapsDir> <overlapsFileName> <queryDir> <queryFileName> <mbrCentroidDir> <mbrCentroidFileName> <fastSums> <N> <K> <partitioning> <mode> <reducers>)
		String[] driver2args = new String[] {mr_partition, mr2outputPath, nameNode, gnnDir, overlapsFile, queryDir, queryFile, gnnDir, mbrCentroidFile, fastSums, K, mode, reducers};
		new gr.uth.ece.dsel.hadoop_prepartitioning.phase2.Driver2().run(driver2args);
		
		long t2 = System.currentTimeMillis();
		String phase2Message = String.format("Phase 2 time: %d millis\n", t2 - t15);
		System.out.println(phase2Message);
		writeToFile(outputTextFile, phase2Message);
				
		// Phase 2.5
		// execute (<namenode name> <MR2 hdfs dir name> <hdfs GNN dir name> <K>)
		String[] phase25GDargs = new String[] {nameNode, mr2outputPath, gnnDir, K};
		gr.uth.ece.dsel.hadoop_prepartitioning.phase2_5.Phase2_5.main(phase25GDargs);
		
		long t25 = System.currentTimeMillis();
		String phase25Message = String.format("Phase 2.5 time: %d millis\n", t25 - t2);
		System.out.println(phase25Message);
		writeToFile(outputTextFile, phase25Message);
		
		// Phase 3
		// execute (<input path> <output path> <namenode name> <overlapsDir> <overlapsFileName> <queryDir> <queryFileName> <mbrCentroidDir> <mbrCentroidFileName> <gnn25Dir> <gnn25FileName> <heuristics> <fastSums> <N> <K> <partitioning> <mode> <reducers>)
		String[] driver3args = new String[] {mr_partition, mr3outputPath, nameNode, gnnDir, overlapsFile, queryDir, queryFile, gnnDir, mbrCentroidFile, gnnDir, gnn25File, heuristics, fastSums, N, K, partitioning, mode, reducers};
		new gr.uth.ece.dsel.hadoop_prepartitioning.phase3.Driver3().run(driver3args);
		
		long t3 = System.currentTimeMillis();
		String phase3Message = String.format("Phase 3 time: %d millis\n", t3 - t25);
		System.out.println(phase3Message);
		writeToFile(outputTextFile, phase3Message);
		
		// Phase 3.5
		// execute (<namenode name> <MR3 hdfs dir name> <gnn25Dir> <GNN25filename> <K>)
		String[] phase35GDargs = new String[] {nameNode, mr3outputPath, gnnDir, gnn25File, K};
		gr.uth.ece.dsel.hadoop_prepartitioning.phase3_5.Phase3_5.main(phase35GDargs);
		
		long t35 = System.currentTimeMillis();
		String phase35Message = String.format("Phase 3.5 time: %d millis\n", t35 - t3);
		System.out.println(phase35Message);
		writeToFile(outputTextFile, phase35Message);
		
		String gdMessage = String.format("%s-%s using {%s method, heuristics:%s, fast sums:%s} time: %d millis\n", partitioning.toUpperCase(), mode.toUpperCase(), phase15, heuristics, fastSums, t35 - tstart);
		System.out.println(gdMessage);
		writeToFile(outputTextFile, gdMessage);
		
		outputTextFile.close();
	}
	
	private static void writeToFile(Formatter file, String s)
	{
		try
		{
			outputTextFile.format(s);
		}
		catch (FormatterClosedException formatterException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(1);
		}
	}
}
