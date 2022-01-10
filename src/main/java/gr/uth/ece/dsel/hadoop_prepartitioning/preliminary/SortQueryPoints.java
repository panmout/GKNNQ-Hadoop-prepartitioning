package gr.uth.ece.dsel.hadoop_prepartitioning.preliminary;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;

public final class SortQueryPoints
{
	private static String nameNode; // hostname
	private static String username; // username
	private static String queryDir; // HDFS dir containing query dataset
	private static String queryDataset; // query dataset name in HDFS
	private static String queryDatasetPath; // full HDFS path+name of query dataset
	private static FileOutputStream fileout;
	private static ObjectOutputStream outputObjectFile; // local output object file
	private static ArrayList<Point> qpoints; // list containing query points
	private static String outputFileName; // output file name
	private static String outputFilePath; // full hdfs path name
	
	public static void main(String[] args)
	{
		Long t0 = System.currentTimeMillis();
		
		for (String arg: args)
		{
			String[] newarg;
			if (arg.contains("="))
			{
				newarg = arg.split("=");
				
				if (newarg[0].equals("nameNode"))
					nameNode = newarg[1];
				if (newarg[0].equals("queryDir"))
					queryDir = newarg[1];
				if (newarg[0].equals("queryDataset"))
					queryDataset = newarg[1];
			}
			else
				throw new IllegalArgumentException("not a valid argument, must be \"name=arg\", : " + arg);
		}
		
		username = System.getProperty("user.name");
		queryDatasetPath = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, queryDir, queryDataset);
		qpoints = new ArrayList<Point>();
		outputFileName = "qpoints_sorted.ser";
		outputFilePath = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, queryDir, outputFileName);
		
		Long t1 = System.currentTimeMillis();
		
		try // open files
		{
			fileout = new FileOutputStream(outputFileName);
			outputObjectFile = new ObjectOutputStream(fileout); // open local output object file
			
			FileSystem fs = FileSystem.get(new Configuration());
			qpoints = new ArrayList<Point>(ReadHdfsFiles.getQueryPoints(queryDatasetPath, fs)); // read querypoints
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
		
		Long t2 = System.currentTimeMillis();
		
		// sort list by x ascending
		Collections.sort(qpoints, new PointXComparator("min"));
		
		Long t3 = System.currentTimeMillis();
		
		// write to files
		try
		{
			// local
			outputObjectFile.writeObject(qpoints);
			
			outputObjectFile.close();
			fileout.close();
			
			// write to hdfs
			FileSystem fs = FileSystem.get(new Configuration());
			Path path = new Path(outputFilePath);
			ObjectOutputStream outputStream = new ObjectOutputStream(fs.create(path));
			outputStream.writeObject(qpoints);
			outputStream.close();
		}
		catch (IOException ioException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(2);
		}
		
		Long t4 = System.currentTimeMillis();
		
		Long totalTime = System.currentTimeMillis() - t0;
		
		System.out.printf("Total time: %d millis\n", totalTime);
		System.out.printf("Read from HDFS time: %d millis\n", t2 - t1);
		System.out.printf("Sorting time: %d millis\n", t3 - t2);
		System.out.printf("Write to HDFS time: %d millis\n", t4 - t3);
	}
}
