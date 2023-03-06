package gr.uth.ece.dsel.hadoop_prepartitioning.preliminary;

import gr.uth.ece.dsel.hadoop_prepartitioning.util.Point;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.PointXComparator;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.ReadHdfsFiles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public final class SortQueryPoints
{
	private static String nameNode; // hostname
	private static String queryDir; // HDFS dir containing query dataset
	private static String queryDataset; // query dataset name in HDFS
	private static FileOutputStream fileout;
	private static ObjectOutputStream outputObjectFile; // local output object file

	public static void main(String[] args)
	{
		long t0 = System.currentTimeMillis();
		
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

		// username
		String username = System.getProperty("user.name");
		// full HDFS path+name of query dataset
		String queryDatasetPath = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, queryDir, queryDataset);
		// list containing query points
		ArrayList<Point> qpoints = new ArrayList<>();
		// output file name
		String outputFileName = "qpoints_sorted.ser";
		// full hdfs path name
		String outputFilePath = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, queryDir, outputFileName);
		
		long t1 = System.currentTimeMillis();
		
		try // open files
		{
			fileout = new FileOutputStream(outputFileName);
			outputObjectFile = new ObjectOutputStream(fileout); // open local output object file
			
			FileSystem fs = FileSystem.get(new Configuration());
			qpoints = new ArrayList<>(ReadHdfsFiles.getQueryPoints(queryDatasetPath, fs)); // read querypoints
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
		
		long t2 = System.currentTimeMillis();
		
		// sort list by x ascending
		qpoints.sort(new PointXComparator("min"));
		
		long t3 = System.currentTimeMillis();
		
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
		
		long t4 = System.currentTimeMillis();
		
		long totalTime = System.currentTimeMillis() - t0;
		
		System.out.printf("Total time: %d millis\n", totalTime);
		System.out.printf("Read from HDFS time: %d millis\n", t2 - t1);
		System.out.printf("Sorting time: %d millis\n", t3 - t2);
		System.out.printf("Write to HDFS time: %d millis\n", t4 - t3);
	}
}
