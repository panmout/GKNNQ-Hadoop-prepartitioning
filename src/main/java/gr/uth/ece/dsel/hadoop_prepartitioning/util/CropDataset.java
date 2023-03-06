package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CropDataset
{	
	public static void main(String[] args)
	{
		String hostname = args[0]; // namenode name
		String username = System.getProperty("user.name");
		String datasetDir = args[1]; // HDFS directory containing dataset file
		String datasetFileName = args[2]; // dataset filename
		String datasetFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, datasetDir, datasetFileName); // full HDFS path to dataset file
		String outputDir = args[3]; // output dir
		String outputFileName = args[4]; // output filename
		String outputFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, outputDir, outputFileName); // full HDFS path to output dataset file
		
		// output MBR coordinates
		double xmin = Double.parseDouble(args[5]);
		double xmax = Double.parseDouble(args[6]);
		double ymin = Double.parseDouble(args[7]);
		double ymax = Double.parseDouble(args[8]);
		
		ArrayList<Point> points = new ArrayList<>();
		
		FileSystem fs;
		Path pt;
		
		// read and crop dataset
		try
		{
			fs = FileSystem.get(new Configuration());
			pt = new Path(datasetFile); // create path object from path string
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt))); // create reader object from java data stream object
			
			String line;
			while ((line = reader.readLine())!= null) // while input has more lines
			{
				Point newpoint = GnnFunctions.stringToPoint(line, "\t"); // read a line into point
				double x = newpoint.getX();
				double y = newpoint.getY();
				if (x >= xmin && x <= xmax && y >= ymin && y <= ymax) // check geometric conditions
					points.add(newpoint); // add point to list
			}
			reader.close(); // close file
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open FileSystem, exiting");
			System.exit(1);
		}
		
		// write cropped dataset to HDFS
		if (!points.isEmpty())
		{
			try
			{
				fs = FileSystem.get(new Configuration());
				pt = new Path(outputFile);
				FSDataOutputStream outputStream = fs.create(pt);
				for (Point p: points)
					outputStream.writeBytes(String.format("%d\t%16.15f\t%16.15f\n", p.getId(), p.getX(), p.getY()));
				outputStream.close();
			}
			catch (IOException ioException)
			{
				System.err.println("Error writing to file, exiting");
				System.exit(3);
			}
			
		}
		
	}
}
