package gr.uth.ece.dsel.hadoop_prepartitioning.preliminary;

import java.io.IOException;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;

import org.apache.hadoop.fs.FSDataOutputStream;

public final class MbrCentroid
{
	private static String nameNode; // hostname
	private static String username; // username
	private static String queryDir; // HDFS dir containing query dataset
	private static String queryDataset; // query dataset name in HDFS
	private static String queryDatasetPath; // full HDFS path+name of query dataset
	private static String gnnDir; // HDFS dir containing GNN files
	private static String mbrcentroidFileName; // mbrcentroid file name in HDFS
	private static Formatter outputTextFile; // local output text file
	private static ArrayList<Point> qPoints; // arraylist for query dataset point objects
	private static double step; // step size
	private static double minDist; // minimum sum of distances
	private static int counter_limit; // counter for exiting while loop
	private static double diff; // distance limit between previous and next (x, y) points
	
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
				if (newarg[0].equals("gnnDir"))
					gnnDir = newarg[1];
				if (newarg[0].equals("step"))
					step = Double.parseDouble(newarg[1]);
				if (newarg[0].equals("minDist"))
					minDist = Double.parseDouble(newarg[1]);
				if (newarg[0].equals("diff"))
					diff = Double.parseDouble(newarg[1]);
				if (newarg[0].equals("counter_limit"))
					counter_limit = Integer.parseInt(newarg[1]);
				
			}
			else
				throw new IllegalArgumentException("not a valid argument, must be \"name=arg\", : " + arg);
		}
		
		username = System.getProperty("user.name");
		queryDatasetPath = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, queryDir, queryDataset);
		mbrcentroidFileName = String.format("hdfs://%s:9000/user/%s/%s/mbrcentroid.txt", nameNode, username, gnnDir);
		
		double xmin = Double.POSITIVE_INFINITY; // MBR
		double xmax = Double.NEGATIVE_INFINITY;
		double ymin = Double.POSITIVE_INFINITY;
		double ymax = Double.NEGATIVE_INFINITY;
		double xc = 0; // centroid coordinates
		double yc = 0;
		double sumdist = 0; // sumdist(centroid, Q);
		
		try // open files and do math
		{
			FileSystem fs = FileSystem.get(new Configuration());
			
			outputTextFile = new Formatter("mbrcentroid.txt"); // open local output text file
			
			qPoints = new ArrayList<Point>(ReadHdfsFiles.getQueryPoints(queryDatasetPath, fs)); // read querypoints
			
			// calculate MBR, centroid coords
			
			double sumx = 0; // sums for centroid calculation
			double sumy = 0;
			
			for (Point p : qPoints)
			{
				sumx += p.getX();
				sumy += p.getY();
				
				xmin = Math.min(xmin, p.getX());
				xmax = Math.max(xmax, p.getX());
				ymin = Math.min(ymin, p.getY());
				ymax = Math.max(ymax, p.getY());
			}
			
			// initialization of x, y
			double xprev = sumx / qPoints.size();
			double yprev = sumy / qPoints.size();
						
			// counter for exiting while loop
			int counter = 0;
			
			// new-old (x, y) points distance
			double pdist = 0;
			
			// iteration (gradient descent)
			do
			{
				double xnext = xprev - step*thetaQx(xprev, yprev);
				double ynext = yprev - step*thetaQy(xprev, yprev);
				
				pdist = GnnFunctions.distance(xnext, ynext, xprev, yprev);
				
				xprev = xnext;
				yprev = ynext;
				
				sumdist = distcQ(xnext, ynext);
				
				System.out.printf("xnext = %f\t", xnext);
				System.out.printf("ynext = %f\t", ynext);
				System.out.printf("step = %f\t", step);
				System.out.printf("counter = %d\t", counter);
				System.out.printf("points_distance = %f\t", pdist);
				System.out.printf("distcQ = %f\n", sumdist);
				
				counter++;
			}
			// while sumdist is less than predifined minimum and counter less than limit and points distance greater than limit
			while (sumdist > minDist && counter < counter_limit && pdist > diff);
			
			// centroid coordinates
			xc = xprev;
			yc = yprev;
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
		
		// write to files
		try
		{
			// output: (MBR borders) xmin, xmax, ymin, ymax, (centroid) xc, yc, sumdist(centroid, Q)
			String output = String.format("%11.10f\t%11.10f\t%11.10f\t%11.10f\t%11.10f\t%11.10f\t%.10f\n", xmin, xmax, ymin, ymax, xc, yc, sumdist);
			outputTextFile.format(output);
			outputTextFile.close();
			
			// write to hdfs
			FileSystem fs = FileSystem.get(new Configuration());
			Path path = new Path(mbrcentroidFileName);
			FSDataOutputStream outputStream = fs.create(path);
			outputStream.writeBytes(output);
			outputStream.close();
		}
		catch (FormatterClosedException formatterException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(2);
		}
		catch (IOException ioException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(3);
		}
		
		Long totalTime = System.currentTimeMillis() - t0;
		
		System.out.printf("Total time: %d millis\n", totalTime);
	}
	
	public static final double thetaQx(double x, double y)
	{
		double sum = 0;
		
		for (Point q: qPoints)
		{
			double xi = q.getX();
			double yi = q.getY();
			
			sum += (x - xi)/GnnFunctions.distance(x, y, xi, yi);
		}
		return sum;
	}
	
	public static final double thetaQy(double x, double y)
	{
		double sum = 0;
		
		for (Point q: qPoints)
		{
			double xi = q.getX();
			double yi = q.getY();
			
			sum += (y - yi)/GnnFunctions.distance(x, y, xi, yi);
		}
		return sum;
	}
	
	public static final double distcQ (double x, double y)
	{
		double sum = 0;
		
		for (Point q: qPoints)
		{
			double xi = q.getX();
			double yi = q.getY();
			
			sum += GnnFunctions.distance(x, y, xi, yi);
		}
		return sum;
	}
}
