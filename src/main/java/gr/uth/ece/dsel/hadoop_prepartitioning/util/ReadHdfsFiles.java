package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class ReadHdfsFiles
{
	private static BufferedReader reader;
	private static Path pt;
	private static String line;
	
	// read query dataset from hdfs
	public static final ArrayList<Point> getQueryPoints(String queryDatasetFile, FileSystem fs)
	{
		ArrayList<Point> qpoints = new ArrayList<Point>();
		
		try
		{
			pt = new Path(queryDatasetFile); // create path object from path string
			reader = new BufferedReader(new InputStreamReader(fs.open(pt))); // create reader object from java data stream object
			
			while ((line = reader.readLine())!= null) // while input has more lines
			{
				qpoints.add(GnnFunctions.stringToPoint(line, "\t")); // add point to list
			}
			reader.close(); // close file
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return qpoints;
	}
	
	// read sorted query dataset from hdfs
	public static final ArrayList<Point> getSortedQueryPoints(String sortedQueryFile, FileSystem fs)
	{
		ArrayList<Point> qpoints = new ArrayList<Point>();
		
		try
		{
			pt = new Path(sortedQueryFile); // create path object from path string
			ObjectInputStream input = new ObjectInputStream(fs.open(pt)); // open HDFS sortedQuery file
			qpoints.addAll((ArrayList<Point>) input.readObject()); // assign sortedQuery binary form to ArrayList<Point>
		}
		catch (ClassNotFoundException classNotFoundException)
		{
			System.err.println("Invalid object type");
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return qpoints;
	}
	
	// read mbrCentroid array from hdfs
	public static final double[] getMbrCentroid(String mbrCentroidFile, FileSystem fs)
	{
		double[] mbrC = new double[7];
		
		try
		{
			pt = new Path(mbrCentroidFile); // create path object from path string
			reader = new BufferedReader(new InputStreamReader(fs.open(pt))); // create reader object from java data stream object
			line = reader.readLine();
			String[] data = line.trim().split("\t"); // split it at '\t'
			for (int i = 0; i < data.length; i++)
			{
				mbrC[i] = Double.parseDouble(data[i]); // read (xmin, xmax, ymin, ymax, xc, yc, sumdist_c_Q) and put into array
			}
			reader.close(); // close file
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return mbrC;
	}
	
	// read overlaps hashset from hdfs
	public static final HashSet<String> getOverlaps(String overlapsFile, FileSystem fs)
	{
		HashSet<String> overlaps = new HashSet<String>();
		
		try
		{
			// read overlaps output from hdfs
			pt = new Path(overlapsFile); // create path object from path string
			reader = new BufferedReader(new InputStreamReader(fs.open(pt))); // create reader object from java data stream object
			
			while ((line = reader.readLine())!= null) // while input has more lines
			{
				overlaps.add(line.trim()); // add cell to hashset
			}
			reader.close(); // close file
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return overlaps;
	}
	
	// read treefile from hdfs
	public static final Node getTree(String treeFile, FileSystem fs)
	{
		Node root = null;
		try
		{
			Path pt = new Path(treeFile); // create path object from path string
			ObjectInputStream input = new ObjectInputStream(fs.open(pt)); // open HDFS tree file
			root = (Node) input.readObject(); // assign quad tree binary form to root node
		}
		catch (ClassNotFoundException classNotFoundException)
		{
			System.err.println("Invalid object type");
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		return root;
	}
	
	// read mapreduce1 output from hdfs as hashmap
	public static final HashMap<String, Integer> getMR1output(String mr1OutFull, FileSystem fs)
	{
		HashMap<String, Integer> cell_tpoints = new HashMap<String, Integer>();
		
		try // open files
		{
			FileStatus[] status = fs.listStatus(new Path(mr1OutFull)); // FileStatus iterates through contents of hdfs dir
			
			// read mapreduce1 output from hdfs
			for (int i = 1; i < status.length; i++) // skipping status[0] = "_SUCCESS" file
			{
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()))); // create reader object from java data stream object
				
				while ((line = reader.readLine())!= null) // while input has more lines
				{
					String data[] = line.trim().split("\t");
					String cell = data[0]; // 1st element is point cell
					Integer num = Integer.parseInt(data[1]); // 2nd element is number of training points in cell
					cell_tpoints.put(cell, num); // add to hashmap
				}
				reader.close(); // close file
			}
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return cell_tpoints;
	}
	
	// read mapreduce 2 or 3 output from hdfs as max priority queue
	public static final PriorityQueue<IdDist> getPhase23Neighbors(String mr23OutFull, FileSystem fs, int k)
	{
		PriorityQueue<IdDist> neighbors = new PriorityQueue<IdDist>(k, new IdDistComparator("max"));
		
		try
		{
			fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(mr23OutFull)); // FileStatus iterates through contents of hdfs dir
			
			// read mapreduce 2 or 3 output from hdfs
			for (int i = 1; i < status.length; i++) // skipping status[0] = "_SUCCESS" file
			{
				reader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()))); // create reader object from java data stream object
				
				while ((line = reader.readLine())!= null) // while input has more lines
				{
					String data[] = line.trim().split("\t");
					for (int j = 0; j < data.length; j += 2) // mr2 output is {pid1, dist1, pid2, dist2,...,pidK, distK}
					{
						int pid = Integer.parseInt(data[j]); // 1st pair element is point id
						double dist = Double.parseDouble(data[j + 1]); // 2nd pair element is distance
						IdDist neighbor = new IdDist(pid, dist);
						if (neighbors.size() < k) // if queue is not full, add new tpoints 
						{
							neighbors.offer(neighbor);
						}
						else
						{
							// eliminate duplicates
							if (!GnnFunctions.isDuplicate(neighbors, neighbor))
							{
								double dm = neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
								if (dist < dm) // if new point distance is smaller than head's
								{
									neighbors.poll(); // remove top element
									neighbors.offer(neighbor);
								}
							}
						}
					}
				}
				reader.close(); // close file
			}
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return neighbors;
	}
	
	// read Phase 2.5 neighbors from hdfs as max priority queue
	public static final PriorityQueue<IdDist> getPhase25Neighbors(String gnn25File, FileSystem fs, int k)
	{
		PriorityQueue<IdDist> neighbors = new PriorityQueue<IdDist>(k, new IdDistComparator("max"));
		
		try
		{
			pt = new Path(gnn25File); // create path object from path string
			reader = new BufferedReader(new InputStreamReader(fs.open(pt))); // create reader object from java data stream object
			line = reader.readLine();
			String[] data = line.trim().split("\t"); // split it at '\t'
			for (int i = 0; i < data.length; i += 2) // gnn25 output is {pid1, dist1, pid2, dist2,...,pidK, distK}
			{
				int pid = Integer.parseInt(data[i]); // 1st pair element is point_id
				double dist = Double.parseDouble(data[i + 1]); // 2nd pair element is distance
				neighbors.offer(new IdDist(pid, dist)); // add to priority queue
			}
			reader.close(); // close file
		}
		catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
		}
		
		return neighbors;
	}
}
