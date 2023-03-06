package gr.uth.ece.dsel.hadoop_prepartitioning.phase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class Reducer2 extends Reducer<Text, Text, Text, Text>
{
	private int K; // user defined (k-nn)
	private PriorityQueue<IdDist> neighbors; // max heap of K neighbors
	private String mode; // bf or ps
	private BfNeighbors bfn;
	private PsNeighbors psn;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//String cell = key.toString(); // key is cell_id (mappers' output) - not needed

		// list containing training points
		ArrayList<Point> tpoints = new ArrayList<>(); // list of tpoints in this cell

		for (Text value: values) // run through value of mapper output
		{
			String line = value.toString(); // read a line

			String[] data = GnnFunctions.stringToArray(line, "\t");
			
			int pid = 0;
			double x = 0;
			double y = 0;
			
			for (int i = 0; i < data.length; i += 3)
			{
				pid = Integer.parseInt(data[i]);
				x = Double.parseDouble(data[i + 1]);
				y = Double.parseDouble(data[i + 2]);
				tpoints.add(new Point(pid, x, y)); // create point and add to tpoints list
			}
		}
		
		// set TOTAL_POINTS metrics variable
		context.getCounter(Metrics.TOTAL_TPOINTS).increment(tpoints.size());
		
		// max heap of K neighbors (IdDist)
		if (mode.equals("bf"))
		{
			bfn.setTpoints(tpoints);
			neighbors.addAll(bfn.getNeighbors());
		}
		else if (mode.equals("ps"))
		{
			psn.setTpoints(tpoints);
			neighbors.addAll(psn.getNeighbors());
		}
		else
			throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		// write output
		// outKey = null
		// outValue is {tpoint1_id, dist1, tpoint2_id, dist2,...,tpointK_id, distK}
		
		PriorityQueue<IdDist> neighbors2 = new PriorityQueue<>(K, new IdDistComparator("min")); // min heap
		
		while (!neighbors.isEmpty())
		{
			IdDist neighbor = neighbors.poll();
			if (!GnnFunctions.isDuplicate(neighbors2, neighbor))
				neighbors2.add(neighbor);
		}
		
		String outValue = (neighbors2.isEmpty()) ? null: GnnFunctions.pqToString(neighbors2, K, "min"); // get PQ as String
		
		if (outValue != null)
			context.write(null, new Text(outValue));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		K = Integer.parseInt(conf.get("K"));

		// hostname
		String hostname = conf.get("namenode"); // get namenode name
		// username
		String username = System.getProperty("user.name"); // get user name

		// HDFS dir containing query file
		String queryDatasetDir = conf.get("queryDir"); // get query dataset dir
		// query file name in HDFS
		String queryDatasetFileName = conf.get("queryFileName"); // get query dataset filename
		// full HDFS path to query file
		String queryDatasetFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, queryDatasetDir, queryDatasetFileName); // full HDFS path to query dataset file

		// HDFS dir containing mbrCentroid file
		String mbrCentroidDir = conf.get("mbrCentroidDir"); // get mbrCentroid dir
		// mbrCentroid file name in HDFS
		String mbrCentroidFileName = conf.get("mbrCentroidFileName"); // get mbrCentroid filename
		// full HDFS path to tree file
		String mbrCentroidFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, mbrCentroidDir, mbrCentroidFileName); // full HDFS path to mbrCentroid file

		neighbors = new PriorityQueue<>(K, new IdDistComparator("max")); // max heap of K neighbors

		// break sumDist loops on (true) or off (false)
		boolean fastSums = conf.getBoolean("fastSums", false); // default : false (normal mode)

		mode = conf.get("mode");
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration

		// array of doubles to put MBR, centroid, sumdist(centroid, Q)
		double[] mbrC = ReadHdfsFiles.getMbrCentroid(mbrCentroidFile, fs); // read mbrCentroid array

		PriorityQueue<IdDist> emptyneighbors = new PriorityQueue<>(K, new IdDistComparator("max"));

		// list containing query points
		ArrayList<Point> qpoints;
		if (mode.equals("bf"))
		{
			qpoints = new ArrayList<>(ReadHdfsFiles.getQueryPoints(queryDatasetFile, fs)); // read querypoints
			bfn = new BfNeighbors(K, mbrC, qpoints, emptyneighbors, fastSums, context);
		}
		else if (mode.equals("ps"))
		{
			qpoints = new ArrayList<>(ReadHdfsFiles.getSortedQueryPoints(queryDatasetFile, fs)); // read querypoints
			psn = new PsNeighbors(K, mbrC, qpoints, emptyneighbors, fastSums, context);
		}
		else
			throw new IllegalArgumentException("mode arg must be 'bf' or 'ps'");
	}
}
