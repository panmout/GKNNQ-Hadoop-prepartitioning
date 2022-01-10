package gr.uth.ece.dsel.hadoop_prepartitioning.phase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;

public class Reducer3 extends Reducer<Text, Text, Text, Text>
{
	private int K; // user defined (k-nn)
	private String hostname; // hostname
	private String username; // username
	private String queryDatasetDir; // HDFS dir containing query file
	private String queryDatasetFileName; // query file name in HDFS
	private String queryDatasetFile; // full HDFS path to query file
	private String mbrCentroidDir; // HDFS dir containing mbrCentroid file
	private String mbrCentroidFileName; // mbrCentroid file name in HDFS
	private String mbrCentroidFile; // full HDFS path to tree file
	private String gnn25Dir; // HDFS dir containing gnn25 file
	private String gnn25FileName; // gnn25 file name in HDFS
	private String gnn25File; // full HDFS path to gnn25 file
	private ArrayList<Point> qpoints; // list containing query points
	private ArrayList<Point> tpoints; // list containing training points
	private double[] mbrC; // array of doubles to put MBR, centroid, sumdist(centroid, Q)
	private PriorityQueue<IdDist> neighbors2; // Phase 2.5 neighbors
	private PriorityQueue<IdDist> neighbors3; // new neighbors list
	private boolean fastSums; // break sumDist loops on (true) or off (false)
	private String mode; // bf or ps
	private BfNeighbors bfn;
	private PsNeighbors psn;
	private String line;
	private String[] data;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//int cell = key.get(); // key is cell_id (mappers' output)
		
		tpoints = new ArrayList<Point>(); // list of tpoints in this cell
		
		for (Text value: values) // run through value of mapper output
		{
			line = value.toString(); // read a line
			
			data = GnnFunctions.stringToArray(line, "\t");
			
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
			neighbors3.addAll(bfn.getNeighbors());
		}
		else if (mode.equals("ps"))
		{
			psn.setTpoints(tpoints);
			neighbors3.addAll(psn.getNeighbors());
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
		
		PriorityQueue<IdDist> neighbors = new PriorityQueue<IdDist>(K, new IdDistComparator("min")); // min heap
		
		while (!neighbors3.isEmpty())
		{
			IdDist neighbor = neighbors3.poll();
			if (!GnnFunctions.isDuplicate(neighbors, neighbor))
				neighbors.add(neighbor);
		}
		
		String outValue = (neighbors.isEmpty()) ? null: GnnFunctions.pqToString(neighbors, K, "min"); // get PQ as String
		
		if (outValue != null)
			context.write(null, new Text(outValue));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		K = Integer.parseInt(conf.get("K"));
		
		hostname = conf.get("namenode"); // get namenode name
		username = System.getProperty("user.name"); // get user name
		
		queryDatasetDir = conf.get("queryDir"); // get query dataset dir
		queryDatasetFileName = conf.get("queryFileName"); // get query dataset filename
		queryDatasetFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, queryDatasetDir, queryDatasetFileName); // full HDFS path to query dataset file
		
		mbrCentroidDir = conf.get("mbrCentroidDir"); // get mbrCentroid dir
		mbrCentroidFileName = conf.get("mbrCentroidFileName"); // get mbrCentroid filename
		mbrCentroidFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, mbrCentroidDir, mbrCentroidFileName); // full HDFS path to mbrCentroid file
		
		gnn25Dir = conf.get("gnn25Dir"); // HDFS directory containing gnn25 file
		gnn25FileName = conf.get("gnn25FileName"); // get gnn25 filename
		gnn25File = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, gnn25Dir, gnn25FileName); // full HDFS path to gnn25 file
		
		fastSums = conf.getBoolean("fastSums", false); // default : false (normal mode)
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		mbrC = ReadHdfsFiles.getMbrCentroid(mbrCentroidFile, fs); // read mbrCentroid array
		
		neighbors2 = new PriorityQueue<IdDist>(K, new IdDistComparator("max"));
		neighbors2.addAll(ReadHdfsFiles.getPhase25Neighbors(gnn25File, fs, K)); // add Phase 2.5 neighbors
		
		neighbors3 = new PriorityQueue<IdDist>(K, new IdDistComparator("max"));
		
		mode = conf.get("mode");
		
		if (mode.equals("bf"))
		{
			qpoints = new ArrayList<Point>(ReadHdfsFiles.getQueryPoints(queryDatasetFile, fs)); // read querypoints
			bfn = new BfNeighbors(K, mbrC, qpoints, neighbors2, fastSums, context);
		}
		else if (mode.equals("ps"))
		{
			qpoints = new ArrayList<Point>(ReadHdfsFiles.getSortedQueryPoints(queryDatasetFile, fs)); // read querypoints
			psn = new PsNeighbors(K, mbrC, qpoints, neighbors2, fastSums, context);
		}
	}
}
