package gr.uth.ece.dsel.hadoop_prepartitioning.phase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import java.util.ArrayList;
import java.util.HashSet;
import java.io.IOException;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private String hostname; // hostname
	private String username; // username
	private String mbrCentroidDir; // HDFS dir containing mbrCentroid file
	private String mbrCentroidFileName; // mbrCentroid file name in HDFS
	private String mbrCentroidFile; // full HDFS path to tree file
	private String overlapsDir; // HDFS dir containing overlaps file
	private String overlapsFileName; // overlaps file name in HDFS
	private String overlapsFile; // full HDFS path to overlaps file
	private String gnn25Dir; // HDFS dir containing gnn25 file
	private String gnn25FileName; // gnn25 file name in HDFS
	private String gnn25File; // full HDFS path to gnn25 file
	private String queryDatasetDir; // HDFS dir containing query file
	private String queryDatasetFileName; // query file name in HDFS
	private String queryDatasetFile; // full HDFS path to query file
	private HashSet<String> overlaps; // intersected nodes from Phase 1.5 hdfs import
	private double[] mbrC; // array that contains MBR[0-3]-centroid[4-5] coords and sumdist(centroid, Q)[6]
	private double bestDist; // best GNN distance from Phase 2.5 output file
	private ArrayList<Point> qpoints; // arraylist for query dataset point objects
	private int N; // N*N cells
	private int K;
	private boolean heuristics; // heuristics on (true) or off (false)
	private boolean fastSums; // break sumDist loops on (true) or off (false) 
	private String mode; // bf or ps
	private String line;
	private String[] data;
	private String cell;
	private boolean bool;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		line = value.toString(); // read a line from MR1 output
		
		data = GnnFunctions.stringToArray(line, "\t");
		
		cell = data[0]; // we are only interested in cell_id
		
		bool = true; // pruning flag (true --> pass, false --> prune)
		
		if (!overlaps.contains(cell) && value.toString() != null) // continue only for cells not in overlaps and contains points
		{
			double x0 = 0; // cell's lower left corner coords, cell width
			double y0 = 0;
			double ds = 0;
			
			if (partitioning.equals("qt")) // quadtree cell
			{
				// calculate cell's coords
				for (int i = 0; i < cell.length(); i++) // check cellname's digits
				{
					switch(cell.charAt(i))
					{
						case '0':
							y0 += 1.0/Math.pow(2, i + 1); // if digit = 0 increase y0
							break;
						case '1':
							x0 += 1.0/Math.pow(2, i + 1); // if digit = 1 increase x0
							y0 += 1.0/Math.pow(2, i + 1); // and y0
							break;
						case '3':
							x0 += 1.0/Math.pow(2, i + 1); // if digit = 3 increase x0
							break;
					}
				}
				ds = 1.0 / Math.pow(2, cell.length()); // cell side length
				/* cell's lower left corner: x0, y0
				 *        upper left corner: x0, y0 + s
				 *        upper right corner: x0 + s, y0 + s
				 *        lower right corner: x0 + s, y0
				 */
			}
			else if (partitioning.equals("gd")) // grid cell
			{
				int intCell = Integer.parseInt(cell);
				
				// calculate cell's coords
		 		ds = 1.0/N; // interval ds
		 		int i = intCell % N; // get i
		 		int j = (intCell - i) / N; // get j
		 		
		 		/* cell's lower left corner: x0, y0
		 		 *        upper left corner: x0, y0 + ds
		 		 *        upper right corner: x0 + ds, y0 + ds
		 		 *        lower right corner: x0 + ds, y0
		 		 */
		 		
		 		x0 = i*ds;
		 		y0 = j*ds;
			}
			
			// check heuristics 1, 2, 3
			bool = GnnFunctions.heuristics123(x0, y0, ds, mbrC, qpoints, bestDist, fastSums, heuristics, context);
			
			
			// if boolean var is true, cell is not pruned and goes to output
			if (bool == true)
			{
				// increment cell counter
				context.getCounter(Metrics.CELLS_PROC).increment(1);
				
				StringBuilder points = new StringBuilder();
				for (int i = 1; i < data.length; i++)
					points.append(data[i]).append("\t");
				context.write(new Text(cell), new Text(points.toString()));
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		partitioning = conf.get("partitioning");
		
		N = Integer.parseInt(conf.get("N")); // get N
		
		hostname = conf.get("namenode"); // get namenode name
		username = System.getProperty("user.name"); // get user name
		
		overlapsDir = conf.get("overlapsDir"); // HDFS directory containing overlaps file
		overlapsFileName = conf.get("overlapsFileName"); // get overlaps filename
		overlapsFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, overlapsDir, overlapsFileName); // full HDFS path to overlaps file
		overlaps = new HashSet<String>();
		
		mbrCentroidDir = conf.get("mbrCentroidDir"); // HDFS directory containing mbrCentroid file
		mbrCentroidFileName = conf.get("mbrCentroidFileName"); // get mbrCentroid filename
		mbrCentroidFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, mbrCentroidDir, mbrCentroidFileName); // full HDFS path to mbrCentroid file
		mbrC = new double[7];
		
		gnn25Dir = conf.get("gnn25Dir"); // HDFS directory containing gnn25 file
		gnn25FileName = conf.get("gnn25FileName"); // get gnn25 filename
		gnn25File = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, gnn25Dir, gnn25FileName); // full HDFS path to gnn25 file
		
		queryDatasetDir = conf.get("queryDir"); // get query dataset dir
		queryDatasetFileName = conf.get("queryFileName"); // get query dataset filename
		queryDatasetFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, queryDatasetDir, queryDatasetFileName); // full HDFS path to query dataset file
		qpoints = new ArrayList<Point>();
		
		K = Integer.parseInt(conf.get("K")); // get K
		
		heuristics = conf.getBoolean("heuristics", true); // default heuristics on
		
		fastSums = conf.getBoolean("fastSums", false); // default : false (normal mode)
		
		mode = conf.get("mode");
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		overlaps = new HashSet<String>(ReadHdfsFiles.getOverlaps(overlapsFile, fs)); // read overlaps
		
		mbrC = ReadHdfsFiles.getMbrCentroid(mbrCentroidFile, fs); // read mbrCentroid array
		
		if (mode.equals("bf"))
			qpoints = new ArrayList<Point>(ReadHdfsFiles.getQueryPoints(queryDatasetFile, fs)); // read querypoints
		else if (mode.equals("ps"))
			qpoints = new ArrayList<Point>(ReadHdfsFiles.getSortedQueryPoints(queryDatasetFile, fs)); // read querypoints
		
		bestDist = ReadHdfsFiles.getPhase25Neighbors(gnn25File, fs, K).peek().getDist(); // k-th neighbor distance
	}
}
