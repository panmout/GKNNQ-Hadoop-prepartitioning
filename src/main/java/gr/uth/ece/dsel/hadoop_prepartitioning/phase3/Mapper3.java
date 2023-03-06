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
	private HashSet<String> overlaps; // intersected nodes from Phase 1.5 hdfs import
	private double[] mbrC; // array that contains MBR[0-3]-centroid[4-5] coords and sumdist(centroid, Q)[6]
	private double bestDist; // best GNN distance from Phase 2.5 output file
	private ArrayList<Point> qpoints; // arraylist for query dataset point objects
	private int N; // N*N cells
	private boolean heuristics; // heuristics on (true) or off (false)
	private boolean fastSums; // break sumDist loops on (true) or off (false)

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line from MR1 output

		String[] data = GnnFunctions.stringToArray(line, "\t");

		String cell = data[0]; // we are only interested in cell_id

		boolean bool; // pruning flag (true --> pass, false --> prune)

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
			if (bool)
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

		// hostname
		String hostname = conf.get("namenode"); // get namenode name
		// username
		String username = System.getProperty("user.name"); // get user name

		// HDFS dir containing overlaps file
		String overlapsDir = conf.get("overlapsDir"); // HDFS directory containing overlaps file
		// overlaps file name in HDFS
		String overlapsFileName = conf.get("overlapsFileName"); // get overlaps filename
		// full HDFS path to overlaps file
		String overlapsFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, overlapsDir, overlapsFileName); // full HDFS path to overlaps file
		overlaps = new HashSet<String>();

		// HDFS dir containing mbrCentroid file
		String mbrCentroidDir = conf.get("mbrCentroidDir"); // HDFS directory containing mbrCentroid file
		// mbrCentroid file name in HDFS
		String mbrCentroidFileName = conf.get("mbrCentroidFileName"); // get mbrCentroid filename
		// full HDFS path to tree file
		String mbrCentroidFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, mbrCentroidDir, mbrCentroidFileName); // full HDFS path to mbrCentroid file
		mbrC = new double[7];

		// HDFS dir containing gnn25 file
		String gnn25Dir = conf.get("gnn25Dir"); // HDFS directory containing gnn25 file
		// gnn25 file name in HDFS
		String gnn25FileName = conf.get("gnn25FileName"); // get gnn25 filename
		// full HDFS path to gnn25 file
		String gnn25File = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, gnn25Dir, gnn25FileName); // full HDFS path to gnn25 file

		// HDFS dir containing query file
		String queryDatasetDir = conf.get("queryDir"); // get query dataset dir
		// query file name in HDFS
		String queryDatasetFileName = conf.get("queryFileName"); // get query dataset filename
		// full HDFS path to query file
		String queryDatasetFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, queryDatasetDir, queryDatasetFileName); // full HDFS path to query dataset file
		qpoints = new ArrayList<>();

		int k = Integer.parseInt(conf.get("K")); // get K

		heuristics = conf.getBoolean("heuristics", true); // default heuristics on
		
		fastSums = conf.getBoolean("fastSums", false); // default : false (normal mode)

		// bf or ps
		String mode = conf.get("mode");
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		overlaps = new HashSet<>(ReadHdfsFiles.getOverlaps(overlapsFile, fs)); // read overlaps
		
		mbrC = ReadHdfsFiles.getMbrCentroid(mbrCentroidFile, fs); // read mbrCentroid array
		
		if (mode.equals("bf"))
			qpoints = new ArrayList<>(ReadHdfsFiles.getQueryPoints(queryDatasetFile, fs)); // read querypoints
		else if (mode.equals("ps"))
			qpoints = new ArrayList<>(ReadHdfsFiles.getSortedQueryPoints(queryDatasetFile, fs)); // read querypoints
		
		bestDist = ReadHdfsFiles.getPhase25Neighbors(gnn25File, fs, k).peek().getDist(); // k-th neighbor distance
	}
}
