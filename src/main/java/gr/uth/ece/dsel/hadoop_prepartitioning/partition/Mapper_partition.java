package gr.uth.ece.dsel.hadoop_prepartitioning.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.GnnFunctions;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.Node;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.Point;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.ReadHdfsFiles;

public class Mapper_partition extends Mapper<LongWritable, Text, Text, Text>
{
	private String partitioning; // bf or qt
	private String hostname; // hostname
	private String username; // username
	private String treeDir; // HDFS dir containing tree file
	private String treeFileName; // tree file name in HDFS
	private String treeFile; // full HDFS path to tree file
	private Node root; // create root node
	private int N; // N*N cells
	private String line;
	private String cell;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		line = value.toString(); // read a line
		
		Point p = GnnFunctions.stringToPoint(line, "\t");
		
		if (partitioning.equals("qt")) // quadtree cell
			cell = GnnFunctions.pointToCellQT(p.getX(), p.getY(), root);
		else if (partitioning.equals("gd")) // grid cell
			cell = GnnFunctions.pointToCellGD(p, N);
		
		// output: {cell, training point}
		context.write(new Text(cell), new Text(line));
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		partitioning = conf.get("partitioning");
		
		if (partitioning.equals("qt"))
		{
			hostname = conf.get("namenode"); // get namenode name
			username = System.getProperty("user.name"); // get user name
			treeDir = conf.get("treeDir"); // HDFS directory containing tree file
			treeFileName = conf.get("treeFileName"); // get tree filename
			treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, treeDir, treeFileName); // full HDFS path to tree file
			FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
			
			root = ReadHdfsFiles.getTree(treeFile, fs);
		}
		else if (partitioning.equals("gd"))
			N = Integer.parseInt(conf.get("N")); // get N
	}
}
