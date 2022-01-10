package gr.uth.ece.dsel.hadoop_prepartitioning.phase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;
import org.apache.hadoop.fs.FileSystem;
import java.util.HashSet;
import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>
{
	private String hostname; // hostname
	private String username; // username
	private String overlapsDir; // HDFS dir containing overlaps file
	private String overlapsFileName; // overlaps file name in HDFS
	private String overlapsFile; // full HDFS path to overlaps file
	private HashSet<String> overlaps;
	private String line;
	private String[] data;
	private String cell;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		line = value.toString(); // read a line
		
		data = GnnFunctions.stringToArray(line, "\t");
		
		cell = data[0]; // first element is cell
			
		if (overlaps.contains(cell))
		{
			StringBuilder points = new StringBuilder();
			for (int i = 1; i < data.length; i++)
				points.append(data[i]).append("\t");
			
			context.write(new Text(cell), new Text(points.toString()));
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		hostname = conf.get("namenode"); // get namenode name
		username = System.getProperty("user.name"); // get user name
		
		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration
		
		overlapsDir = conf.get("overlapsDir"); // HDFS directory containing overlaps file
		overlapsFileName = conf.get("overlapsFileName"); // get overlaps filename
		overlapsFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, overlapsDir, overlapsFileName); // full HDFS path to overlaps file
		
		overlaps = new HashSet<String>(ReadHdfsFiles.getOverlaps(overlapsFile, fs)); // read overlaps
	}
}
