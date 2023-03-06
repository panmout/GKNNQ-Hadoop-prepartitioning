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
	private HashSet<String> overlaps;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line

		String[] data = GnnFunctions.stringToArray(line, "\t");

		String cell = data[0]; // first element is cell

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

		// hostname
		String hostname = conf.get("namenode"); // get namenode name
		// username
		String username = System.getProperty("user.name"); // get user name

		FileSystem fs = FileSystem.get(conf); // get filesystem type from configuration

		// HDFS dir containing overlaps file
		String overlapsDir = conf.get("overlapsDir"); // HDFS directory containing overlaps file
		// overlaps file name in HDFS
		String overlapsFileName = conf.get("overlapsFileName"); // get overlaps filename
		// full HDFS path to overlaps file
		String overlapsFile = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, overlapsDir, overlapsFileName); // full HDFS path to overlaps file

		overlaps = new HashSet<>(ReadHdfsFiles.getOverlaps(overlapsFile, fs)); // read overlaps
	}
}
