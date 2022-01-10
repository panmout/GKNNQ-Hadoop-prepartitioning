package gr.uth.ece.dsel.hadoop_prepartitioning.phase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver3 extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver3(), args);
		System.exit(res);
	}
	
	// metrics variables
	protected enum Metrics { TOTAL_TPOINTS, CELLS_PROC };
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 18)
		{
			System.err.println("Usage: Driver3 <input path> <output path> <namenode name> <overlapsDir> <overlapsFileName> <queryDir> <queryFileName> <mbrCentroidDir> <mbrCentroidFileName> <gnn25Dir> <gnn25FileName> <heuristics> <fastSums> <N> <K> <partitioning> <mode> <reducers>");
			System.exit(-1);
		}
		// Create configuration
		Configuration conf = new Configuration();
		
		// Set custom args
		conf.set("namenode", args[2]);
		conf.set("overlapsDir", args[3]);
		conf.set("overlapsFileName", args[4]);
		conf.set("queryDir", args[5]);
		conf.set("queryFileName", args[6]);
		conf.set("mbrCentroidDir", args[7]);
		conf.set("mbrCentroidFileName", args[8]);
		conf.set("gnn25Dir", args[9]);
		conf.set("gnn25FileName", args[10]);
		conf.set("heuristics", args[11]);
		conf.set("fastSums", args[12]);
		conf.set("N", args[13]);
		conf.set("K", args[14]);
		conf.set("partitioning", args[15]);
		conf.set("mode", args[16]);
		
		
		// Create job
		Job job = Job.getInstance(conf, "MapReduce3");
		job.setJarByClass(Driver3.class);
		
		// Setup MapReduce job
		job.setMapperClass(Mapper3.class);
		job.setReducerClass(Reducer3.class);
		job.setNumReduceTasks(Integer.parseInt(args[17]));
		
		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		// use with snappy compression
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
