package gr.uth.ece.dsel.hadoop_prepartitioning.phase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver2 extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver2(), args);
		System.exit(res);
	}
	
	// metrics variables
	protected enum Metrics { TOTAL_TPOINTS };
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 13)
		{
			System.err.println("Usage: Driver2 <input path> <output path> <namenode name> <overlapsDir> <overlapsFileName> <queryDir> <queryFileName> <mbrCentroidDir> <mbrCentroidFileName> <fastSums> <K> <mode> <reducers>");
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
		conf.set("fastSums", args[9]);
		conf.set("K", args[10]);
		conf.set("mode", args[11]);
		
		// Create job
		Job job = Job.getInstance(conf, "MapReduce2");
		job.setJarByClass(Driver2.class);
		
		// Setup MapReduce job
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setNumReduceTasks(Integer.parseInt(args[12]));
		
		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
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
