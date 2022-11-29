package gr.uth.ece.dsel.hadoop_prepartitioning.phase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver1 extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver1(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.println("Usage: Driver1 <input path> <output path> <reducers>");
			System.exit(-1);
		}
		
		// Create configuration
		Configuration conf = new Configuration();
		 
		// Create job
		Job job = Job.getInstance(conf, "MapReduce1");
		job.setJarByClass(this.getClass());
 
		// Setup MapReduce job
		job.setMapperClass(Mapper1.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
 
		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
 
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
