package gr.uth.ece.dsel.hadoop_prepartitioning.util.takeSample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SampleDriver extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new SampleDriver(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 4)
		{
			System.err.println("Usage: SampleDriver <input path> <output path> <samplerate> <reducers>");
			System.exit(-1);
		}
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Set custom args
		conf.set("samplerate", args[2]);
		
		// Create job
		Job job = Job.getInstance(conf, "TakeSample");
		job.setJarByClass(SampleDriver.class);
		
		// Setup MapReduce job
		job.setMapperClass(SampleMapper.class);
		job.setReducerClass(SampleReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		
		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
 
		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
