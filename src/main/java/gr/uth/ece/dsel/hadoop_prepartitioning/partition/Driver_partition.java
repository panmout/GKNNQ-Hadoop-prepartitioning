package gr.uth.ece.dsel.hadoop_prepartitioning.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver_partition extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Driver_partition(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 8)
		{
			System.err.println("Usage: Driver_partition <input path> <output path> <namenode name> <treeDir> <treeFileName> <N> <partitioning> <reducers>");
			System.exit(-1);
		}
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Set custom args
		conf.set("namenode", args[2]);
		conf.set("treeDir", args[3]);
		conf.set("treeFileName", args[4]);
		conf.set("N", args[5]);
		conf.set("partitioning", args[6]);
		
		// Create job
		Job job = Job.getInstance(conf, "Map_partition");
		job.setJarByClass(this.getClass());
		
		/*
		// Compression (Snappy)
		conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
		job.setOutputFormatClass(SequenceFileOutputFormat.class); 
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		*/
		
		// Setup MapReduce job
		job.setMapperClass(Mapper_partition.class);
		job.setReducerClass(Reducer_partition.class);
		job.setNumReduceTasks(Integer.parseInt(args[7]));
 
		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
 
		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
 
		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Compression (bzip2)
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		
		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
