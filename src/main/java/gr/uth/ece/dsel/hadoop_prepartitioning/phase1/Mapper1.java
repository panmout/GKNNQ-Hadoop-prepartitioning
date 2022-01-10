package gr.uth.ece.dsel.hadoop_prepartitioning.phase1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.GnnFunctions;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.Metrics;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private String cell;
	private int num_points;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line
		
		String[] data = GnnFunctions.stringToArray(line, "\t");
		
		cell = data[0];
		
		num_points = (data.length - 1) / 3;
		
		// increment cells number
		context.getCounter(Metrics.NUM_CELLS).increment(1);
		
		context.write(new Text(cell), new IntWritable(num_points));
	}
}
