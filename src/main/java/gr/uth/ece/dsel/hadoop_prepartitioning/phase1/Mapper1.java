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

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString(); // read a line
		
		String[] data = GnnFunctions.stringToArray(line, "\t");

		String cell = data[0];

		int num_points = (data.length - 1) / 3;
		
		// increment cells number
		context.getCounter(Metrics.NUM_CELLS).increment(1);
		
		context.write(new Text(cell), new IntWritable(num_points));
	}
}
