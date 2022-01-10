package gr.uth.ece.dsel.hadoop_prepartitioning.util.takeSample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashSet;
import java.util.Random;
import java.io.IOException;

public class SampleMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private int samplerate;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		HashSet<Integer> randomNumbers = new HashSet<Integer>(samplerate); // [percentSample] size set for random integers
		
		Random random = new Random();
		
		while (randomNumbers.size() < samplerate) // fill list
			randomNumbers.add(random.nextInt(100)); // add a random integer 0 - 99
		
		// read training dataset and get sample points
		
		if (randomNumbers.contains(random.nextInt(100))) // [percentSample]% probability
			context.write(new Text("key"), value);	
	}
	
	@Override
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		samplerate = Integer.parseInt(conf.get("samplerate"));
	}
}
