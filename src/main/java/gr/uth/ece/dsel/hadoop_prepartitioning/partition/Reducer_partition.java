package gr.uth.ece.dsel.hadoop_prepartitioning.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Reducer_partition extends Reducer<Text, Text, Text, Text>
{
	private final StringBuilder points = new StringBuilder();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		points.delete(0, points.length());
		String cell = key.toString();
		
		for (Text point: values)
			points.append(point.toString()).append("\t");
		
		context.write(new Text(cell), new Text(points.toString()));
	}
}
