package gr.uth.ece.dsel.hadoop_prepartitioning.util.takeSample;

import gr.uth.ece.dsel.hadoop_prepartitioning.util.GnnFunctions;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.Point;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class SampleReducer extends Reducer<Text, Text, Text, Text>
{
	private ArrayList<Point> tpoints;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		// ascending comparator by id for Point objects (id, x, y)
		Comparator<Point> idComparator = Comparator.comparingInt(Point::getId);
		
		tpoints = new ArrayList<>();
		
		for (Text value: values)
			tpoints.add(GnnFunctions.stringToPoint(value.toString(), "\t"));
		
		tpoints.sort(idComparator);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		for (Point tp: tpoints)
		{
			context.write(null, new Text(String.format("%d\t%f\t%f", tp.getId(), tp.getX(), tp.getY())));
		}
	}
}
