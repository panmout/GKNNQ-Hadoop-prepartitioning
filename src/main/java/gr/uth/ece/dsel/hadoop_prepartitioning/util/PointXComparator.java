package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.util.Comparator;

public final class PointXComparator  implements Comparator<Point>
{
	private final int a;
	
	public PointXComparator(String type)
	{
		if (type.equals("min")) // x-ascending comparator for Point objects
			this.a = -1;
		else if (type.equals("max")) // x-descending comparator for Point objects
			this.a = 1;
		else throw new IllegalArgumentException("argument must be 'min' or 'max'");
	}
	
	@Override
	public int compare(Point element1, Point element2)
	{
		if (element1.getX() < element2.getX())
			return this.a;
		else if (element1.getX() == element2.getX())
			return 0;
		else
			return -this.a;
	}

}
