package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.util.Comparator;

public final class PointYComparator  implements Comparator<Point>
{
	private final int a;
	
	public PointYComparator(String type)
	{
		if (type.equals("min")) // y-ascending comparator for Point objects
			this.a = -1;
		else if (type.equals("max")) // y-descending comparator for Point objects
			this.a = 1;
		else throw new IllegalArgumentException("argument must be 'min' or 'max'");
	}
	
	@Override
	public int compare(Point element1, Point element2)
	{
		if (element1.getY() < element2.getY())
			return this.a;
		else if (element1.getY() == element2.getY())
			return 0;
		else
			return -this.a;
	}

}
