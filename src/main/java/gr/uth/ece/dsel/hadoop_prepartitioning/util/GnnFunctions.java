package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.ArrayList;
import java.util.PriorityQueue;

public final class GnnFunctions
{	
	// String to point
	public static Point stringToPoint(String line, String sep)
	{
		final String[] data = stringToArray(line, sep);
		final int id = Integer.parseInt(data[0]);
		final double x = Double.parseDouble(data[1]);
		final double y = Double.parseDouble(data[2]);
		return new Point(id, x, y);
	}
	
	// Split string to data array
	public static String[] stringToArray(String line, String sep)
	{
		return line.trim().split(sep);
	}
	
	// point to GD cell
	public static String pointToCellGD(Point p, int n)
	{
		/*
		Cell array (numbers inside cells are cell_id)

		    n*ds |---------|----------|----------|----------|----------|----------|--------------|
		         | (n-1)n  | (n-1)n+1 | (n-1)n+2 |          | (n-1)n+i |          | (n-1)n+(n-1) |
		(n-1)*ds |---------|----------|----------|----------|----------|----------|--------------|
		         |         |          |          |          |          |          |              |
		         |---------|----------|----------|----------|----------|----------|--------------|
		         |   j*n   |  j*n+1   |  j*n+2   |          |  j*n+i   |          |   j*n+(n-1)  |
		    j*ds |---------|----------|----------|----------|----------|----------|--------------|
		         |         |          |          |          |          |          |              |
		         |---------|----------|----------|----------|----------|----------|--------------|
		         |   2n    |   2n+1   |   2n+2   |          |   2n+i   |          |     3n-1     |
		    2*ds |---------|----------|----------|----------|----------|----------|--------------|
		         |    n    |    n+1   |    n+2   |          |    n+i   |          |     2n-1     |
		      ds |---------|----------|----------|----------|----------|----------|--------------|
		         |    0    |     1    |     2    |          |     i    |          |      n-1     |
		         |---------|----------|----------|----------|----------|----------|--------------|
		       0          ds         2*ds                  i*ds               (n-1)*ds          n*ds


		So, cell_id(i,j) = j*n+i
		*/
		
		final double ds = 1.0/n; // interval ds (cell width)
		final double x = p.getX();  // p.x
		final double y = p.getY();  // p.y
		final int i = (int) (x/ds); // i = (int) x/ds
		final int j = (int) (y/ds); // j = (int) y/ds
		final int cellId = j * n + i;
		return String.valueOf(cellId); // return cellId
	}
	
	// node to cell
	public static String nodeToCell(Node node)
	{
		return pointToCellQT((node.getXmin() + node.getXmax()) / 2, (node.getYmin() + node.getYmax()) / 2, node);
	}
	
	// point to QT cell
	public static String pointToCellQT(double x, double y, Node node)
	{
		if (node.getNW() != null)
		{
			if (x >= node.getXmin() && x < (node.getXmin() + node.getXmax()) / 2) // point inside SW or NW
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SW
					return "2" + pointToCellQT(x, y, node.getSW());
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NW
					return "0" + pointToCellQT(x, y, node.getNW());
			}
			else if (x >= (node.getXmin() + node.getXmax()) / 2 && x < node.getXmax()) // point inside SE or NE
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SE
					return "3" + pointToCellQT(x, y, node.getSE());
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NE
					return "1" + pointToCellQT(x, y, node.getNE());
			}
		}
		return "";
	}
	
	// cell pruning heuristics (Mapper 3.1)
	public static boolean heuristics123(double x0, double y0, double ds, double[] mbrCentroid, ArrayList<Point> qpoints, double bestDist, boolean fastSums, boolean heuristics, Context context)
	{
		// read MBR coordinates
	    final double xmin = mbrCentroid[0];
	    final double xmax = mbrCentroid[1];
	    final double ymin = mbrCentroid[2];
	    final double ymax = mbrCentroid[3];
	    // read centroid coordinates
	    final double xc = mbrCentroid[4];
	    final double yc = mbrCentroid[5];
	    // read sumDistCQ
	    final double sumDistCQ = mbrCentroid[6];
	    
	    final int qsize = qpoints.size();
 		
		boolean bool = true; // pruning flag (true --> pass, false --> prune)
		
		// heuristic 1 (single point method), prune if: minDist(cell, centroid) >= [bestDist + sumDist(centroid, Q)] / |Q|
		if (bool && heuristics)
		{
			final double dist = pointSquareDistance(xc, yc, x0, y0, x0 + ds, y0 + ds);
			
			if (dist * qsize >= bestDist + sumDistCQ)
			{
				bool = false;
				
				// increment heuristic 1 success
				context.getCounter(Metrics.HEUR1_SUCCESS).increment(1);
			}
		}
		
		// heuristic 2 (minimum bounding method), prune if: minDist(cell, MBR) >= bestDist / |Q|
		if (bool && heuristics)
		{
			// increment heuristic 1 fail
			context.getCounter(Metrics.HEUR1_FAIL).increment(1);
			
			final double dist = squaresDistance(xmin, ymin, xmax, ymax, x0, y0, x0 + ds, y0 + ds);
			if (dist * qsize >= bestDist)
			{
				bool = false;
				
				// increment heuristic 2 success
				context.getCounter(Metrics.HEUR2_SUCCESS).increment(1);
			}
		}
			
		// heuristic 3 (2nd minimum bounding method), prune if: minDist(cell, Q) >= bestDist
		if (bool && heuristics)
		{
			// increment heuristic 2 fail
			context.getCounter(Metrics.HEUR2_FAIL).increment(1);
						
			double sumDistCellQ = 0; // sum of distances of cell to each point of Q
			
			for (Point q : qpoints)
			{
				sumDistCellQ += pointSquareDistance(q.getX(), q.getY(), x0, y0, x0 + ds, y0 + ds);
				
				if (fastSums && sumDistCellQ >= bestDist) // fast sums
					break;
			}
			if (sumDistCellQ >= bestDist)
			{
				bool = false;
				
				// increment heuristic 3 success
				context.getCounter(Metrics.HEUR3_SUCCESS).increment(1);
			}
			else
				// increment heuristic 3 fail
				context.getCounter(Metrics.HEUR3_FAIL).increment(1);
		}
		return bool;
	}
	
	// heuristic 4 - true => pass
	public static boolean heuristic4(int qsize, double dpc, double dm, double sumDistcQ)
	{
		return (qsize * dpc < dm + sumDistcQ);
	}
	
	// return euclidean distance between two points (x1, y1) and (x2, y2)
	public static double distance(double x1, double y1, double x2, double y2)
	{
		return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
	}
	
	// calculate sum of distances from a tpoint to all qpoints
	public static double calcSumDistQ(Point tpoint, ArrayList<Point> queryPoints, boolean fastSums, double dist)
	{				
		double sumdist = 0;
		for (Point qpoint : queryPoints)
		{
			sumdist += distance(qpoint.getX(), qpoint.getY(), tpoint.getX(), tpoint.getY());
			
			if (fastSums && dist > 0 && sumdist > dist) // if fastSums == true break loop
				break;
		}
		return sumdist;
	}
	
	// calculate sum of x-distances from a tpoint to all qpoints
	public static double calcSumDistQx(Point tpoint, ArrayList<Point> queryPoints, boolean fastSums, double dist)
	{
		double sumdistDx = 0;
		for (Point qpoint : queryPoints)
		{
			sumdistDx += Math.abs(qpoint.getX() - tpoint.getX());
			
			if (fastSums && dist > 0 && sumdistDx > dist) // if fastSums == true break loop
				break;
		}
		return sumdistDx;
	}
	
	public static double pointSquareDistance(double xp, double yp, double x1, double y1, double x2, double y2)
	{
		final double dx = Math.max(Math.max(x1 - xp, 0.0), xp - x2);
		final double dy = Math.max(Math.max(y1 - yp, 0.0), yp - y2);

		return Math.sqrt(dx * dx + dy * dy);
	}
	
	public static double squaresDistance(double x1, double y1, double x2, double y2, double x3, double y3, double x4, double y4)
	{
		final boolean left = x4 < x1;
		final boolean right = x2 < x3;
		final boolean bottom = y4 < y1;
		final boolean top = y2 < y3;
		
		double minDist = 0;
		
		if (top && left)
			minDist = distance(x1, y2, x4, y3);
		else if (left && bottom)
			minDist = distance(x1, y1, x4, y4);
		else if (bottom && right)
			minDist = distance(x2, y1, x3, y4);
		else if (right && top)
			minDist = distance(x2, y2, x3, y3);
		else if (left)
			minDist = x1 - x4;
		else if (right)
			minDist = x3 - x2;
		else if (bottom)
			minDist = y1 - y4;
		else if (top)
			minDist = y3 - y2;
		
		return minDist;
	}
	
	public static int binarySearchTpoints(double x, ArrayList<Point> points)
	{
		int low = 0;
		int high = points.size() - 1;
		int middle = (low + high + 1) / 2;
		int location = -1;
		
		do
		{
			if (x >= points.get(middle).getX())
			{
				if (middle == points.size() - 1) // middle = array length
					location = middle;
				else if (x < points.get(middle + 1).getX()) // x between middle and high
					location = middle;
				else // x greater than middle but not smaller than middle+1
					low = middle + 1;
			}
			else // x smaller than middle
				high = middle - 1;
			
			middle = (low + high + 1) / 2; // recalculate middle
			
		} while ((low < high) && (location == -1));
		
		return location;
	}
	
	// join two PQs
	public static PriorityQueue<IdDist> joinPQ(PriorityQueue<IdDist> pq1, PriorityQueue<IdDist> pq2, int k)
	{
		PriorityQueue<IdDist> pq = new PriorityQueue<>(k, new IdDistComparator("max"));
		
		while (!pq1.isEmpty())
		{
			IdDist n1 = pq1.poll();
			if (!isDuplicate(pq, n1))
				pq.offer(n1);
		}
		
		while (!pq2.isEmpty())
		{
			IdDist n2 = pq2.poll();
			if (!isDuplicate(pq, n2))
				pq.offer(n2);
		}
		
		while (pq.size() > k)
			pq.poll();
		
		return pq;
	}
	
	// check for duplicates in PriorityQueue
	public static boolean isDuplicate(PriorityQueue<IdDist> pq, IdDist neighbor)
	{
		for (IdDist elem : pq)
			if (elem.getId() == neighbor.getId())
				return true;
		return false;
	}
	
	// PriorityQueue<IdDist> to String (smallest to largest distance)
	public static String pqToString(PriorityQueue<IdDist> pq, int k, String comp)
	{
		PriorityQueue<IdDist> newPQ = new PriorityQueue<>(k, new IdDistComparator(comp));
		
		newPQ.addAll(pq);
		
		String output = "";
		
		int counter = 0;
		
		while (!newPQ.isEmpty() && counter < k) // add neighbors to output
		{
			IdDist elem = newPQ.poll();
			output = output.concat(String.format("%d\t%.10f\t", elem.getId(), elem.getDist()));
			counter++;
		}
		
		return output;
	}
}
