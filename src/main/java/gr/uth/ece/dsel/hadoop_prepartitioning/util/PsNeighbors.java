package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import org.apache.hadoop.mapreduce.Reducer.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;

public final class PsNeighbors
{
	private int k;
	private final ArrayList<Point> qpoints;
	private final double[] mbrCentroid;
	private final PriorityQueue<IdDist> neighbors;
	private ArrayList<Point> tpoints;
	private boolean changed = false; // priority queue will be returned only if changed
	private final boolean fastsums;
	private final Context context;
	
	public PsNeighbors(int K, double[] mbrC, ArrayList<Point> qp, PriorityQueue<IdDist> pq, boolean fs, Context con)
	{
		this.k = K;
		this.qpoints = new ArrayList<>(qp);
		this.mbrCentroid = Arrays.copyOf(mbrC, mbrC.length);
		this.neighbors = new PriorityQueue<>(pq);
		this.fastsums = fs;
		this.context = con;
	}
	
	public void setTpoints(ArrayList<Point> tp)
	{
		this.tpoints = new ArrayList<>(tp);
	}
	
	public PriorityQueue<IdDist> getNeighbors()
	{		
		// read MBR coordinates
		double xmin = this.mbrCentroid[0];
	    double xmax = this.mbrCentroid[1];
	    
	    // sort list by x ascending
	 	this.tpoints.sort(new PointXComparator("min"));
	 	
	 	//System.out.printf("tpoints = %d\n", this.tpoints.size());
	 	
	 	if (!this.tpoints.isEmpty()) // if this cell has any tpoints
		{
			// get median point of Q
			int m = this.qpoints.size() / 2; // median index of Q
			double xm = this.qpoints.get(m).getX(); // x of median qpoint
			
			double x_left = this.tpoints.get(0).getX(); // leftmost tpoint's x
			double x_right = this.tpoints.get(this.tpoints.size() - 1).getX(); // rightmost tpoint's x
			
			boolean check_right = false;
			boolean check_left = false;
			
			int right_limit = 0;
			int left_limit = 0;
			
			if (xm < x_left) // median qpoint is at left of all tpoints
			{
				check_right = true;
				right_limit = 0;
			}
			else if (x_right < xm) // median qpoint is at right of all tpoints
			{
				check_left = true;
				left_limit = this.tpoints.size() - 1;
			}
			else // median qpoint is among tpoints
			{
				check_left = true;
				check_right = true;
				
				int tindex = GnnFunctions.binarySearchTpoints(xm, this.tpoints); // get tpoints array index for median qpoint interpolation
				right_limit = tindex + 1;
				left_limit = tindex;
			}
			
			boolean cont_search = true; // set flag to true
			
			if (check_left)
			{
				while ((left_limit > -1) && (xt(left_limit) > xmin))  // if tpoint's x is inside MBR
					if (!calc_sum_dist_in(left_limit--))
					{
						cont_search = false;
						break;
					}
				if (cont_search) // if tpoint's x is outside MBR
				{
					while (left_limit > -1)
						if (!calc_sum_dist_out(left_limit--))
							break;
				}
				// x-check success, add remaining tpoints
				if (left_limit > 0) // could be left_limit = -1
					this.context.getCounter(Metrics.SKIPPED_TPOINTS).increment(left_limit);
				//System.out.printf("left_limit = %d\n", left_limit);
			}
			
			cont_search = true; // set flag to true
			
			if (check_right)
			{
				while (right_limit < this.tpoints.size() && (xt(right_limit) < xmax)) // if tpoint's x is inside MBR
					if (!calc_sum_dist_in(right_limit++))
					{
						cont_search = false;
						break;
					}
				if (cont_search) // if tpoint's x is outside MBR
				{
					while (right_limit < this.tpoints.size())
						if (!calc_sum_dist_out(right_limit++))
							break;
				}
				// x-check success, add remaining tpoints
				if (this.tpoints.size() - right_limit > 0) // could be right_limit = tpoints.size()
					this.context.getCounter(Metrics.SKIPPED_TPOINTS).increment(this.tpoints.size() - right_limit);
				//System.out.printf("tpoints.size - right_limit = %d\n", this.tpoints.size() - right_limit);
			}
		}
	 	
	 	if (this.changed)
	    	return this.neighbors;
	    else
	    	return new PriorityQueue<>(this.k, new IdDistComparator("max"));
	 	// end PsNeighbors
	}
	
	private boolean calc_sum_dist_in(int i) // if tpoint's x is inside MBR
	{
		// read centroid coordinates
		double xc = this.mbrCentroid[4];
	    double yc = this.mbrCentroid[5];
	    // read sumDistCQ
	    double sumDistCQ = this.mbrCentroid[6];
	    
		Point tpoint = this.tpoints.get(i); // get tpoint
		double xt = tpoint.getX(); // tpoint's x
		double yt = tpoint.getY(); // tpoint's y
		
		//System.out.printf("checking tpoint %d\n", tpoint.getId());
		
		if (this.neighbors.size() < this.k) // if queue is not full, add new tpoints 
		{
			double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, false, 0); // distance calculation
			this.neighbors.offer(new IdDist(tpoint.getId(), sumdist)); // insert to queue
			this.changed = true;
			//increment SUMDIST metrics variable
			context.getCounter(Metrics.SUMDIST).increment(1);
			//System.out.println(pqToString());
			return true; // check next point
		}
		else  // if queue is full, run some checks and replace elements
		{
			double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
			
			double dpc = GnnFunctions.distance(xc, yc, xt, yt); // tpoint-centroid distance
			
			if (!GnnFunctions.heuristic4(this.qpoints.size(), dpc, dm, sumDistCQ)) // if |Q|*dist(p,c) >= MaxHeap.root.dist + dist(centroid, Q) then prune point
			{
				//System.out.printf("tpoint: %d\tdpc success: qsize = %d\tdpc = %f\tqsize*dpc = %f\tdm = %f\tsumDistCQ = %f\tdm + sumDistCQ = %f\n", tpoint.getId(), this.qpoints.size(), dpc, this.qpoints.size()*dpc, dm, sumDistCQ, dm + sumDistCQ);
				//increment DPC_COUNT metrics variable
				this.context.getCounter(Metrics.DPC_COUNT).increment(1);
				return true; // check next point
			}
			
			double sumdx = GnnFunctions.calcSumDistQx(tpoint, this.qpoints, this.fastsums, dm); // calculate sumdx of tpoint from Q
			
			if (sumdx <= dm)
				//System.out.printf("tpoint: %d\tsumdx_fail: sumdx = %f <= dm = %f\n", tpoint.getId(), sumdx, dm);
			
			//increment SUMDX metrics variable
			this.context.getCounter(Metrics.SUMDX).increment(1);
			
			if (sumdx > dm)  // if sumdx bigger than MaxHeap.root.dist
			{
				//System.out.printf("tpoint: %d\tsumdx_success: sumdx = %f > dm = %f\n", tpoint.getId(),sumdx, dm);
				//increment SUMDX_SUCCESS metrics variable
				this.context.getCounter(Metrics.SUMDX_SUCCESS).increment(1);
				return false; // end while loop
			}
			
			double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, this.fastsums, dm); // distance calculation
			
			//increment SUMDIST metrics variable
			this.context.getCounter(Metrics.SUMDIST).increment(1);
			
			if (sumdist < dm) // finally compare distances
			{
				this.neighbors.poll(); // remove top element
				this.neighbors.offer(new IdDist(tpoint.getId(), sumdist)); // insert to queue
				this.changed = true;
				//System.out.println(pqToString());
			}
			return true; // check next point
		}
	}
	
	private boolean calc_sum_dist_out(int i) // if tpoint's x is outside MBR
	{
		// read centroid coordinates
		double xc = this.mbrCentroid[4];
	    double yc = this.mbrCentroid[5];
	    // read sumDistCQ
	    double sumDistCQ = this.mbrCentroid[6];
	    
		Point tpoint = this.tpoints.get(i); // get tpoint
		double xt = tpoint.getX(); // tpoint's x
		double yt = tpoint.getY(); // tpoint's y
		
		//System.out.printf("checking tpoint %d\n", tpoint.getId());
		
		if (this.neighbors.size() < this.k) // if queue is not full, add new tpoints 
		{
			double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, false, 0); // distance calculation
			this.neighbors.offer(new IdDist(tpoint.getId(), sumdist)); // insert to queue
			this.changed = true;
			//increment SUMDIST metrics variable
			context.getCounter(Metrics.SUMDIST).increment(1);
			//System.out.println(pqToString());
			return true; // check next point
		}
		else  // if queue is full, run some checks and replace elements
		{
			double dpcx = Math.abs(xt - xc); // calculate (tpoint, centroid) x-dist
			
			double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
			
			double sumdx = this.qpoints.size()*dpcx; // calculate sumdx(tpoint, Q) = |Q|*dpcx
			
			if (sumdx <= dm)
				//System.out.printf("tpoint: %d\tsumdx_fail: sumdx = %f <= dm = %f\n", tpoint.getId(), sumdx, dm);
			
			if (sumdx >= dm) // if |Q|*dx(p,c) >= MaxHeap.root.dist then terminate search
			{
				//System.out.printf("tpoint: %d\tsumdx_success: sumdx = %f > dm = %f\n", tpoint.getId(), sumdx, dm);
				//increment SUMDX_SUCCESS metrics variable
				this.context.getCounter(Metrics.SUMDX_SUCCESS).increment(1);
				return false; // end while loop
			}
						
			double dpc = GnnFunctions.distance(xc, yc, xt, yt); // calculate (tpoint, centroid) dist
			
			if (!GnnFunctions.heuristic4(this.qpoints.size(), dpc, dm, sumDistCQ)) // if |Q|*dist(p,c) >= MaxHeap.root.dist + dist(centroid, Q) then prune point
			{
				//System.out.printf("tpoint: %d\tdpc success: qsize = %d\tdpc = %f\tqsize*dpc = %f\tdm = %f\tsumDistCQ = %f\tdm + sumDistCQ = %f\n", tpoint.getId(), this.qpoints.size(), dpc, this.qpoints.size()*dpc, dm, sumDistCQ, dm + sumDistCQ);
				//increment DPC_COUNT metrics variable
				this.context.getCounter(Metrics.DPC_COUNT).increment(1);
				return true; // check next point
			}
			
			double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, this.fastsums, dm); // distance calculation
			
			//increment SUMDIST metrics variable
			this.context.getCounter(Metrics.SUMDIST).increment(1);
			
			if (sumdist < dm) // finally compare distances
			{
				this.neighbors.poll(); // remove top element
				this.neighbors.offer(new IdDist(tpoint.getId(), sumdist)); // insert to queue
				this.changed = true;
				//System.out.println(pqToString());
			}
			return true; // check next point	
		}
	}
	
	private double xt(int i)
	{
		Point tpoint = this.tpoints.get(i); // get tpoint
		return tpoint.getX(); // tpoint's x
	}
}
