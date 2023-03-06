package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;
import org.apache.hadoop.mapreduce.Reducer.Context;

public final class BfNeighbors
{
	private int k;
	private final ArrayList<Point> qpoints;
	private ArrayList<Point> tpoints;
	private final double[] mbrCentroid;
	private final PriorityQueue<IdDist> neighbors;
	private final boolean fastsums;
	private final Context context;
	
	public BfNeighbors(int K, double[] mbrC, ArrayList<Point> qp, PriorityQueue<IdDist> pq, boolean fs, Context con)
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
	
	public final PriorityQueue<IdDist> getNeighbors()
	{
		boolean changed = false; // priority queue will be returned only if changed
		
	    // read centroid coordinates
	    double xc = this.mbrCentroid[4];
	    double yc = this.mbrCentroid[5];
	    // read sumDistCQ
	    double sumDistCQ = this.mbrCentroid[6];
	    
	    for (Point tpoint : this.tpoints)
	    {
			int tid = tpoint.getId();
	    	double xt = tpoint.getX();
	    	double yt = tpoint.getY();
	    	
	    	// if PriorityQueue not full, add new tpoint (IdDist)
	    	if (this.neighbors.size() < this.k) {
	    		double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, false, 0); // distance calculation
	    		this.neighbors.add(new IdDist(tid, sumdist)); // insert to queue
	    		changed = true;
	    	}
	    	else // if queue is full, run some checks and replace elements
	    	{
	    		double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
	    		double dpc = GnnFunctions.distance(xt, yt, xc, yc); // tpoint-centroid distance
	    		
	    		if (!GnnFunctions.heuristic4(this.qpoints.size(), dpc, dm, sumDistCQ)) // if |Q|*dist(p,c) >= MaxHeap.root.dist + dist(centroid, Q) then prune point
	  		  	{
		    		//increment DPC_COUNT metrics variable
	    			this.context.getCounter(Metrics.DPC_COUNT).increment(1);
	  		  	}
	    		else // if |Q|*dist(p,c) < MaxHeap.root.dist + dist(centroid, Q) then pass point
	  		  	{
	    			double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, this.fastsums, dm); // distance calculation
					
	  				if (sumdist < dm) // compare distance
	  				{
	  					this.neighbors.poll(); // remove top element
	  					this.neighbors.add(new IdDist(tid, sumdist)); // insert to queue
	  					changed = true;
	  				} // end if
	  			} // end if
	    	} // end else
		} // end for
	    if (changed)
	    	return neighbors;
	    else
	    	return new PriorityQueue<>(this.k, new IdDistComparator("max"));
	} // end gdBfNeighbors
}