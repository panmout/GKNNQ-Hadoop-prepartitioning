package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.io.Serializable;
//import java.util.HashSet;

public final class Node implements Serializable
{
	private int low, high; // lower, higher index of sample array
	private Node nw, ne, sw, se; // children
	private double xmin, xmax, ymin, ymax; // node boundaries
	//private HashSet<Integer> contPoints = new HashSet<Integer>(); // points contained
	
	// constructor
	public Node (double xmin, double ymin, double xmax, double ymax)
	{
		this.xmin = xmin;
		this.xmax = xmax;
		this.ymin = ymin;
		this.ymax = ymax;
	}
	
	public final int getLow()
	{
		return this.low;
	}

	public final void setLow(int low)
	{
		this.low = low;
	}

	public final int getHigh()
	{
		return this.high;
	}

	public final void setHigh(int high)
	{
		this.high = high;
	}
	
	public final double getXmin() {
		return this.xmin;
	}

	public final void setXmin(double xmin) {
		this.xmin = xmin;
	}

	public final double getXmax() {
		return this.xmax;
	}

	public final void setXmax(double xmax) {
		this.xmax = xmax;
	}

	public final double getYmin() {
		return this.ymin;
	}

	public final void setYmin(double ymin) {
		this.ymin = ymin;
	}

	public final double getYmax() {
		return this.ymax;
	}

	public final void setYmax(double ymax) {
		this.ymax = ymax;
	}

	public final Node getNW() {
		return this.nw;
	}

	public final void setNW(Node nW) {
		this.nw = nW;
	}

	public final Node getNE() {
		return this.ne;
	}

	public final void setNE(Node nE) {
		this.ne = nE;
	}

	public final Node getSW() {
		return this.sw;
	}

	public final void setSW(Node sW) {
		this.sw = sW;
	}

	public final Node getSE() {
		return this.se;
	}

	public final void setSE(Node sE) {
		this.se = sE;
	}
	
	/*
	public final void addPoints(int i)
	{
		this.contPoints.add(i);
	}
	
	public final void removePoints()
	{
		if (this.nw != null)
		{
			this.nw.removePoints();
			this.nw.contPoints.clear();
		}
		if (this.ne != null)
		{
			this.ne.removePoints();
			this.ne.contPoints.clear();
		}
		if (this.sw != null)
		{
			this.sw.removePoints();
			this.sw.contPoints.clear();
		}
		if (this.se != null)
		{
			this.se.removePoints();
			this.se.contPoints.clear();
		}
	}

	public final HashSet<Integer> getContPoints() {
		return this.contPoints;
	}

	public final void setContPoints(HashSet<Integer> contPoints) {
		this.contPoints = new HashSet<Integer>(contPoints);
	}
	
	public final void removePoint(int i) {
		this.contPoints.remove(i);
	}
	*/
}
