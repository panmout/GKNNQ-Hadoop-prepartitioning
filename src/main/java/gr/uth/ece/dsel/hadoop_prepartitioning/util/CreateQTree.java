/*
package gr.uth.ece.dsel.hadoop_prepartitioning.util;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class CreateQTree
{
	private static Formatter outputTextFile; // local output text file
	private static FileOutputStream fileout;
	private static ObjectOutputStream outputObjectFile; // local output object file
	private static int capacity;
	private static HashMap<Integer, Double[]> sample_dataset;
	private static int numCells = 0;
	private static StringBuilder x = new StringBuilder();
	private static String treeFilePath;
	private static String treeFileName;
	private static String trainingDatasetPath;
	private static int samplerate;
	private static HashSet<Double> widths = new HashSet<Double>(); // store cell widths
	
	public CreateQTree(int newCapacity, String newTreeFilePath, String newTreeFileName, String newTrainingDatasetPath, int newSamplerate)
	{
		capacity = newCapacity;
		treeFilePath = newTreeFilePath;
		treeFileName = newTreeFileName;
		trainingDatasetPath = newTrainingDatasetPath;
		samplerate = newSamplerate;
	}
	
	// create root node (maximum capacity method)
	private static final Node createQT(Node node)
	{
		if (node.getContPoints().size() > capacity)
		{
			node = createChildren(node);
			
			node.setNW(createQT(node.getNW()));
			node.setNE(createQT(node.getNE()));
			node.setSW(createQT(node.getSW()));
			node.setSE(createQT(node.getSE()));
		}
		return node;
	}
	
	// create root node (all children split method)
	private static final Node createQT(Node node, boolean force)
	{
		if (node.getContPoints().size() > capacity || (force == true))
		{
			int nodeNumPoints = node.getContPoints().size();
			
			node = createChildren(node);
			
			if (nodeNumPoints <= capacity)
			{
				node.setNW(createQT(node.getNW(), false));
				node.setNE(createQT(node.getNE(), false));
				node.setSW(createQT(node.getSW(), false));
				node.setSE(createQT(node.getSE(), false));
			}
			else
			{
				force = false;
				
				if (node.getNW().getContPoints().size() > capacity)
				{
					force = true;
				}
				if (node.getNE().getContPoints().size() > capacity)
				{
					force = true;
				}
				if (node.getSW().getContPoints().size() > capacity)
				{
					force = true;
				}
				if (node.getSE().getContPoints().size() > capacity)
				{
					force = true;
				}
				node.setNW(createQT(node.getNW(), force));
				node.setNE(createQT(node.getNE(), force));
				node.setSW(createQT(node.getSW(), force));
				node.setSE(createQT(node.getSE(), force));
			}
		}
		return node;
	}
	
	// create root node (average width split method)
	private static final Node createQT(Node node, double avgWidth)
	{
		if ((node.getContPoints().size() > capacity) || (node.getXmax() - node.getXmin() > avgWidth)) // divide node only if it has many points or is bigger than average size
		{
			node = createChildren(node);
			
			node.setNW(createQT(node.getNW(), avgWidth));
			node.setNE(createQT(node.getNE(), avgWidth));
			node.setSW(createQT(node.getSW(), avgWidth));
			node.setSE(createQT(node.getSE(), avgWidth));
		}
		return node;
	}
	
	// create children and split sample training points
	private static final Node createChildren(Node node)
	{
		// create child nodes
		node.setNW(new Node(node.getXmin(), (node.getYmin() + node.getYmax()) / 2, (node.getXmin() + node.getXmax()) / 2, node.getYmax()));
		node.setNE(new Node((node.getXmin() + node.getXmax()) / 2, (node.getYmin() + node.getYmax()) / 2, node.getXmax(), node.getYmax()));
		node.setSW(new Node(node.getXmin(), node.getYmin(), (node.getXmin() + node.getXmax()) / 2, (node.getYmin() + node.getYmax()) / 2));
		node.setSE(new Node((node.getXmin() + node.getXmax()) / 2, node.getYmin(), node.getXmax(), (node.getYmin() + node.getYmax()) / 2));
		
		// partition dataset to child nodes
		Iterator<Integer> iterator = node.getContPoints().iterator(); // create iterator
		while (iterator.hasNext()) // while set has elements
		{
			int pid = iterator.next();
			Double[] coords = sample_dataset.get(pid);
			double x = coords[0];
			double y = coords[1];
			if (x >= node.getXmin() && x < (node.getXmin() + node.getXmax()) / 2) // point inside SW or NW
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SW
				{
					node.getSW().addPoints(pid);
				}
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NW
				{
					node.getNW().addPoints(pid);
				}
			}
			else if (x >= (node.getXmin() + node.getXmax()) / 2 && x < node.getXmax()) // point inside SE or NE
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SE
				{
					node.getSE().addPoints(pid);
				}
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NE
				{
					node.getNE().addPoints(pid);
				}
			}
			iterator.remove();
			node.removePoint(pid); // remove point from parent node
		}
		return node;
	}
	
	private static final void df_repr(Node node) // create qtree in string form
	{
		if (node.getNW() == null)
		{
			x.append("0");
			numCells++;
		}
		else
		{
			x.append("1");
			df_repr(node.getNW());
			df_repr(node.getNE());
			df_repr(node.getSW());
			df_repr(node.getSE());
		}
	}
	
	// get leaves widths
	private static final void getWidths(Node node)
	{		
		if (node.getNW() == null)
		{
			widths.add(node.getXmax() - node.getXmin());
		}
		else
		{
			getWidths(node.getNW());
			getWidths(node.getNE());
			getWidths(node.getSW());
			getWidths(node.getSE());
		}
	}
	
	private static final void readSample()
	{
		try // open files
		{
			fileout = new FileOutputStream(treeFileName);
			outputObjectFile = new ObjectOutputStream(fileout); // open local output object file
			outputTextFile = new Formatter("qtree.txt"); // open local output text file
			
			FileSystem fs = FileSystem.get(new Configuration());
			Path trainingPath = new Path(trainingDatasetPath);
			BufferedReader trainingBr = new BufferedReader(new InputStreamReader(fs.open(trainingPath))); // open HDFS training dataset file
			
			sample_dataset = new HashMap<Integer, Double[]>();
			
			HashSet<Integer> randomNumbers = new HashSet<Integer>(samplerate); // [percentSample] size set for random integers
			
			Random random = new Random();
			
			while (randomNumbers.size() < samplerate) // fill list
				randomNumbers.add(random.nextInt(100)); // add a random integer 0 - 99
			
			String line;
			// read training dataset and get sample points
			while ((line = trainingBr.readLine()) != null)
			{
				if (randomNumbers.contains(random.nextInt(100))) // [percentSample]% probability
				{
					String[] data = line.trim().split("\t");
					int pid = Integer.parseInt(data[0]); // tpoint id
					double x = Double.parseDouble(data[1]); // get x
					double y = Double.parseDouble(data[2]); // get y
					Double[] tpoint = {x, y};
					sample_dataset.put(pid, tpoint); // add {pid, x, y} to hashmap
				}
			}
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
	}
	
	private static final void writeFiles(Node node)
	{		
		// write to files
		try
		{
			// local
			outputTextFile = new Formatter("qtree.txt");
			outputTextFile.format("%s", x);
			outputObjectFile.writeObject(node);
			
			outputObjectFile.close();
			outputTextFile.close();
			fileout.close();
			
			// write to hdfs
			FileSystem fs = FileSystem.get(new Configuration());
			Path path = new Path(treeFilePath);
			ObjectOutputStream outputStream = new ObjectOutputStream(fs.create(path));
			outputStream.writeObject(node);
			outputStream.close();
		}
		catch (FormatterClosedException formatterException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(2);
		}
		catch (IOException ioException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(2);
		}
	}
	
	// create quadtree (capacity based only)
	public static final void createQTree()
	{
		readSample();
		
		// create quad tree from sample dataset
		Node root = new Node(0.0, 0.0, 1.0, 1.0); // create root node
		
		for (int i : sample_dataset.keySet()) // add all sample points to root node
		{
			root.addPoints(i);
		}
		
		root = createQT(root); // create tree from root
		
		root.removePoints(); // remove all tpoints from tree
		
		df_repr(root); // create tree string
		
		System.out.printf("number of cells: %d\n", numCells);
		
		writeFiles(root);
	}
	
	// create quadtree (all children split method = if one child splits, all will)
	public static final void createAllChldSplitQTree()
	{
		readSample();
		
		// create quad tree from sample dataset
		Node root = new Node(0.0, 0.0, 1.0, 1.0); // create root node
		
		for (int i : sample_dataset.keySet()) // add all sample points to root node
		{
			root.addPoints(i);
		}
		
		root = createQT(root, false); // create tree from root
		
		root.removePoints(); // remove all tpoints from tree
		
		df_repr(root); // create tree string
		
		System.out.printf("number of cells: %d\n", numCells);
		
		writeFiles(root);
	}
	
	// create quadtree (capacity based only)
	public static final void createAvgWidthQTree()
	{
		readSample();
		
		// create quad tree from sample dataset
		Node root1 = new Node(0.0, 0.0, 1.0, 1.0); // create root node
		
		for (int i : sample_dataset.keySet()) // add all sample points to root node
		{
			root1.addPoints(i);
		}
		
		// create initial tree from root, 'POSITIVE_INFINITY' is used to make the right OR statement 'false'
		root1 = createQT(root1, Double.POSITIVE_INFINITY);
		
		getWidths(root1); // get all widths of leaves
		
		double averageWidth = 0;
		
		for (double i : widths)
		{
			averageWidth += i;
		}
		
		averageWidth = averageWidth / widths.size();
		
		System.out.printf("average width: %11.10f\n", averageWidth);
		
		Node root2 = new Node(0.0, 0.0, 1.0, 1.0); // create root node
		
		for (int i : sample_dataset.keySet()) // add all sample points to root node
		{
			root2.addPoints(i);
		}
		
		root2 = createQT(root2, averageWidth); // create final tree from root and average width
		
		root2.removePoints(); // remove all tpoints from tree
		
		df_repr(root2); // create tree string
		
		System.out.printf("number of cells: %d\n", numCells);
		
		writeFiles(root2);
	}
}
*/