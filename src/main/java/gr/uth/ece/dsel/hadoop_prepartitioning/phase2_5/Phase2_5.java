package gr.uth.ece.dsel.hadoop_prepartitioning.phase2_5;

import java.io.IOException;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;

public class Phase2_5
{
	private static String hostname; // hostname
	private static String username; // username
	private static String mr_2_dir; // mapreduce2 dir name
	private static String mr_2_out_full; // = "hdfs://HadoopStandalone:9000/user/panagiotis/mapreduce2/part-r-00000"
	private static String gnnDir; // HDFS dir containing GNN files
	private static String gnn25FileName; // gnn25 file name in HDFS
	private static Formatter outputTextFile; // local output text file
	private static int K; // GNN K
	private static PriorityQueue<IdDist> neighbors25; // max heap of K neighbors
	
	public static void main(String[] args)
	{
		hostname = args[0]; // namenode name is 1st argument
		username = System.getProperty("user.name");
		
		mr_2_dir = args[1]; // mapreduce2 dir is 2nd argument
		mr_2_out_full = String.format("hdfs://%s:9000/user/%s/%s", hostname, username, mr_2_dir); // full pathname to mapreduce2 dir in hdfs
		
		gnnDir = args[2]; // HDFS directory containing GNN files is 3rd argument
		gnn25FileName = String.format("hdfs://%s:9000/user/%s/%s/gnn2_5.txt", hostname, username, gnnDir);
		
		K = Integer.parseInt(args[3]); // K is 4th argument
		
		neighbors25 = new PriorityQueue<IdDist>(K, new IdDistComparator("min")); // min heap
		
		try // read / write files
		{
			FileSystem fs = FileSystem.get(new Configuration());
			
			// read MR2 output
			neighbors25.addAll(ReadHdfsFiles.getPhase23Neighbors(mr_2_out_full, fs, K));
			
			// output text file {pid1, dist1, pid2, dist2,...,pidK, distK} best neighbors list so far
			outputTextFile = new Formatter("gnn2_5.txt");
						
			String output = GnnFunctions.pqToString(neighbors25, K, "min"); // get PQ as String
			
			outputTextFile.format(output);
			outputTextFile.close();
			
			// write to hdfs
			Path path = new Path(gnn25FileName);
			FSDataOutputStream outputStream = fs.create(path);
			outputStream.writeBytes(output);
			outputStream.close();
		}
		catch (FormatterClosedException formatterException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(2);
		}
		catch (IOException ioException)
		{
			System.err.println("Could not open file, exiting");
			System.exit(1);
		}
	}
}
