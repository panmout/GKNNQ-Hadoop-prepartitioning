package gr.uth.ece.dsel.hadoop_prepartitioning.phase3_5;

import java.io.IOException;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import gr.uth.ece.dsel.hadoop_prepartitioning.util.*;

public class Phase3_5
{
	private static String hostname; // hostname
	private static String username; // username
	private static String mr_3_dir; // mapreduce3 dir name
	private static String mr_3_out_full; // = "hdfs://HadoopStandalone:9000/user/panagiotis/mapreduce3/part-r-00000"
	private static String gnn25Dir; // HDFS dir containing gnn25 file
	private static String gnn25FileName; // gnn25 file name in HDFS
	private static String gnn25File; // full HDFS path to gnn25 file
	private static Formatter outputTextFile; // local output text file
	private static int K; // GNN K
	private static PriorityQueue<IdDist> neighbors2; // max heap of K neighbors from Phase 2.5
	private static PriorityQueue<IdDist> neighbors3; // max heap of K neighbors from Phase 3
	
	public static void main(String[] args)
	{
		hostname = args[0]; // namenode name is 1st argument
		username = System.getProperty("user.name");
		
		mr_3_dir = args[1]; // mapreduce3 dir is 2nd argument
		mr_3_out_full = String.format("hdfs://%s:9000/user/%s/%s", hostname, username, mr_3_dir); // full pathname to mapreduce2 dir in hdfs
		
		gnn25Dir = args[2]; // HDFS directory containing gnn25 file is 3rd argument
		gnn25FileName = args[3]; // gnn25 filename is 4th argument
		gnn25File = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, gnn25Dir, gnn25FileName); // full HDFS path to gnn25 file
		
		K = Integer.parseInt(args[4]); // K is 5th argument
		
		try // read / write files
		{
			FileSystem fs = FileSystem.get(new Configuration());
			
			// read Phase 2.5 neighbors PQ from hdfs
			neighbors2 = new PriorityQueue<IdDist>(ReadHdfsFiles.getPhase25Neighbors(gnn25File, fs, K));
			
			// read Phase 3 neighbors PQ from hdfs
			neighbors3 = new PriorityQueue<IdDist>(ReadHdfsFiles.getPhase23Neighbors(mr_3_out_full, fs, K));
			
			// output text file {pid1, dist1, pid2, dist2,...,pidK, distK} final GNN list
			outputTextFile = new Formatter("gnn_final.txt");
			
			PriorityQueue<IdDist> neighbors35 = new PriorityQueue<IdDist>(K, new IdDistComparator("min")); // min heap
			
			// join PQs from Phases 2.5 & 3
			neighbors35.addAll(GnnFunctions.joinPQ(neighbors2, neighbors3, K));
			
			String output = GnnFunctions.pqToString(neighbors35, K, "min"); // get PQ as String
			
			outputTextFile.format(output);
			outputTextFile.close();
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
