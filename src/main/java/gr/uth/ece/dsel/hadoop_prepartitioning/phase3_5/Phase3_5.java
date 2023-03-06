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

	public static void main(String[] args)
	{
		// hostname
		String hostname = args[0]; // namenode name is 1st argument
		// username
		String username = System.getProperty("user.name");

		// mapreduce3 dir name
		String mr_3_dir = args[1]; // mapreduce3 dir is 2nd argument
		// = "hdfs://HadoopStandalone:9000/user/panagiotis/mapreduce3/part-r-00000"
		String mr_3_out_full = String.format("hdfs://%s:9000/user/%s/%s", hostname, username, mr_3_dir); // full pathname to mapreduce2 dir in hdfs

		// HDFS dir containing gnn25 file
		String gnn25Dir = args[2]; // HDFS directory containing gnn25 file is 3rd argument
		// gnn25 file name in HDFS
		String gnn25FileName = args[3]; // gnn25 filename is 4th argument
		// full HDFS path to gnn25 file
		String gnn25File = String.format("hdfs://%s:9000/user/%s/%s/%s", hostname, username, gnn25Dir, gnn25FileName); // full HDFS path to gnn25 file

		// GNN K
		int k = Integer.parseInt(args[4]); // K is 5th argument

		try // read / write files
		{
			FileSystem fs = FileSystem.get(new Configuration());
			
			// read Phase 2.5 neighbors PQ from hdfs
			// max heap of K neighbors from Phase 2.5
			PriorityQueue<IdDist> neighbors2 = new PriorityQueue<>(ReadHdfsFiles.getPhase25Neighbors(gnn25File, fs, k));
			
			// read Phase 3 neighbors PQ from hdfs
			// max heap of K neighbors from Phase 3
			PriorityQueue<IdDist> neighbors3 = new PriorityQueue<>(ReadHdfsFiles.getPhase23Neighbors(mr_3_out_full, fs, k));
			
			// output text file {pid1, dist1, pid2, dist2,...,pidK, distK} final GNN list
			// local output text file
			Formatter outputTextFile = new Formatter("gnn_final.txt");
			
			PriorityQueue<IdDist> neighbors35 = new PriorityQueue<IdDist>(k, new IdDistComparator("min")); // min heap
			
			// join PQs from Phases 2.5 & 3
			neighbors35.addAll(GnnFunctions.joinPQ(neighbors2, neighbors3, k));
			
			String output = GnnFunctions.pqToString(neighbors35, k, "min"); // get PQ as String
			
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
