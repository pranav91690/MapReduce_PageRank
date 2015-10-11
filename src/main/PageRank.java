package main;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parser.XmlInputFormat;

public class PageRank extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		int status = ToolRunner.run(new Configuration(), new PageRank(), args);
		
		System.exit(status);
	}

	public int run(String args[]) throws Exception {
		String inputxml = args[0];
		String rootpath = args[1];
		String iterpath = rootpath + "/tmp/iter";

		// Job 1
		runMapReduceJob(inputxml, rootpath + "/tmp/out1", RemoveLinkMapper.class, RemoveLinkReducer.class, 1);

		// Job 2
		runMapReduceJob(rootpath + "/tmp/out1", rootpath + "/tmp/out2", LinkMapper.class, LinkReducer.class, 0);

		// Job 3
		runMapReduceJob(rootpath + "/tmp/out2", rootpath + "/tmp/out3", NodeCountMapper.class, NodeCountReducer.class, 0);

		// Job 4
		long N = getNodeCount(rootpath + "/tmp/out3");

		runMapReduceJob(rootpath + "/tmp/out2", iterpath + "0", AppendRankMapper.class, AppendRankReducer.class, N);

		// Job 5
		for (int iter = 1; iter < 9; iter++) {

			runMapReduceJob(iterpath + (iter - 1), iterpath + iter, AlgoMapper.class, AlgoReducer.class, N);

		}

		// Job 6
		runMapReduceJob(iterpath + 8, rootpath + "/tmp/out5", SortMapper.class, SortReducer.class, N);

		return 0;

	}
	
	public static long getNodeCount(String fpath) throws Exception{
		Scanner sc = new Scanner(new File(fpath + "/part-r-00000"));
		try{
			String str = sc.nextLine();
			return Long.parseLong(str.trim());
		}
		finally{
			sc.close();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean runMapReduceJob(String inputPath, String outputPath, Class mapperClass, Class reducerClass, long N) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		configuration.set("nodecount", "" + N);
		
		if (N == 1) {
			configuration.set("xmlinput.start", "<page>");
			configuration.set("xmlinput.end", "</page>");
			configuration.set(
					"io.serializations",
					"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		}

		Job hadoopJob = Job.getInstance(configuration);
		hadoopJob.setJarByClass(PageRank.class);
		
		if (N == 5){
			hadoopJob.setSortComparatorClass(SortComparator.class);
			hadoopJob.setMapOutputKeyClass(Text.class);
			hadoopJob.setMapOutputValueClass(Text.class);
		}
		
		hadoopJob.setMapperClass(mapperClass);
		hadoopJob.setReducerClass(reducerClass);
		
		hadoopJob.setOutputKeyClass(Text.class);
		hadoopJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(hadoopJob, new Path(inputPath));
		hadoopJob.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(hadoopJob, new Path(outputPath));
		hadoopJob.setOutputFormatClass(TextOutputFormat.class);
		
		if (N == 1){
			hadoopJob.setInputFormatClass(XmlInputFormat.class);
		}

		return hadoopJob.waitForCompletion(true);

	}

}