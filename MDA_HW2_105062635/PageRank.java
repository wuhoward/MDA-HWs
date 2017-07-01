package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;  
import java.io.InputStreamReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;  
import java.util.PriorityQueue;  
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;	
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	public static final int NODES = 10876;
	public static final int PRECISION = 0;
	public static final int TOPN = 10;
	public static final int MAXROUND = 100;
	public static final double BETA = 0.8;
	
	public static class PRMapper1 extends Mapper<Object, Text, Text, Text>{
		private Text page = new Text();
		private Text value = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String from = ((FileSplit) context.getInputSplit()).getPath().toString();
			String[] input = value.toString().split("\t");
			if(from.matches("(.*)part-r-(.*)")){
				page.set(input[0]);
				value.set("R" + input[1]);
				context.write(page, value); 
			}
			else if(Character.isDigit(input[0].charAt(0))){
				page.set(input[0]);
				value.set(input[1]);
				context.write(page, value); 
				page.set(input[1]);
				value.set("TO");
				context.write(page, value); 
			}
		}
	}

	public static class PRReducer1 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {
			String neighbors = "";
			String weight = "";
			result.set("");
			Configuration conf = context.getConfiguration();
			String round = conf.get("round");
			for (Text val : values) {
				if(val.toString().charAt(0) == 'R') weight += val.toString().substring(1);
				else if(!val.toString().equals("TO"))neighbors += "\t" + val.toString();
			}
			if(round.equals("0"))result.set(Double.toString(1.0/NODES) + neighbors);
			else result.set(weight + neighbors);
			context.write(key, result);
		}
	}

	public static class PRMapper2 extends Mapper<Object, Text, Text, Text>{
		private Text page = new Text();
		private Text value = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t");	
			if(input.length>2){
				value.set(Double.toString(BETA*Double.parseDouble(input[1])/(input.length-2)));
				for(int i=2; i<input.length; i++){
					page.set(input[i]);
					context.write(page, value); 
					page.set("SUM");
					context.write(page, value);
				}
			}
			page.set("SUM");
			value.set("N" + input[0]);
			context.write(page, value);
		}
	}

	public static class PRReducer2 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {
			double sum = 0;
			String nodes = "";
			for (Text val : values) {
				if(val.toString().charAt(0) == 'N')nodes += "\t" + val.toString().substring(1);
				else sum += Double.parseDouble(val.toString());
			}
			if(key.toString().equals("SUM"))sum += 1-BETA;
			else sum += (1-BETA)/(double)NODES;
			if(nodes.length()>0)result.set(Double.toString(sum) + nodes);
			else result.set(Double.toString(sum));
			context.write(key, result);
		}
	}
	
	public static class PRMapper3 extends Mapper<Object, Text, Text, Text>{
		private Text page = new Text();
		private Text value = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t");
			if(input[0].equals("SUM")){
				value.set(Double.toString((1.0-Double.parseDouble(input[1]))/NODES));
				for(int i=2; i<input.length; i++){
					page.set(input[i]);
					context.write(page, value); 
				}
			}
			else{
				page.set(input[0]);
				value.set(input[1]);
				context.write(page, value); 
			}
		}
	}
	
	public static class PRReducer3 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {
			double sum = 0;
			for (Text val : values) {
				sum += Double.parseDouble(val.toString());
			}
			if(PRECISION > 0)result.set(Double.toString(BigDecimal.valueOf(sum).setScale(PRECISION, RoundingMode.HALF_UP).doubleValue()));
			else result.set(Double.toString(sum));
			context.write(key, result);
		}
	}
	
	public static class dComparator implements Comparator<String>
	{
		@Override
		public int compare(String x, String y)
		{
			String[] input1 = x.split("\t");
			String[] input2 = y.split("\t");
			return Double.parseDouble(input1[1]) > Double.parseDouble(input2[1]) ? 1 : -1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: PageRank <in> <out>");
			System.exit(2);
		}
		FileSystem hdfs = FileSystem.get(conf);

		int round = 0;
		while(true){
			conf.set("round", Integer.toString(round));
			Job job1 = new Job(conf, "PageRank step 1");
			job1.setJarByClass(PageRank.class);
			job1.setMapperClass(PRMapper1.class);
			job1.setReducerClass(PRReducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
			if(round > 0)FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job1, new Path("output/tmp"));
			job1.waitForCompletion(true);
			
			if(round > 0)hdfs.rename(new Path(otherArgs[1]), new Path("output/old"));
		
			Job job2 = new Job(conf, "PageRank step 2");
			job2.setJarByClass(PageRank.class);
			job2.setMapperClass(PRMapper2.class);
			job2.setReducerClass(PRReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path("output/tmp"));
			FileOutputFormat.setOutputPath(job2, new Path("output/tmp2"));
			job2.waitForCompletion(true);
		
			Job job3 = new Job(conf, "PageRank step 3");
			job3.setJarByClass(PageRank.class);
			job3.setMapperClass(PRMapper3.class);
			job3.setReducerClass(PRReducer3.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job3, new Path("output/tmp2"));
			FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));
			job3.waitForCompletion(true);

			hdfs.delete(new Path("output/tmp"), true);
			hdfs.delete(new Path("output/tmp2"), true);			
			round++;
			System.out.println("========= round " + Integer.toString(round) + " finished! ==========\n");
			if(round > 1){
				FileChecksum chksum1 = hdfs.getFileChecksum(new Path("output/old/part-r-00000"));
				FileChecksum chksum2 = hdfs.getFileChecksum(new Path(otherArgs[1] + "/part-r-00000"));
				hdfs.delete(new Path("output/old"), true);	
				if(chksum1.equals(chksum2))break;
			}
			if(round == MAXROUND)break;
		}
		
		Comparator<String> comparator = new dComparator();
		PriorityQueue<String> queue = new PriorityQueue<String>(TOPN, comparator);	
		FSDataInputStream inStream = hdfs.open(new Path(otherArgs[1] + "/part-r-00000"));
		BufferedReader in = new BufferedReader(new InputStreamReader(inStream, "UTF-8"));
		String line;
		while ((line = in.readLine()) != null){ 
			queue.offer(line);
			if (queue.size() > TOPN)queue.poll();
        }
		in.close();
		inStream.close();
		ArrayList<String> top = new ArrayList<String>();
		while(queue.size() > 0)top.add(queue.poll());
		for(int i=top.size()-1; i>=0; i--)System.out.println(top.get(i));
		System.exit(0);
	}
}
