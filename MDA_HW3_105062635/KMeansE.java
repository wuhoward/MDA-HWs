package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;  
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList; 
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

public class KMeansE {
	public static final int POINTS = 4601;
	public static final int DIM = 58;
	public static final int MAX_ITER = 20;
	
	public static class KMMapper1 extends Mapper<Object, Text, Text, Text>{
		private Text point = new Text();
		private Text position = new Text();
		private int dcount = 0;
		private int ccount = 0;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String from = ((FileSplit) context.getInputSplit()).getPath().toString();
			if(from.matches("(.*)data.txt")){
				point.set(Integer.toString(dcount));
				position.set("D\t" + value.toString());
				context.write(point, position); 
				dcount++;
			}
			else if(value.toString().charAt(0) != 'C'){
				for(int i=0; i<POINTS; i++){
					point.set(Integer.toString(i));
					position.set(Integer.toString(ccount) + " " + value.toString());
					context.write(point, position); 
				}
				ccount++;
			}
		}
	}

	public static class KMReducer1 extends Reducer<Text, Text, Text, Text> {
		private Text belong = new Text();
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {	
			String p = "";
			double min = Double.MAX_VALUE;
			int minC = 0;
			double cost = 0;
			ArrayList<String> clusters = new ArrayList<String>();
			for (Text val : values) {
				String[] input = val.toString().split("\t");
				if(input[0].equals("D")) p = input[1];
				else clusters.add(val.toString());
			}
			for(String c : clusters){
				double distance = 0;
				String[] a = c.toString().split(" ");
				String[] b = p.toString().split(" ");
				for(int i=0; i<DIM; i++){
					distance += (Double.parseDouble(b[i]) - Double.parseDouble(a[i+1]))*(Double.parseDouble(b[i]) - Double.parseDouble(a[i+1]));
				}
				if(distance < min){
					min = distance;
					minC = Integer.parseInt(a[0]);
				}
			}
			belong.set(Integer.toString(minC));
			result.set(p);
			context.write(belong, result);
			belong.set("COST");
			result.set(Double.toString(min));
			context.write(belong, result);
		}
	}

	public static class KMMapper2 extends Mapper<Object, Text, Text, Text>{
		private Text c = new Text();
		private Text position = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t");	
			c.set(input[0]);
			position.set(input[1]);
			context.write(c, position);
		}
	}

	public static class KMReducer2 extends Reducer<Text, Text, Text, Text> {
		private Text cost_s = new Text();
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {
			int count = 0;
			double cost = 0;
			String sum_s = "";
			double[] sum = new double[DIM];
			if(key.toString().equals("COST")){
				for (Text val : values) {
					cost += Double.parseDouble(val.toString());
				}
				cost_s.set("COST");
				result.set(Double.toString(cost));
				context.write(cost_s, result);
			}
			else{
				for (Text val : values) {
					String[] input = val.toString().split(" ");
					for(int i=0; i<input.length; i++){
						sum[i] += Double.parseDouble(input[i]);
					}
				count++;
				}
				for(int i=0; i<DIM; i++){
					sum_s += " " + Double.toString(sum[i]/count);
				}
				result.set(sum_s.trim());
				context.write(result, null);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: KMeansE <in1> <in2> <out>");
			System.exit(2);
		}
		FileSystem hdfs = FileSystem.get(conf);
		
		ArrayList<String> cost = new ArrayList<String>();
		for(int i=0; i<MAX_ITER; i++){
			Job job1 = new Job(conf, "KMeansE step 1");
			job1.setJarByClass(KMeansE.class);
			job1.setMapperClass(KMMapper1.class);
			job1.setReducerClass(KMReducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
			if(i == 0)FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
			else FileInputFormat.addInputPath(job1, new Path(otherArgs[2]));
			FileOutputFormat.setOutputPath(job1, new Path("output/tmp"));
			job1.waitForCompletion(true);
			
			if(i != 0)hdfs.delete(new Path(otherArgs[2]), true);
			
			Job job2 = new Job(conf, "KMeansE step 2");
			job2.setJarByClass(KMeansE.class);
			job2.setMapperClass(KMMapper2.class);
			job2.setReducerClass(KMReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path("output/tmp"));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			job2.waitForCompletion(true);

			hdfs.delete(new Path("output/tmp"), true);		
			
			FSDataInputStream inStream = hdfs.open(new Path(otherArgs[2] + "/part-r-00000"));
			BufferedReader in = new BufferedReader(new InputStreamReader(inStream, "UTF-8"));
			String line;
			while ((line = in.readLine()) != null){ 
				if(line.charAt(0) == 'C'){
					cost.add(line);
					System.out.println("========= round " + Integer.toString(i+1) + " " + line + " ==========\n");
					break;
				}
			}
			in.close();
			inStream.close();
			
			//System.out.println("========= round " + Integer.toString(i+1) + " finished! ==========\n");
		}
		
		for(int i=0; i<cost.size(); i++)System.out.println("round " + Integer.toString(i+1) + " " + cost.get(i));
		
		ArrayList<String> pList = new ArrayList<String>();
		FSDataInputStream inStream = hdfs.open(new Path(otherArgs[2] + "/part-r-00000"));
		BufferedReader in = new BufferedReader(new InputStreamReader(inStream, "UTF-8"));
		String line;
		double distance = 0;
		while ((line = in.readLine()) != null){ 
			if(line.charAt(0) != 'C'){
				pList.add(line);
			}
		}
		for(int i=0; i<pList.size(); i++){
			for(int j=0; j<pList.size(); j++){
				distance = 0;
				String[] a = pList.get(i).toString().split(" ");
				String[] b = pList.get(j).toString().split(" ");
				for(int k=0; k<DIM; k++){
					distance += (Double.parseDouble(b[k]) - Double.parseDouble(a[k]))*(Double.parseDouble(b[k]) - Double.parseDouble(a[k]));
				}
				System.out.print(Double.toString(Math.sqrt(distance)) + " ");
			}
			System.out.print("\n");
		}
		in.close();
		inStream.close();
		
		System.exit(0);
	}
}
