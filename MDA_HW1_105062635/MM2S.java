package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MM2S {
	public static class MMMapper1 extends Mapper<Object, Text, Text, Text>{
		private Text j = new Text();
		private Text m = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split(",");
			if(input[0].equals("M")){
				j.set(input[2]);
				m.set(input[0] + "," + input[1] + "," + input[3]);
			}
			else{
				j.set(input[1]);
				m.set(input[0] + "," + input[2] + "," + input[3]);
			}
			context.write(j, m); 
		}
	}

	public static class MMReducer1 extends Reducer<Text, Text, Text, Text> {
		private Text newkey = new Text();
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {
			int height = 0, width = 0, index = 0, scalar = 0;
			int[] M = new int[1000];
			int[] N = new int[1000];

			for (Text val : values) {
				String[] input = val.toString().split(",");
				index = Integer.parseInt(input[1]);
				scalar = Integer.parseInt(input[2]);
				if(input[0].equals("M")){
					M[index] = scalar;
					if(height < index)height = index;
				}
				else {
					N[index] = scalar;
					if(width < index)width = index;
				}
			}
			for(int i = 0; i < height + 1; i++){
				for(int j = 0; j < width + 1; j++){
					newkey.set(Integer.toString(i) + "," + Integer.toString(j));
					result.set(Integer.toString(M[i]*N[j]));
					context.write(newkey, result);
				}
			}
		}
	}

	public static class MMMapper2 extends Mapper<Object, Text, Text, Text>{
		private Text j = new Text();
		private Text m = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\\t");
			j.set(input[0]);
			m.set(input[1]);
			context.write(j, m); 
		}
	}

	public static class MMReducer2 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,	Context context	) throws IOException, InterruptedException {
			int sum = 0;

			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			result.set(Integer.toString(sum));
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "matrix multiplication step 1");
		job1.setJarByClass(MM2S.class);
		job1.setMapperClass(MMMapper1.class);
		job1.setReducerClass(MMReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path("output/tmp"));
		job1.waitForCompletion(true);
		
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job2 = new Job(conf, "matrix multiplication step 2");
		job2.setJarByClass(MM2S.class);
		job2.setMapperClass(MMMapper2.class);
		job2.setReducerClass(MMReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("output/tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		job2.waitForCompletion(true);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path("output/tmp"), true);
		System.exit(0);
	}
}
