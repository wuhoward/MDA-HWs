package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MM1S {
	public static final int HEIGHT = 4;
	public static final int WIDTH = 4;
	public static final int COMMON = 2;
	public static class MMMapper extends Mapper<Object, Text, Text, Text>{

		private Text ik = new Text();
		private Text Mjm = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] input = value.toString().split(",");
			if(input[0].equals("M")){
				for(int i = 0; i < WIDTH; i++){
					ik.set(input[1] + "," + Integer.toString(i));
					Mjm.set(input[0] + "," + input[2] + "," + input[3]);
					context.write(ik, Mjm); 
				}
			}
			else{
				for(int i = 0; i < HEIGHT; i++){
					ik.set(Integer.toString(i) + "," + input[2]);
					Mjm.set(input[0] + "," + input[1] + "," + input[3]);
					context.write(ik, Mjm); 
				}	
			}
		}
	}

	public static class MMReducer
		extends Reducer<Text, Text, Text, Text> {
			private Text result = new Text();

			public void reduce(Text key, Iterable<Text> values,
					Context context
					) throws IOException, InterruptedException {
				int sum = 0;
				int[] M = new int[COMMON];
				int[] N = new int[COMMON];

				for (Text val : values) {
					String[] input = val.toString().split(",");
					if(input[0].equals("M"))M[Integer.parseInt(input[1])] = Integer.parseInt(input[2]);
					else N[Integer.parseInt(input[1])] = Integer.parseInt(input[2]);
				}
				for(int i = 0; i < COMMON; i++){
					sum += M[i]	* N[i];
				}
				result.set(Integer.toString(sum));
				context.write(key, result);
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MM1S <in> <out>");
			System.exit(2);
		}
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = new Job(conf, "matrix multiplication");
		job.setJarByClass(MM1S.class);
		job.setMapperClass(MMMapper.class);
		job.setReducerClass(MMReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
