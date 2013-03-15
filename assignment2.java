/*******************************************************
 *  Class name: assignment2
 *  Author: Viswaakshan
 *  Description: Apache Hadoop MapReduce Homework 1.2
 *  			to find top 10 most frequent visitees
 ********************************************************/
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class assignment2 {
	//Mapper 1: to get Visitee Count from CSV input	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			List<String> csvFields = Arrays.asList(line.split(",",-1));
                String visiteeJoinName = csvFields.get(19) + " " +csvFields.get(20);
                String Output1 = visiteeJoinName.toLowerCase();
                word.set(Output1);
                context.write(word, one);
		}
	}
	//Mapper 2: to get Top 10 Visitees from Intermediate Output emitted by WordCount Mapper
	public static class Top10Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> localTop10 = new TreeMap<Integer, Text>();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\t");
			String visitorName = "Null";
			int visitorCount = 0;
			if (tokenizer.hasMoreTokens()) {
				visitorName = tokenizer.nextToken();
			}
			if (tokenizer.hasMoreTokens()) {
				visitorCount = Integer.parseInt(tokenizer.nextToken());
			}
			localTop10.put(visitorCount, new Text(visitorCount + "\t" + visitorName));
			if (localTop10.size() > 10) {
				localTop10.remove(localTop10.firstKey());
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text t : localTop10.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	//Reducer 1 for WordCount Mapper
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				//IntWritable value = (IntWritable) values.next();
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	//Reducer 2 for Top10Mapper
	public static class Top10Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> localTop10 = new TreeMap<Integer, Text>();
		private Text finalOutput = new Text();
		private int ctr = 0;

		public void reduce(NullWritable key, Iterator<Text> values,
				Context context) throws IOException, InterruptedException {
			while (values.hasNext()) {
				String[] outputStr = values.next().toString().split("\t");
				String visitorName = outputStr[1];
				int visitorCount = Integer.parseInt(outputStr[0]);
				finalOutput.set("" + visitorCount + "\t" + visitorName);
				localTop10.put(visitorCount, finalOutput);
				// Output local top 10 records with null key
				if (localTop10.size() > 10) {
					localTop10.remove(localTop10.firstKey());
				}
			}
			for (Text t : localTop10.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1);
		job1.setJobName("MapReduce1");
		job1.setJarByClass(assignment2.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(Map.class);
		job1.setCombinerClass(Reduce.class);
		job1.setReducerClass(Reduce.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.waitForCompletion(true);

		if (job1.isSuccessful()) {
			Configuration conf2 = new Configuration();
			Job job2 = new Job(conf2, "MapReduce2");
			job2.setJarByClass(assignment2.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);

			job2.setMapperClass(Top10Mapper.class);
			job2.setCombinerClass(Top10Reducer.class);
			job2.setReducerClass(Top10Reducer.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));

			job2.setNumReduceTasks(1);
			job2.waitForCompletion(true);
		}
	}
}
