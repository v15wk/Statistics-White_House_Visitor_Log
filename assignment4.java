/*******************************************************
 *  Class name: assignment4
 *  Author: Viswaakshan
 *  Description: Apache Hadoop MapReduce Homework 1.4
 *  			to find the Average visits per month	
 ********************************************************/
import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class assignment4{
	// Mapper 1: to get Visitor Count from CSV input
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// Split the CSV fields using the Comma delimiter and store in String Array
			String[] csvFields = line.split(","); 

			if (csvFields.length > 1) {
				String field = csvFields[11];
				// Derives the Month and Year values by pattern matching
				Pattern pattern = Pattern.compile("(\\d+)\\/(\\d+)\\/(\\d+)");
				Matcher m = pattern.matcher(field);
				IntWritable month = null;
				IntWritable year = new IntWritable(2009);
				int year_int = 2009;
				if (m.find()) {
					month = new IntWritable(Integer.parseInt(m.group(1)));
					year_int = Integer.parseInt(m.group(3));
					if (year_int > 2001 && year_int < 2013) {
						year = new IntWritable(year_int);
					}
				}
			if (month != null)
			context.write(month, year);
		}
	}
}

// Reducer 1 for WordCount Mapper
public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		HashSet<Integer> hash = new HashSet<Integer>();
		int count = 0;
		for (IntWritable val : values) {
			count++;
			hash.add(val.get());
		}
		int unique = hash.size();
		int average = count / unique;
		// output intermediate value and key pair
		context.write(key, new IntWritable(average));
	}
}

public static void main(String[] args) throws Exception {
	Configuration conf1 = new Configuration();
	Job job1 = new Job(conf1);
	job1.setJobName("assignment4");

	job1.setJarByClass(assignment4.class);
	job1.setOutputKeyClass(IntWritable.class);
	job1.setOutputValueClass(IntWritable.class);

	job1.setMapperClass(Map.class);
	job1.setReducerClass(Reduce.class);

	FileInputFormat.addInputPath(job1, new Path(args[0]));
	FileOutputFormat.setOutputPath(job1, new Path(args[1]));

	job1.waitForCompletion(true);

	}
}