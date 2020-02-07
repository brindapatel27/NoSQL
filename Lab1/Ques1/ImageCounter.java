package ImageCounter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class ImageCounter 
{
	public static class ImageMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
		{
			String str = value.toString();
			if (str.contains("GET") && str.contains("images")) {
				if(str.contains(".gif"))
				{
					value.set("GIF");
					context.write(value, new IntWritable(1));
				}
				else if(str.contains(".jpg"))
				{
					value.set("JPG");
					context.write(value, new IntWritable(1));
				}
				else 
				{
					value.set("OTHERS");
					context.write(value, new IntWritable(1));
				}
			}
		}
	}
	
	public static class ImageReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			int count=0;
			
			for(IntWritable x: values)
			{
				count += x.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf,"ImageCounter");
		job.setJarByClass(ImageCounter.class);
		
		job.setMapperClass(ImageMapper.class);
		job.setReducerClass(ImageReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		outputPath.getFileSystem(conf).delete(outputPath,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}