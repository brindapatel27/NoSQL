package monthlySummaries;

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

public class MonthlySummaries 
{
	public static class SummariesMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
		{
			String str = value.toString();
			
			int i = 0;
			while(i<str.length() && str.charAt(i)!='['){
				i++;
			}
			i += 4;
			String month=String.valueOf(str.charAt(i))+String.valueOf(str.charAt(i+1))+String.valueOf(str.charAt(i+2));
			i += 4;
			String year=String.valueOf(str.charAt(i))+String.valueOf(str.charAt(i+1))+String.valueOf(str.charAt(i+2))+String.valueOf(str.charAt(i+3));
			
			value.set(month+"-"+year);
			while(i<str.length() && str.charAt(i)!='"'){
				i++;
			}
			i++;
			while(i<str.length() && str.charAt(i)!='"'){
				i++;
			}
		
			i++;
			
			while(i<str.length() && str.charAt(i)!=' ')
			{
				i++;
			}
			
			i+=2;
			
			while(i<str.length() && str.charAt(i)!=' ')
			{
				i++;
			}
			
			String sizestr="";
			int fsize=0;
			i++;
			
			if(str.charAt(i)!='-')
			{
				while(i<str.length() && str.charAt(i)!=' ')
				{
					sizestr+=str.charAt(i);
					i++;
				}
				fsize=Integer.parseInt(sizestr);
			}
			
			context.write(value ,new IntWritable(fsize));
		}
	}
	
	public static class SummariesReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			int sum=0;
			int count=0;
			for(IntWritable x: values)
			{
				sum+=x.get();
				count++;
			}
			
			String scnt=Integer.toString(count);
			
			String nkey = key.toString();
			key.set(nkey+" "+scnt);
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf,"dataSize");
		job.setJarByClass(MonthlySummaries.class);
		
		job.setMapperClass(SummariesMapper.class);
		job.setReducerClass(SummariesReducer.class);
		
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