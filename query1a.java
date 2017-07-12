import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class query1a {
	public static class Myclass extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String ss[] = value.toString().split("\t");
			int a=Integer.parseInt(ss[7]);
			if(ss[4].toString().contains("DATA ENGINEER"))
			context.write(new Text(ss[7]),new IntWritable(a));
		}
	}
	public static class Myreducer extends Reducer<Text,IntWritable,NullWritable,Text>
	{
		TreeMap<Double,Text> val = new TreeMap<Double,Text>();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
		{

		long count1 = 0,count2=0,count3=0,count4=0,count5=0,count6=0;
			
			String myval = "";
			
			double growth1 = 0.0,growth2=0.0,growth3=0.0,growth4=0.0,growth5=0.0,avggrowth=0.0;
			
			for(IntWritable t1:values)
			{
			    
				if(t1.get()==2011)
				{
					count1++;
				}
				if(t1.get()==2012)
				{
					count2++;
				}
				if(t1.get()==2013)
				{
					count3++;
				}
				if(t1.get()==2014)
				{
					count4++;
				}
				if(t1.get()==2015)
				{
					count5++;
				}
				if(t1.get()==2016)
				{
					count6++;
				}
			}
			if( count2 > count1)                  
			{
				if(count1==0)                      
				{
					growth1 = (count2)*100;
				}
				else
				{
				growth1 = ((count2-count1)/count1)*100;
				}
			}
			
			else if(count1!=0 && count1 > count2)          
			{
				growth1 = ((count2-count1)/count1)*100;
			}
			
			if(count2 !=0 && count3 > count2)
			{
				growth2 = ((count3-count2)/count2)*100;
			}
			else if(count2!=0 && count2 > count3)
			{
				growth2 = ((count3-count2)/count2)*100;
			}
			
			if(count3 !=0 && count4 > count3)
			{
				growth3 = ((count4-count3)/count3)*100;
			}
			else if(count3!=0 && count3 > count4)
			{
				growth3 = ((count4-count3)/(double)count3)*100;
			}
			
			if(count4 !=0 && count5 > count4)
			{
				growth4 = ((count5-count4)/count4)*100;
			}
			else if(count4!=0 && count4 > count5)
			{
				growth4 = ((count5-count4)/count4)*100;
			}
			
			if(count5!=0 && count6 > count5)
			{
				growth5 = ((count6-count5)/count5)*100;
			}
			else if(count5!=0 && count5 > count6)
			{
				growth5 = ((count6-count5)*100/count5)*100;
			}
		
			avggrowth = (growth1+growth2+growth3+growth4+growth5)/5;
			myval = key.toString();
		myval = myval+"-"+avggrowth;
		
			val.put(avggrowth,new Text(myval) );
			if(val.size()>5)
			{
				val.remove(val.lastKey());
			}
		
		}
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			for(Text m:val.descendingMap().values())
			
				context.write(NullWritable.get(), new Text(val.toString()));
			
		}
	}
	public static void  main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"new job");
		job.setJarByClass(query1a.class);
		job.setMapperClass(Myclass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(Myreducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
