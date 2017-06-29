import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class query5a {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split("\t");
			context.write(new Text(arr[4]), new Text(arr[7]+" "+arr[4]));
	}}
	public static class Mypartitioner extends Partitioner<Text, Text>
	{	
		public int getPartition(Text key, Text value, int arg2) {
	        String s[]=value.toString().split(" ");
	        int i=Integer.parseInt(s[0]);
	        if(i==2011)
	        {
	        	return 0;
	        }
	        if(i==2012)
	        {
	        	return 1;
	        }
	        if(i==2013)
	        {
	        	return 2;
	        }
	        if(i==2014)
	        {
	        	return 3;
	        }
	        if(i==2015)
	        {
	        	return 4;
	        }
			if(i==2016)
			{
				return 5;
			}
			return i;
		}}
public static class Myreducer extends Reducer<Text,Text,Text,Text>
{
	TreeMap<Integer, String> hs=new TreeMap<Integer, String>();
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
	
		int count=0;
		
		for(Text a:value){
		String ss[]=a.toString().split(",");
		count++;
		hs.put(count,ss[1]);
		if(hs.size()>10){
			hs.remove(hs.firstKey());
		}}
		context.write(key, new Text(hs.toString()));
	}}
	
public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"work");
	job.setJarByClass(query5a.class);
	
	job.setMapperClass(Mymapper.class);
	job.setReducerClass(Myreducer.class);
	job.setPartitionerClass(Mypartitioner.class);
	job.setNumReduceTasks(6);
	job.setCombinerClass(Myreducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileSystem.get(conf).delete(new Path(args[1]),true);
	FileInputFormat.addInputPath(job, new Path(args[0]));	
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)?0:1);

}

}
