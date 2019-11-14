import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixPartitioner {
public static class EmpPartitionMapper extends Mapper<LongWritable,Text, IntWritable, Text>
{
	              
  
public void map(LongWritable key, Text value,Context context)throws InterruptedException,IOException, NullPointerException
   {		
	   
	  
		String tokens=value.toString();	
		String tokens1= tokens.substring(3,4);
		 int empNo= Integer.parseInt(tokens1);
		
		if(empNo == 1 || empNo == 2 || empNo == 0 )
		{
				 Text otherDetails= value;
					context.write(new IntWritable(0) ,new Text(otherDetails));
         
	
}
		else if(empNo == 3 || empNo == 4 || empNo == 5 )
		{
				 Text otherDetails= value;
					context.write(new IntWritable(1),new Text(otherDetails));
			
}
		else
		{
				 Text otherDetails= value;
					context.write(new IntWritable(2),new Text(otherDetails));
		
		}
   }
}
public static class EmpPartitionReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
{
	
     public void reduce(IntWritable key, Text value,Context context)throws IOException,InterruptedException 
     {
 		
    	 
    		 context.write(key , value);
    		 
    	  }
     
		}


public static class EmpPartitioner<numberOfTasks> extends Partitioner<IntWritable,Text>
{
	
  public int getPartition(IntWritable key,Text value,int numReduceTasks)
	{
	  String tokens=value.toString();	
		String tokens1= tokens.substring(3,4);
		 int key1 = Integer.parseInt(tokens1);
		
		if(numReduceTasks==0)
			return 0;
	   if(key1 ==  1 || key1 == 0 || key1 == 2 )
			return 0; // part-r-00000
		else if(key1 == 3 || key1 == 4 || key1 ==5)
			return 1%numReduceTasks; //part-r-00001
		else
			return 2%numReduceTasks;// part-r-00003
	}




}

public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "MatrixPartitioner");
job.setJarByClass(MatrixPartitioner.class);


job.setMapperClass(EmpPartitionMapper.class);
job.setNumReduceTasks(3);
job.setPartitionerClass(EmpPartitioner.class);
job.setReducerClass(EmpPartitionReducer.class);

job.setOutputKeyClass(IntWritable.class);
job.setOutputValueClass(Text.class); 


FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
