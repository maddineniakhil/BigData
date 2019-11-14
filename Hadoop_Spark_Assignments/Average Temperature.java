  import java.io.IOException;
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
	import org.apache.log4j.Logger;
	


	public class Averagetemp {
		public static class AvgTemperatureMapper extends	Mapper<LongWritable, Text, Text, IntWritable> {
			private static final int MISSING = 9999;
			
			
			
			public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
				String line = value.toString();
				String year = line.substring(15, 19);
				int airTemperature;
				if (line.charAt(87) == '+') { 
					airTemperature= Integer.parseInt(line.substring(88, 92));
					} 
				else {
					airTemperature= Integer.parseInt(line.substring(87, 92));
					}
				String quality = line.substring(92, 93);
				if (airTemperature!= MISSING && quality.matches("[01459]")) 
				{
					
				
					
					context.write(new Text(year), new IntWritable(airTemperature));
					}
				}
			

	}
		public static class AvgTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			
			public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
				
				int sum1 = 0 ;
				int count1 =0;
				int average = 0;
				
				for (IntWritable value : values)
				{  
					count1++;
								sum1 = sum1 + value.get() ;
				}
				
				average = sum1/count1 ;
				
				context.write(key, new IntWritable(average));
				
			
				context.write(key, new IntWritable(Job.getInstance().getNumReduceTasks()));
				}
			}
		public static void main(String[] args) throws Exception {
			 Configuration conf = new Configuration();

			 Logger logger = Logger.getLogger("Averagetemp");
			 Job job = Job.getInstance(conf, "Average Temp");
			 job.setJarByClass(Averagetemp.class);
			 job.getJobName();
			 job.getNumReduceTasks();
			 job.getPartitionerClass().getName();
			 logger.info(job.getPartitionerClass().getName());
			 logger.info(job.getNumReduceTasks());
			 logger.info(job.getJobName());
			 job.setMapperClass(AvgTemperatureMapper.class);
			 //job.setCombinerClass(IntSumReducer.class);
			 job.setReducerClass(AvgTemperatureReducer.class);
			 job.setOutputKeyClass(Text.class);
			
			 job.setOutputValueClass(IntWritable.class);
			 FileInputFormat.addInputPath(job, new Path(args[0]));
			 FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
	}