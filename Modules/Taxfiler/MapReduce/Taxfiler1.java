
import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Taxfiler1 {

	public static class Mymapper1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (!columns[4].equals("Nonfiler")) {
				context.write(new Text("Filereducation"),new DoubleWritable(Double.parseDouble(columns[5])));
			}
		}

	}
	public static class Myreducer1 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			TreeMap<Double,String> max=new TreeMap<Double,String>();
			Double average=0.0;
			int check=0;
			double sum=0;
			for (DoubleWritable retreive : values) {
				max.put((Double)retreive.get(),"maximumincome");		
			}	
			NavigableMap<Double, String> nav=max.descendingMap();
			Set<Double> set=nav.keySet();
			Iterator<Double> itr=set.iterator();
				
			while(itr.hasNext()){
				if(check>10){
			    break; 		
				}	
				else
				{
					sum=sum+itr.next();
				}	
				check++;	
				}
			average=sum/(check+1);
			context.write(new Text("averageoftop5"),new DoubleWritable(average));
			context.write(new Text("highestincome"),new DoubleWritable(itr.next()));

		}
	}	
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Taxfiler1.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job, new Path("/census"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);
	}
}