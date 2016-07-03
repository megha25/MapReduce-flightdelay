import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OneStopFlightDriver extends Configured implements Tool{
 
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new OneStopFlightDriver(),args);
    System.exit(exitCode);
  }
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
		      System.err.println("Usage: Join <input path> <output path>");
		      System.exit(-1);
		    }

		    Job job = new Job(getConf());
		    job.setJarByClass(OneStopFlightDriver.class);
		    job.setJobName("join");

		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    job.setMapperClass(OneStopFlightMapper.class);
		    job.setReducerClass(OneStopFlightReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(OneStopTuplePair.class);

		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    job.setNumReduceTasks(4);
		    return(job.waitForCompletion(true) ? 0 : 1);
		}
}
