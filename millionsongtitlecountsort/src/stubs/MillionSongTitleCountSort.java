package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MillionSongTitleCountSort {

  public class NonSplittableTextInputFormat extends TextInputFormat {
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
  }
  
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: MillionSongTitleCountSort <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job();
    
    job.setJarByClass(MillionSongTitleCountSort.class);
    
    job.setJobName("Million Song Title Count Sort");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MillionSongTCSMappper.class);
    
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setSortComparatorClass(IntComparator.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    job.setNumReduceTasks(1);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

