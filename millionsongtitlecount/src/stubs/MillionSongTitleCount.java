package stubs;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;


public class MillionSongTitleCount extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: MillionSongTitleCount <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    Configuration conf = job.getConfiguration();
    job.setJobName("Millon Song Title Count");
    
    DistributedCache.addCacheFile(new URI("stopwords.txt"), conf);
    
    job.setJarByClass(MillionSongTitleCount.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MillionSongTitleCountMapper.class);
    job.setReducerClass(MillionSongTitleCountReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setNumReduceTasks(1);
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  	}


	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new MillionSongTitleCount(), args);
		System.exit(exitCode);
	}
}
