package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.SequenceFile.CompressionType;  
import org.apache.hadoop.io.compress.SnappyCodec;


public class MillionSongParse {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: MillionSongParse <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(MillionSongParse.class);
    job.setJobName("Millon Song Parse");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
    
    job.setMapperClass(MillionSongMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
