package stubs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import org.junit.Before;
import org.junit.Test;


public class TestMillionSongTCS {

  MapDriver<Text, Text, IntWritable, Text> mapDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
    MillionSongTCSMappper mapper = new MillionSongTCSMappper();
    mapDriver = new MapDriver<Text, Text, IntWritable, Text>();
    mapDriver.setMapper(mapper);

  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

    mapDriver.withInput(new Text("the"), new Text("3"));
    mapDriver.withOutput(new IntWritable(3), new Text("the"));

    mapDriver.runTest();

  }
  
}
