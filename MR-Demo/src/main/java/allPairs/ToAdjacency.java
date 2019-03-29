package allPairs;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ToAdjacency extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(AllPairs.class);

  /**
   * The mapper reads through each line from the input file and gets the structure of the graph.
   * It calculates the pagerank for the given vertex by using the overflow and size variable setup
   * in the job context from the previous job. It also emits each adjacent vertex and adds its
   * pagerank / adjacent pages to it.
   */

  public static class ToAdjacencyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private HashMap<Integer, List<AdjacentVertex>> map = new HashMap<>();

    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws
            IOException,
            InterruptedException {

      int k = Integer.parseInt(context.getConfiguration().get("kValue"));
      String[] values = value.toString().split(",");
      int keyVertex = Integer.parseInt(values[0]);
      int valueVertex = Integer.parseInt(values[1]);
      if (keyVertex < k && valueVertex < k){
        context.write(new IntWritable(keyVertex), new IntWritable(valueVertex));
      }

    }
  }

  /**
   * For each key iterate through all the vertices the reducer receives associated with that key.
   * Reccomputes the graph strucutre if the vertex received in the vertex itself, and add to the
   * sum of pagerank otherwise. Sets the pagerank to be the sum of all prs incoming to the vertex
   * and emits the vertex # and its graph structure/pagerank
   */

  public static class AdjacencyReducer extends Reducer<IntWritable, IntWritable, IntWritable,
          Vertex> {

    @Override
    public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
      Vertex v = new Vertex();
      v.setPage(key.get());

      for (IntWritable temp : values) {
        v.addAdjacent(new AdjacentVertex(temp.get(), 1));
      }
      context.write(new IntWritable(v.getPage()), v);
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    int runs = 100;
    int i = 0;
    long size = 0;
    long overflow = 0;
    // Iterates through each job, creating new output and input folders for each iteration.

    // Final output, map only job which produces final AllPairs numbers

    final Configuration initialConf = getConf();
    initialConf.setDouble("dangling", overflow);
    initialConf.setLong("size", size);
    final Job initialJob = Job.getInstance(initialConf, "step" + i);
    initialJob.setJarByClass(ToAdjacency.class);
    final Configuration initialJobConfiguration= initialJob.getConfiguration();
    initialJobConfiguration.set("mapreduce.output.textoutputformat.separator", " ");
    initialJobConfiguration.set("kValue", "5000");
    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    final FileSystem fileSystem = FileSystem.get(initialConf);
////    if (fileSystem.exists(new Path(args[1]))) {
////      fileSystem.delete(new Path(args[1]), true);
////    }
    // ================
    initialJob.setMapperClass(ToAdjacencyMapper.class);
    initialJob.setReducerClass(AdjacencyReducer.class);
    initialJob.setOutputKeyClass(IntWritable.class);
    initialJob.setMapOutputValueClass(IntWritable.class);
    initialJob.setOutputValueClass(Vertex.class);
    FileInputFormat.addInputPath(initialJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(initialJob, new Path(args[0] + -1));
    //initialJob.addCacheFile(new Path(args[0] + "/test.txt").toUri());
    return initialJob.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    String[] test = new String[2];
    test[0] = "/Users/davidaron/Documents/CS6240/MR-Demo/input";
    test[1] = "/Users/davidaron/Documents/CS6240/MR-Demo/output";


    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new ToAdjacency(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}
