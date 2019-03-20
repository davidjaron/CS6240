package pr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS;

public class PageRank extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(PageRank.class);

  /**
   * The mapper reads through each line from the input file and gets the structure of the graph.
   * It calculates the pagerank for the given vertex by using the overflow and size variable setup
   * in the job context from the previous job. It also emits each adjacent vertex and adds its
   * pagerank / adjacent pages to it.
   */

  public static class VertexMapper extends Mapper<Object, Text, IntWritable, Vertex> {
    public final Vertex v = new Vertex();
    private long size;
    private double dangling;

    @Override
    protected void setup(Context context) {
      this.size = context.getConfiguration().getLong("size", 0);
      this.dangling = context.getConfiguration().getDouble("dangling", 0.0) / 1000000000;
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
//			final StringTokenizer itr = new StringTokenizer(value.toString());
//			while (itr.hasMoreTokens()) {
//					word.set(itr.nextToken());
//				context.write(word, one);
//			}
      String test = value.toString();
      String[] line = test.split("\t");
      v.setVertex(true);
      v.setPage(Integer.parseInt(line[0]));

      String[] vertexInfo = line[1].split(" ");
      Double rank = Double.parseDouble(vertexInfo[0]);
      // First iteration does not have size yet so just rank is set as rank for v
      if (this.size == 0){
        v.setPageRank(rank);
        // In all other iterations the rank is set using the pagerank emitted from previous run,
        // and the overflow from the dummy node
      } else {
        v.setPageRank(rank + .15/size + dangling/size);
      }
      int len = Integer.parseInt(vertexInfo[1]);

      // Emits the PR given to each adjacent vertex.
      List<Integer> adjacent = new ArrayList<>();
      for (int i = 0; i < len; i++){
        int temp = Integer.parseInt(vertexInfo[2+i]);
        adjacent.add(temp);
        Vertex adj = new Vertex();
        adj.setPage(temp);
        if (this.size == 0){
          adj.setPageRank(rank);
        } else {
          adj.setPageRank(v.getPageRank()/len);
        }
        context.write(new IntWritable(temp), adj);
      }
      v.setAdjacent(adjacent);
      context.write(new IntWritable(v.getPage()), v);
      context.getCounter(CustomCounter.TOTAL_VERTICES).increment(1);

    }
  }

  /**
   * For each key iterate through all the vertices the reducer receives associated with that key.
   * Reccomputes the graph strucutre if the vertex received in the vertex itself, and add to the
   * sum of pagerank otherwise. Sets the pagerank to be the sum of all prs incoming to the vertex
   * and emits the vertex # and its graph structure/pagerank
   */

  public static class VertexReducer extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {

    @Override
    public void reduce(final IntWritable key, final Iterable<Vertex> values, final Context context)
            throws IOException, InterruptedException {
      double s = 0;
      Vertex m = null;

      for (final Vertex ver : values) {
        if (ver.isVertex()) {
          Vertex temp = new Vertex();
          temp.setAdjacent(ver.getAdjacent());
          temp.setPage(ver.getPage());
          m = temp;
          // Used for when the key == 0. Increments counter for overflow which is used in the
          // next job.
        } else if (ver.getPage() == 0){
          context.getCounter(CustomCounter.OVERFLOW).increment((long) (ver.getPageRank() *
                  10000000));
        } else {
          s += ver.getPageRank();
        }
      }

      if (m != null){
        int p = m.getPage();
        // Multiples the sum by .85 to get the value from incoming edges
        m.setPageRank(.85 * s);
        context.write(new IntWritable(p), m);
      }
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    int runs = 10;
    int i = 0;
    long size = 0;
    long overflow = 0;
    // Iterates through each job, creating new output and input folders for each iteration.
    while (i < runs){
      final Configuration initialConf = getConf();
      final Job initialJob = Job.getInstance(initialConf, "step" + i);
      initialJob.setJarByClass(PageRank.class);
      final Configuration initialJobConfiguration= initialJob.getConfiguration();
      initialJobConfiguration.set("mapreduce.output.textoutputformat.separator", "\t");
      // Delete output directory, only to ease local development; will not work on AWS. ===========
//      final FileSystem fileSystem = FileSystem.get(initialConf);
//      if (fileSystem.exists(new Path(args[1]))) {
//        fileSystem.delete(new Path(args[1]), true);
//      }
      // ================
      initialJob.setInputFormatClass(NLineInputFormat.class);
      if (i == 0){
        NLineInputFormat.addInputPath(initialJob, new Path(args[0]));
      } else {
        NLineInputFormat.addInputPath(initialJob, new Path(args[0]+ (i-1)));
      }
      initialJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);
      initialJob.setMapperClass(VertexMapper.class);
      initialJob.setReducerClass(VertexReducer.class);
      initialJob.setOutputKeyClass(IntWritable.class);
      initialJob.setOutputValueClass(Vertex.class);
      //FileInputFormat.addInputPath(initialJob, new Path(args[0] + (i-1)));
      FileOutputFormat.setOutputPath(initialJob, new Path(args[0] + i));
      int success = initialJob.waitForCompletion(true) ? 0 : 1;
      if (success == 0){
        // Takes the value of overflow into the dummy node, and the size of the graph and passes
        // them to the next iteration of the job.
        size = initialJob.getCounters().findCounter(CustomCounter.TOTAL_VERTICES).getValue();
        overflow = initialJob.getCounters().findCounter(CustomCounter.OVERFLOW).getValue();
        initialConf.setDouble("dangling", overflow);
        initialConf.setLong("size", size);
      }
      i++;
    }

    // Final output, map only job which produces final PageRank numbers

    final Configuration initialConf = getConf();
    initialConf.setDouble("dangling", overflow);
    initialConf.setLong("size", size);
    final Job initialJob = Job.getInstance(initialConf, "step" + i);
    initialJob.setJarByClass(PageRank.class);
    final Configuration initialJobConfiguration= initialJob.getConfiguration();
    initialJobConfiguration.set("mapreduce.output.textoutputformat.separator", "\t");
    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    final FileSystem fileSystem = FileSystem.get(initialConf);
//    if (fileSystem.exists(new Path(args[1]))) {
//      fileSystem.delete(new Path(args[1]), true);
//    }
    // ================
    initialJob.setMapperClass(OutputMapper.class);
    initialJob.setNumReduceTasks(0);
    initialJob.setOutputKeyClass(IntWritable.class);
    initialJob.setOutputValueClass(Vertex.class);
    FileInputFormat.addInputPath(initialJob, new Path(args[0] + (i-1)));
    FileOutputFormat.setOutputPath(initialJob, new Path(args[1]));


    return initialJob.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    String[] test = new String[2];
    test[0] = "/Users/davidaron/Documents/CS6240HW4/MR-Demo/input";
    test[1] = "/Users/davidaron/Documents/CS6240HW4/MR-Demo/output";


    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new PageRank(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}
