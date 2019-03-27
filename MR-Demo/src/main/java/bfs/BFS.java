package bfs;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class BFS extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(BFS.class);

  public static class TokenizerMapper extends Mapper<Object, Text, Text, VertexStructure> {
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException,
            InterruptedException {
//			final StringTokenizer itr = new StringTokenizer(value.toString());
      String[] line = value.toString().split(",");
      VertexStructure v = new VertexStructure();
      v.setVertice(line[0]);
      v.setStructure(true);
      v.setActive(Boolean.parseBoolean(line[1]));
      if (v.getVertice().equals(context.getConfiguration().get("start-node"))){
        v.setActive(true);
      }
      int size = Integer.parseInt(line[2]);
      for (int i = 0; i < size; i++){
        v.addAdjacent(line[3+i]);
      }
      v.setLevel(Integer.parseInt(line[3+size]));
      v.setUsed(Boolean.parseBoolean(line[4+size]));

      if (v.isActive() && !v.isUsed()){
        v.setUsed(true);
        for (String adj : v.getAdjacent()){
          VertexStructure temp = new VertexStructure();
          temp.setActive(true);
          temp.setLevel(v.getLevel() + 1);
          temp.setVertice(adj);
          context.write(new Text(adj), temp);
        }
      }
      context.write(new Text(v.getVertice()), v);
      if (v.isUsed()){
        context.getCounter(CustomCounter.USED).increment(1);
      }
      context.getCounter(CustomCounter.SIZE).increment(1);
    }
  }

  public static class FinalMapper extends Mapper<Object, Text, NullWritable, Text> {
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException,
            InterruptedException {
//			final StringTokenizer itr = new StringTokenizer(value.toString());
      String[] line = value.toString().split(",");
      VertexStructure v = new VertexStructure();
      v.setVertice(line[0]);
      v.setStructure(true);
      v.setActive(Boolean.parseBoolean(line[1]));
      if (v.getVertice().equals(context.getConfiguration().get("start-node"))) {
        v.setActive(true);
      }
      int size = Integer.parseInt(line[2]);
      for (int i = 0; i < size; i++) {
        v.addAdjacent(line[3 + i]);
      }
      v.setLevel(Integer.parseInt(line[3 + size]));
      v.setUsed(Boolean.parseBoolean(line[4 + size]));

      context.write(null, new Text(v.getVertice() + " is at level " + v.getLevel()));
    }
  }

  public static class IntSumReducer extends Reducer<Text, VertexStructure, NullWritable, VertexStructure> {

    @Override
    public void reduce(final Text key, final Iterable<VertexStructure> values, final Context
            context)
            throws IOException, InterruptedException {

      VertexStructure structure = null;
      boolean active = false;
      int level = Integer.MAX_VALUE;

      for (VertexStructure temp : values){
        if (temp.isStructure()){
          structure = new VertexStructure();
          structure.setVertice(temp.getVertice());
          structure.setUsed(temp.isUsed());
          structure.setAdjacent(temp.getAdjacent());
          structure.setLevel(temp.getLevel());
        } else {
          if (temp.isActive()){
            active = true;
            level = temp.getLevel();
          }
        }
      }
      if (structure != null){
        structure.setActive(active);
        if (level != Integer.MAX_VALUE && !structure.isUsed()){
          structure.setLevel(level);
        }
      }
      context.write(null, structure);
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    long size = 100;
    long used = 0;
    int status = 1;
    int itr = 0;

    while (used < size){
      final Configuration conf = getConf();
      final Job job = Job.getInstance(conf, "step" + itr);
      job.setJarByClass(BFS.class);
      final Configuration jobConf = job.getConfiguration();
      jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
      if (itr == 0){
        jobConf.set("start-node", "s");
      } else {
        jobConf.set("start-node", "N/A");
      }

      // Delete output directory, only to ease local development; will not work on AWS. ===========
      final FileSystem fileSystem = FileSystem.get(conf);
      if (fileSystem.exists(new Path(args[1]))) {
        fileSystem.delete(new Path(args[1]), true);
      }
      // ================
      job.setMapperClass(TokenizerMapper.class);
      //job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
      job.setMapOutputValueClass(VertexStructure.class);
      job.setMapOutputKeyClass(Text.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(VertexStructure.class);
      if (itr == 0){
        FileInputFormat.addInputPath(job, new Path(args[0]));
      } else {
        FileInputFormat.addInputPath(job, new Path(args[0] + (itr-1)));
      }
      FileOutputFormat.setOutputPath(job, new Path(args[0] + itr));
      status = job.waitForCompletion(true) ? 0 : 1;
      used = job.getCounters().findCounter(CustomCounter.USED).getValue();
      size = job.getCounters().findCounter(CustomCounter.SIZE).getValue();
      itr++;
    }

    final Configuration initialConf = getConf();
    initialConf.setLong("size", size);
    final Job initialJob = Job.getInstance(initialConf, "produce output");
    initialJob.setJarByClass(BFS.class);
    final Configuration initialJobConfiguration= initialJob.getConfiguration();
    initialJobConfiguration.set("mapreduce.output.textoutputformat.separator", "\t");
    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    final FileSystem fileSystem = FileSystem.get(initialConf);
//    if (fileSystem.exists(new Path(args[1]))) {
//      fileSystem.delete(new Path(args[1]), true);
//    }
    // ================
    initialJob.setMapperClass(FinalMapper.class);
    initialJob.setNumReduceTasks(0);
    initialJob.setOutputKeyClass(NullWritable.class);
    initialJob.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(initialJob, new Path(args[0] + (itr-1)));
    FileOutputFormat.setOutputPath(initialJob, new Path(args[1]));
    return initialJob.waitForCompletion(true) ? 0 : 1;

  }

  public static void main(final String[] args) {
    String[] test = new String[2];
    test[0] = "/Users/davidaron/Documents/CS6240/MR-Demo/input";
    test[1] = "/Users/davidaron/Documents/CS6240/MR-Demo/output";


    if (test.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new BFS(), test);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}