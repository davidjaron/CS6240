package pr;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import bfs.CustomCounter;

public class AllPairs extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(AllPairs.class);

  /**
   * The mapper reads through each line from the input file and gets the structure of the graph.
   * It calculates the pagerank for the given vertex by using the overflow and size variable setup
   * in the job context from the previous job. It also emits each adjacent vertex and adds its
   * pagerank / adjacent pages to it.
   */

  public static class AllPairMapper extends Mapper<LongWritable, Text, IntWritable, Vertex> {
    private HashMap<Integer, List<AdjacentVertex>> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      URI[] files = context.getCacheFiles();
      FileSystem fs = FileSystem.get(context.getConfiguration());
      Path path = new Path(files[0].toString());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
      String line;
      String[] lines;
      line = reader.readLine();
      while (line != null) {
        line = line.replace("\uFEFF", "");
        lines = line.split(" ");
        int key = Integer.parseInt(lines[0]);
        int size = Integer.parseInt(lines[1]);
        map.put(key, new ArrayList<>());
        String[] adjacent = lines[2].split("\\|");
        for (String temp : adjacent){
          String[] adj = temp.split(",");
          if (!map.containsKey(Integer.parseInt(adj[0]))){
            map.put(Integer.parseInt(adj[0]), new ArrayList<>());
          }
          map.get(key).add(new AdjacentVertex(Integer.parseInt(adj[0]), Integer.parseInt(adj[1])));
          if (!map.containsKey(Integer.parseInt(adj[0]))){
            map.put(Integer.parseInt(adj[0]), new ArrayList<>());
          }
        }
        line = reader.readLine();
      }
      context.getCounter(CustomCounter.CHANGED).getValue();
    }



    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws
            IOException,
            InterruptedException {

      Vertex v = new Vertex();
      List<AdjacentVertex> adjacents = new ArrayList<>();

      String valueString = value.toString();
      String[] firstSplit = valueString.split(" ");

      int vertexNum = Integer.parseInt(firstSplit[0]);
      int size = Integer.parseInt(firstSplit[0]);

      v.setPage(vertexNum);
      String[] adjacentString = firstSplit[2].split("\\|");

      for (String temp : adjacentString){
        String[] tempSplit = temp.split(",");
        AdjacentVertex adj = new AdjacentVertex(Integer.parseInt(tempSplit[0]), Integer.parseInt(tempSplit[1]));
        adjacents.add(adj);
      }

      HashMap<Integer, List<Integer>> lowestCost = new HashMap<>();

      for (AdjacentVertex j : adjacents){
        if (!lowestCost.containsKey(j.getVertex())){
          List<Integer> costs = new ArrayList<>();
          costs.add(j.getCost());
          lowestCost.put(j.getVertex(), costs);
        } else {
          lowestCost.get(j.getVertex()).add(j.getCost());
        }
        for (AdjacentVertex temp : map.get(j.vertex)){
          if (!lowestCost.containsKey(temp.getVertex())){
            List<Integer> costs = new ArrayList<>();
            costs.add(temp.getCost() + j.getCost());
            lowestCost.put(temp.getVertex(), costs);
          } else {
            lowestCost.get(temp.getVertex()).add(temp.getCost()+j.getCost());
          }
        }
      }
      for (int k : lowestCost.keySet()){
        int min = Integer.MAX_VALUE;
        for (int cost : lowestCost.get(k)){
          min = Math.min(cost, min);
        }
        v.addAdjacent(new AdjacentVertex(k, min));
      }

      if (!adjacents.containsAll(v.getAdjacent())){
        context.getCounter(CustomCounter.CHANGED).increment(1);
      }


      context.write(new IntWritable(v.getPage()), v);


    }
  }

  /**
   * For each key iterate through all the vertices the reducer receives associated with that key.
   * Reccomputes the graph strucutre if the vertex received in the vertex itself, and add to the
   * sum of pagerank otherwise. Sets the pagerank to be the sum of all prs incoming to the vertex
   * and emits the vertex # and its graph structure/pagerank
   */

//  public static class VertexReducer extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {
//
//    @Override
//    public void reduce(final IntWritable key, final Iterable<Vertex> values, final Context context)
//            throws IOException, InterruptedException {
//      double s = 0;
//      Vertex m = null;
//
//      for (final Vertex ver : values) {
//        if (ver.isVertex()) {
//          Vertex temp = new Vertex();
//          temp.setAdjacent(ver.getAdjacent());
//          temp.setPage(ver.getPage());
//          m = temp;
//          // Used for when the key == 0. Increments counter for overflow which is used in the
//          // next job.
//        } else if (ver.getPage() == 0){
//          context.getCounter(CustomCounter.OVERFLOW).increment((long) (ver.getPageRank() *
//                  10000000));
//        } else {
//          s += ver.getPageRank();
//        }
//      }
//
//      if (m != null){
//        int p = m.getPage();
//        // Multiples the sum by .85 to get the value from incoming edges
//        m.setPageRank(.85 * s);
//        context.write(new IntWritable(p), m);
//      }
//    }
//  }

  @Override
  public int run(final String[] args) throws Exception {
    int i = 0;
    long changed = 1;
    // Iterates through each job, creating new output and input folders for each iteration.

    // Final output, map only job which produces final AllPairs numbers

    ToAdjacency.main(args);

    while (changed > 0){
      final Configuration initialConf = getConf();
      final Job initialJob = Job.getInstance(initialConf, "step" + i);
      initialJob.setJarByClass(AllPairs.class);
      final Configuration initialJobConfiguration= initialJob.getConfiguration();
      initialJobConfiguration.set("mapreduce.output.textoutputformat.separator", " ");
      // Delete output directory, only to ease local development; will not work on AWS. ===========
      final FileSystem fileSystem = FileSystem.get(initialConf);
      if (fileSystem.exists(new Path(args[1]))) {
        fileSystem.delete(new Path(args[1]), true);
      }
      // ================
      initialJob.setMapperClass(AllPairMapper.class);
      initialJob.setNumReduceTasks(0);
      initialJob.setOutputKeyClass(IntWritable.class);
      initialJob.setOutputValueClass(Vertex.class);
      FileInputFormat.addInputPath(initialJob, new Path(args[0] + (i-1)));
      File folder = new File(args[0] + (i-1));
      File[] listOfFiles = folder.listFiles();
      if (listOfFiles != null) {
        for (File f : listOfFiles){
          if (!f.getName().substring(0, 1).equals(".") && !f.getName().substring(0, 1).equals("_")){
            initialJob.addCacheFile(new Path(f.getPath()).toUri());
          }

        }
      }
      FileOutputFormat.setOutputPath(initialJob, new Path(args[0] + i));
      int status = initialJob.waitForCompletion(true) ? 0 : 1;
      changed = initialJob.getCounters().findCounter(CustomCounter.CHANGED).getValue();
      i++;
    }
    return 0;
  }

  public static void main(final String[] args) {
    String[] test = new String[2];
    test[0] = "/Users/davidaron/Documents/CS6240/MR-Demo/input";
    test[1] = "/Users/davidaron/Documents/CS6240/MR-Demo/output";


    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new AllPairs(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}
