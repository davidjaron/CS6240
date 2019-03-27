//package pr;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class OutputMapper extends Mapper<Object, Text, IntWritable, Vertex> {
//    public final Vertex v = new Vertex();
//    private long size;
//    private double dangling;
//
//  @Override
//  protected void setup(Context context) throws IOException, InterruptedException {
//    this.size = context.getConfiguration().getLong("size", 0);
//    this.dangling = context.getConfiguration().getDouble("dangling", 0.0)/100;
//  }
//
//  @Override
//    public void map(final Object key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
//
//    String test = value.toString();
//    String[] line = test.split("\t");
//    v.setVertex(true);
//    v.setPage(Integer.parseInt(line[0]));
//
//    String[] vertexInfo = line[1].split(" ");
//    Double rank = Double.parseDouble(vertexInfo[0]);
//    if (this.size == 0){
//      v.setPageRank(rank);
//    } else {
//      v.setPageRank(rank + .15/size + dangling/size);
//    }
//
//    context.write(new IntWritable(v.getPage()), v);
//    }
//  }
