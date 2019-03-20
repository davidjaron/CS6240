package pr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class SyntheticGraph extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(SyntheticGraph.class);

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Vertex> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
//			final StringTokenizer itr = new StringTokenizer(value.toString());
			int k =Integer.parseInt(value.toString());
//			while (itr.hasMoreTokens()) {
//					word.set(itr.nextToken());
//				context.write(word, one);
//			}
			int curr = 1;
			for (int i = 0; i < k; i++){
				for (int j = 0; j < k; j++ ){
					if (curr % k == 0){
						Vertex v = new Vertex();
						v.addAdjacent(0);
						v.setPageRank(0);
						v.setPageRank(1.0/(k*k));
						context.write(new IntWritable(curr), v);
					} else {
						Vertex v = new Vertex();
						v.addAdjacent(curr+1);
						v.setPageRank(1.0/(k*k));
						context.write(new IntWritable(curr), v);
					}
					curr++;
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(SyntheticGraph.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Vertex.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		String[] test = new String[2];
		test[0] = "/Users/davidaron/Documents/CS6240HW4/MR-Demo/syntheticInput";
		test[1] = "/Users/davidaron/Documents/CS6240HW4/MR-Demo/prStart";


		if (test.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new SyntheticGraph(), test);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}