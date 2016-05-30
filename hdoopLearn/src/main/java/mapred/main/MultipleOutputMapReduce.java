package mapred.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.FileUtils;
import utils.NcdcRecordParser;

/**
 * 作用：一个reduce写多个文件，例如按照stationId划分文件
 * 1.好像setmapper的output类型没有必要，只要设置了reduce的ouput类型就能正常
 * 2.默认的inputformat的key一定要longwriteable类型否则程序虽然不会报错，但是无法读入文件内容
 * @author ben01.li
 *
 */
public class MultipleOutputMapReduce extends Configured implements Tool {

	static class MultipleOutMapper extends Mapper<LongWritable, Text, Text, Text> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			context.write(new Text(parser.getStationId()), value);
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MultipleOutputMapReduce(), args);
	}

	static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			for (Text value : values) {
				parser.parse(value);
				String path =  String.format("%s/%s", parser.getYear(), parser.getStationId());
				multipleOutputs.write(NullWritable.get(), value, path);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.print("usage : ./MultipleOutput input output");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("multiple output");
		job.setJarByClass(MultipleOutputMapReduce.class);
		job.setMapperClass(MultipleOutMapper.class);
		job.setReducerClass(MultipleOutputsReducer.class);
		FileUtils.delAllFile(args[1]);
		String input = args[0];
		String output = args[1];
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
