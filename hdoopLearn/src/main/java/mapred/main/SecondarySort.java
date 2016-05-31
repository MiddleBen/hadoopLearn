package mapred.main;

import java.io.IOException;

import mapred.IntPairComparable;
import mapred.SecondaryPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.FileUtils;
import utils.NcdcRecordParser;

/**
 * 辅助排序，按年份key分区，然后按气温value排序，每个分区就有某年全部数据，而这个数据时按温度降序的，
 * 最后在reduce的groupByCompar阶段读取最高气温 这个例子是个非常综合的例子，较有意义
 * 
 * @author Administrator
 * 
 */
public class SecondarySort extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SecondarySort(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.print("usage : ./SecondarySort input output");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("SecondarySort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondaryMapper.class);
		job.setReducerClass(SecodaryRecuder.class);
		job.setOutputKeyClass(IntPairComparable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setPartitionerClass(SecondaryPartitioner.class);
//		job.setSortComparatorClass(SecondaryComparator.class);
		job.setNumReduceTasks(2);
//		job.setGroupingComparatorClass(SecondaryGroupCompator.class);
		FileUtils.delAllFile(args[1]);
		String input = args[0];
		String output = args[1];
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class SecondaryMapper extends
			Mapper<LongWritable, Text, IntPairComparable, NullWritable> {

		private NcdcRecordParser parse = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parse.parse(value);
			if (parse.isValidTemperature()) {
				context.write(
						new IntPairComparable(parse.getYearInt(), parse
								.getAirTemperature()), NullWritable.get());
			}
		}
	}

	static class SecodaryRecuder
			extends
			Reducer<IntPairComparable, NullWritable, IntPairComparable, NullWritable> {

		@Override
		protected void reduce(IntPairComparable arg0,
				Iterable<NullWritable> arg1, Context arg2) throws IOException,
				InterruptedException {
			arg2.write(arg0, NullWritable.get());
		}

	}
	
	static class SecondaryComparator extends WritableComparator {

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPairComparable a1 = (IntPairComparable)a;
			IntPairComparable a2 = (IntPairComparable)b;
			if (a1.getFirst() != a2.getFirst()) {
				return a1.getFirst() - a2.getFirst();
			} else {
				return -(a1.getSecond() - a2.getSecond());
			}
		}
	}

}
