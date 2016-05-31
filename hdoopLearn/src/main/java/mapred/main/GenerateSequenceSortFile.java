package mapred.main;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.FileUtils;
import utils.NcdcRecordParser;

/**
 * 产生一个排序的sequence文件
 * 1. 块压缩与不压缩相差十倍磁盘，块压缩与非块压缩相差四五倍
 * @author Administrator
 * 
 */
public class GenerateSequenceSortFile extends Configured implements Tool {
	
	static Set<String> mapper = new HashSet<String>();
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GenerateSequenceSortFile(), args);
		System.out.println(mapper);
	}

	static class SequenceMappser extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		static String id = new Random().nextInt(100000) + "";
		
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			mapper.add(id);
			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new IntWritable(parser.getAirTemperature()),
						value);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.print("usage : ./GenerateSequenceSortFile input output");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		FileUtils.delAllFile(args[1]);
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("generate sequence sort file");
		job.setJarByClass(getClass());
		job.setMapperClass(SequenceMappser.class);
		String input = args[0];
		String output = args[1];
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);//至少一个reduce，书上不一样。
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);这句不能要，不能和书上一样，否则不能正常。
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
