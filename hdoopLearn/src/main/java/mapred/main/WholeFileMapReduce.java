package mapred.main;
import java.io.IOException;

import mapred.WholeFileInputFormat;
import mapred.WholeFilePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.FileUtils;

/**
 * 作用：每次读取一个文件的内容，放到n个文件里面，n为reducer数量，根据WholeFilePartitioner给mapper输出分区
 * 命令：java -jar -D mapred.reduce.tasks=2 C:\\Users\\ben01.li\\Desktop\\HADOOP3(jb51.net)\\hadoop-book-3e-code\\input\\smallfiles C:\\Users\\ben01.li\\Desktop\\HADOOP3(jb51.net)\\data\\output
 * 1. mapper调用fileInputFormat,fileInputFormat调用recorder
 * 2. 根据WholeFilePartitioner进行map输出的分区
 * @author ben01.li
 *
 */
public class WholeFileMapReduce extends Configured implements Tool {

	static class WholeFileMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

		private String fileName;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			fileName = fileSplit.getPath().toString();
		}

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context) throws IOException,
				InterruptedException {
			String srt = new String(value.getBytes());
			context.write(new Text(fileName), new Text(srt));
		}

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WholeFileMapReduce(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.print("usage : ./wholeMapre input output");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(WholeFileMapReduce.class);
		job.setMapperClass(WholeFileMapper.class);
		FileUtils.delAllFile(args[1]);
		String input = args[0];
		String output = args[1];
		WholeFileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setPartitionerClass(WholeFilePartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
