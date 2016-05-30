package mapred;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecorder extends RecordReader<NullWritable, BytesWritable>{

	private FileSplit fileSplit;
	private boolean processed;
	TaskAttemptContext context;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.context = context;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return !processed;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		if (!processed) {
			long fileLen = fileSplit.getLength();
			Path filePath = fileSplit.getPath();
			FileSystem fileSystem = filePath.getFileSystem(context.getConfiguration());
			FSDataInputStream fsDataInputStream = fileSystem.open(filePath);
			byte[] buf = new byte[(int) fileLen];
			IOUtils.readFully(fsDataInputStream.getWrappedStream(), buf , 0, (int) fileLen);
			fsDataInputStream.close();
			processed = true;
			return new BytesWritable(buf);
		}
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 0f : 1f;
	}

	@Override
	public void close() throws IOException {

	}}
