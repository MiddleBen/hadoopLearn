package mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WholeFilePartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		if ("file:/C:/Users/ben01.li/Desktop/HADOOP3(jb51.net)/hadoop-book-3e-code/input/smallfiles/b".equals(key.toString())) {
			return 0;
		}
		return 1;
	}

}
