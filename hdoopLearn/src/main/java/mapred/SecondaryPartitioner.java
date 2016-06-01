package mapred;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondaryPartitioner extends Partitioner<MyPairComparable, NullWritable> {

	@Override
	public int getPartition(MyPairComparable key, NullWritable value,
			int numPartitions) {
		return key.getFirst() % numPartitions;
	}
	

}
