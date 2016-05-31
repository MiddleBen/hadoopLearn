package mapred;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.mahout.common.IntPairWritable;

public class SecondaryGroupCompator extends WritableComparator {

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPairWritable a1 = (IntPairWritable)a;
		IntPairWritable a2 = (IntPairWritable)b;
		return a1.getFirst() - a2.getFirst();
	}

}
