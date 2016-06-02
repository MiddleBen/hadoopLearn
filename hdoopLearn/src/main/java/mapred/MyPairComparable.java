package mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.mahout.common.IntPairWritable;

public class MyPairComparable implements WritableComparable<MyPairComparable>,
		Cloneable {

	private int first;

	private int second;

	public MyPairComparable() {
//
	}

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		return super.equals(arg0);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	public MyPairComparable(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public int compareTo(MyPairComparable arg0) {
		return this.compareTo(arg0);
	}

	static {
		WritableComparator.define(IntPairWritable.class,
				new SecondaryComparator());
	}

	public static final class SecondaryComparator extends WritableComparator {

		// 这个构造函数非常重要，少了compare的buffer就是空的啦。
		public SecondaryComparator() {
			super(MyPairComparable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MyPairComparable a1 = (MyPairComparable) a;
			MyPairComparable a2 = (MyPairComparable) b;
			if (a1.getFirst() != a2.getFirst()) {
				return a1.getFirst() - a2.getFirst();
			} else {
				return -(a1.getSecond() - a2.getSecond());
			}
		}
	}

}
