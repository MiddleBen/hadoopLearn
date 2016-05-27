package systemapi;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemReader {

	/**
	 * @param args
	 */
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage : ./fileSystemReader <filePath>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		FSDataInputStream in = null;
		String uri = args[0];
		try {
			FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
			in = fileSystem.open(new Path(uri));
			System.out.println("---------------------1----------");
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println("---------------------2----------");
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);
			System.out.println("---------------------3----------");
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
