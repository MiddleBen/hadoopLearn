package systemapi;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileSystemWriter {

	/**
	 * @param args
	 */
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("usage : ./fileSystemWriter <localFile> <destFile>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		FSDataOutputStream out = null;
		String localUri = args[0];
		String destUri = args[1];
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(new File(localUri)));
			FileSystem fileSystem = FileSystem.get(URI.create(destUri), conf);
			out = fileSystem.create(new Path(destUri), new Progressable() {
				
				public void progress() {
					System.out.println("ben li creating ...");
					
				}
			});
			IOUtils.copyBytes(in, out, 4096, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
