package ch03;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class UrlReader {

	static {
		
	}
	
	public static void main(String[] args) {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		if (args.length != 1) {
			System.out.println("usage : ./urlReader hdsfFilePath");
			return;
		}
		String hdfsFilePath = args[0];
		InputStream input = null;
		try {
			URL url = new URL(hdfsFilePath);
			input = url.openStream();
			IOUtils.copyBytes(input, System.out, 1024, true);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(input);
		}
		
	}
}
