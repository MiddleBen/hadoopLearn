package ch04;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

import ch04.entity.StringPair;

public class WriteAvroData {
	
	public static void main(String[] args) {
		Schema.Parser parser = new Schema.Parser();
		try {
			Schema schema = parser.parse(firstAvro.class.getResourceAsStream("StringPair.avsc"));
			File file = new File("data.avro");
			DatumWriter<StringPair> datumWriter = new GenericDatumWriter<>(schema);
			StringPair datum = new StringPair();
			datum.setLeft("anny left");
			datum.setRight("ben right");
			DataFileWriter<StringPair> dataFileWriter = new DataFileWriter<>(datumWriter);
			dataFileWriter.create(schema, file);
			dataFileWriter.append(datum);
			datum = new StringPair();
			datum.setLeft("ben left");
			datum.setRight("anny right");
			dataFileWriter.append(datum);
			dataFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
