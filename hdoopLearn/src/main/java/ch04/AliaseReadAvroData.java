package ch04;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AliaseReadAvroData {
	
	public static void main(String[] args) {
		Schema.Parser parser = new Schema.Parser();
		try {
			Schema schema = parser.parse(firstAvro.class.getResourceAsStream("StringPairAliase.avsc"));
			File file = new File("data.avro");
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(null, schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
			while(dataFileReader.hasNext()) {
				GenericRecord record = dataFileReader.next();
				String right = record.get("second").toString();
				System.out.println(right);
			}
			dataFileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
