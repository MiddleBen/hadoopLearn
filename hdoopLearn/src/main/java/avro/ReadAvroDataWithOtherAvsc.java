package avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * 1.读写schema是不通的，读的schema只有right字段
 * 2.注意，根据right字段降序，可以换来试试
 */
public class ReadAvroDataWithOtherAvsc {
	
	public static void main(String[] args) {
		Schema.Parser parser = new Schema.Parser();
		try {
			Schema schema = parser.parse(firstAvro.class.getResourceAsStream("ReadSomeofStringPair.avsc"));
			File file = new File("data.avro");
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(null, schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
			while(dataFileReader.hasNext()) {
				GenericRecord record = dataFileReader.next();
				String right = record.get("right").toString();
				System.out.println(right);
				
			}
			dataFileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
