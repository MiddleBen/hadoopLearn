package ch04;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

public class firstAvro {
	
	public static void main(String[] args) {
		Schema.Parser parser = new Schema.Parser();
		try {
			Schema schema = parser.parse(firstAvro.class.getResourceAsStream("StringPair.avsc"));
			GenericRecord datum =new GenericData.Record(schema);
			datum.put("left", new Utf8("L"));
			datum.put("right", new Utf8("R"));
			
			ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			Encoder encoder = EncoderFactory.get().binaryEncoder(arrayOut, null);
			datumWriter.write(datum, encoder);
			encoder.flush();
			arrayOut.close();
			
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			Decoder in = DecoderFactory.get().binaryDecoder(arrayOut.toByteArray(), null);
			GenericRecord result = datumReader.read(null, in );
			System.out.println(result.get("left"));
			System.out.println(result.get("right"));
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

}
