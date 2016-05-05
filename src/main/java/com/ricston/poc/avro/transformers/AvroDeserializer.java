package com.ricston.poc.avro.transformers;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

import com.ricston.poc.avro.AvroPoc;

public class AvroDeserializer extends AbstractMessageTransformer {


	@Override
	public Object transformMessage(MuleMessage message, String outputEncoding)
			throws TransformerException {
		FileInputStream stream = (FileInputStream) message.getPayload();

		DatumReader<AvroPoc> dwDatumReader = new SpecificDatumReader<AvroPoc>(
				AvroPoc.class);

		List<AvroPoc> allUsers = new ArrayList<AvroPoc>();
		try {
			final DataFileStream<AvroPoc> dataFileReader = new DataFileStream<AvroPoc>(
					stream, dwDatumReader);

			while (dataFileReader.hasNext()) {
				AvroPoc record = new AvroPoc();
				record = dataFileReader.next(record);
				allUsers.add(record);
			}
			stream.close();

			dataFileReader.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize records",e);
		}

		message.setPayload(allUsers);

		return message;
	}
}
