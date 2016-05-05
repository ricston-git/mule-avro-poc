package com.ricston.poc.avro.transformers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

import com.ricston.poc.avro.AvroPoc;

public class AvroSerializer extends AbstractMessageTransformer {

	public Object transformMessage(MuleMessage message, String outputEncoding)
			throws TransformerException {

		List<AvroPoc> payload = (List<AvroPoc>) message.getPayload();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		Schema schema = ReflectData.get().getSchema(AvroPoc.class);
		ReflectDatumWriter<Object> reflectDatumWriter = new ReflectDatumWriter<Object>(
				schema);
		try {
			DataFileWriter<Object> writer = new DataFileWriter<Object>(
					reflectDatumWriter).create(schema, outputStream);
			for (AvroPoc currentRecord : payload) {
				writer.append(currentRecord);
			}
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return outputStream.toByteArray();
	}
}
