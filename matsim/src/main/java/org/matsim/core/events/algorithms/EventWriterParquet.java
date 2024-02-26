package org.matsim.core.events.algorithms;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.matsim.api.core.v01.events.Event;
import org.matsim.core.events.handler.BasicEventHandler;

import java.io.IOException;


/**
 * @author steffenaxer
 */
public class EventWriterParquet implements EventWriter, BasicEventHandler {
	ParquetWriter<GenericData.Record> writer;

	EventWriterParquet(String filePath) throws IOException {
		this.writer = getWriter(new Path(filePath));
	}

	@Override
	public void closeFile() {
		try {
			this.writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void handleEvent(Event event) {
		try {
			this.writer.write(getRecord(event));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) {

	}

	ParquetWriter<GenericData.Record> getWriter(Path filePath) throws IOException {
		return AvroParquetWriter.<GenericData.Record>builder(filePath)
			.withCompressionCodec(CompressionCodecName.ZSTD)
			.withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
			.withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
			.withConf(new Configuration())
			.withValidation(false)
			.withDictionaryEncoding(false)
			.build();
	}

	private static GenericData.Record getRecord(Event event) {
		GenericData.Record record = new GenericData.Record(Schema.create(Schema.Type.RECORD));
		record.put("time",event.getTime());
		record.put("type",event.getEventType());
		event.getAttributes().forEach(record::put);
		return record;
	}
}
