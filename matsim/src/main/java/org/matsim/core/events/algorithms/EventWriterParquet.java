package org.matsim.core.events.algorithms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.matsim.api.core.v01.events.Event;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.handler.BasicEventHandler;

/**
 * @author steffenaxer
 */
public class EventWriterParquet implements EventWriter, BasicEventHandler {
	private static final Schema RECORD_SCHEMA = getMapSchema();
	private final ParquetWriter<GenericData.Record> writer;

	EventWriterParquet(String filePath) throws IOException {
		Path p = new Path(filePath);
		this.writer = getWriter(p);
	}

	public static Schema getMapSchema() {

		final Schema valueType = SchemaBuilder.builder().stringType();

		return SchemaBuilder.builder("org.matsim").record("event")
				.fields()
				.name("time").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
				.name("type").type(Schema.create(Schema.Type.STRING)).noDefault()
				.name("attributes").type(Schema.createMap(valueType)).noDefault()
				.endRecord();
	}

	public static Schema getEventSchema() {
		return ReflectData.get().getSchema(Event.class);
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

	public static void main(String[] args) throws IOException {
		matsimParquetWriter("C:\\dev\\msf\\output\\austinWithSpeedProfile\\ITERS\\it.0\\0.events.xml.zst","C:\\dev\\tmp\\events2.parquet");
	}

	private static long matsimParquetWriter(String input, String outout) throws IOException {
		StopWatch watch = new StopWatch();
		watch.start();
		EventWriter w = new EventWriterParquet(outout);
		EventsManager manager = EventsUtils.createEventsManager();
		manager.addHandler(w);
		manager.initProcessing();
		EventsUtils.readEvents(manager, input);
		manager.finishProcessing();
		w.closeFile();
		watch.stop();
		return watch.getTime();
	}

	ParquetWriter<GenericData.Record> getWriter(Path filePath) throws IOException {
		return AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(filePath,new Configuration()))
				.withCompressionCodec(CompressionCodecName.ZSTD)
				.withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
				.withWriteMode(Mode.OVERWRITE)
				.withSchema(RECORD_SCHEMA)
				.withValidation(false)
				.withDictionaryEncoding(true)
				.build();
	}

	private static GenericData.Record getRecord(Event event) {
		GenericData.Record r = new GenericData.Record(RECORD_SCHEMA);
		r.put("time", event.getTime());
		r.put("type", event.getEventType());
		r.put("attributes", event.getAttributes());
		return r;
	}
}
