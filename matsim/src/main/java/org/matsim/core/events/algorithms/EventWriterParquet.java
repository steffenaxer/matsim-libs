package org.matsim.core.events.algorithms;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;


import org.apache.commons.lang3.time.StopWatch;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.matsim.api.core.v01.events.Event;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.handler.BasicEventHandler;

import com.jerolba.carpet.CarpetParquetWriter;
import com.jerolba.carpet.io.OutputStreamOutputFile;

/**
 * @author steffenaxer
 */
public class EventWriterParquet implements EventWriter, BasicEventHandler {
	private final ParquetWriter<EventRecord> writer;
	private final OutputStreamOutputFile output ;

	EventWriterParquet(String filePath) throws IOException {
		this.output = new OutputStreamOutputFile(new FileOutputStream(filePath));
		this.writer = getWriter(output);
	}


	public record EventRecord(double time, String tpye, Map<String, String> attributes) {}

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
			this.writer.write(new EventRecord(event.getTime(),event.getEventType(),event.getAttributes()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws IOException {
		matsimParquetWriter("C:\\dev\\msf\\output\\austinWithSpeedProfile\\ITERS\\it.0\\0.events.xml.zst","C:\\dev\\tmp\\events3.parquet");
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

	ParquetWriter<EventRecord> getWriter(OutputStreamOutputFile outputStream) throws IOException {
		return  CarpetParquetWriter.builder(outputStream, EventRecord.class)
        .withWriteMode(Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.ZSTD)
        .withPageRowCountLimit(100_000)
        .build();
	}

}
