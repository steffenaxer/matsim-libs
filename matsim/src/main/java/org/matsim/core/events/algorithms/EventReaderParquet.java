package org.matsim.core.events.algorithms;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.matsim.api.core.v01.events.Event;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.api.internal.MatsimReader;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.MatsimEventsReader.CustomEventMapper;

public class EventReaderParquet implements MatsimReader {
    private static final  Logger LOG = LogManager.getLogger(EventReaderParquet.class);

    private ParquetReader<GenericRecord> reader;
	private final EventsManager events;
	private final Map<String, CustomEventMapper> customEventMappers = new HashMap<>();

    public EventReaderParquet(final EventsManager events) {
		this.events = events;
        
	}

    public static void main(String[] args) {
        EventsManager eventsManager = EventsUtils.createEventsManager();
        EventReaderParquet eventsReader = new EventReaderParquet(eventsManager);
        eventsManager.initProcessing();
		eventsReader.readFile("C:\\dev\\tmp\\events2.parquet");
		eventsManager.finishProcessing();
	}

    private ParquetReader<GenericRecord> getReader(Path filename) throws IOException {
		return AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(filename, new Configuration())).build();
	}

    @Override
    public void readURL(URL url) {
        try {
            this.reader = getReader(new Path(url.toURI()));
            this.read();
            this.reader.close();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readFile(String filename) {
        try {
            this.reader = getReader(new Path(filename));
            this.read();
            this.reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void read() throws IOException
    {
        Object obj = reader.read();
        while (obj != null) {
         if (obj instanceof GenericRecord genericRecord) {
          this.processEvent(genericRecord);
         }
         obj = reader.read();
        }
    }

     static class AttributeAdapter {
        GenericRecord genericRecord;

        AttributeAdapter(GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        String getValue(String key) {
            return null;
        }

    }

    Event processEvent(GenericRecord genericRecord)
    {
        Object attributes = genericRecord.get("attribues");
        return null;
    }



    public void addCustomEventMapper(String eventType, CustomEventMapper cem) {
		this.customEventMappers.put(eventType, cem);
	}
    
}
