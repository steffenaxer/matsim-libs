package org.matsim.core.events.algorithms;

import org.dizitart.no2.Nitrite;
import org.dizitart.no2.collection.Document;
import org.dizitart.no2.collection.NitriteCollection;
import org.dizitart.no2.rocksdb.RocksDBModule;
import org.dizitart.no2.transaction.Session;
import org.dizitart.no2.transaction.Transaction;
import org.matsim.api.core.v01.events.Event;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.handler.BasicEventHandler;
import org.rocksdb.Options;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author steffenaxer
 */
public class EventsWriterNitrite implements EventWriter, BasicEventHandler {
	private final Nitrite db;
	private final Queue<Document> queue = new LinkedBlockingQueue<>();
	EventsWriterNitrite(String filePath)
	{

		RocksDBModule storeModule = RocksDBModule.withConfig()
			.filePath(filePath)
			.build();

		 this.db = Nitrite.builder()
			.loadModule(storeModule)
			.openOrCreate();

		db.getCollection("events");
	}

	public static void main(String[] args)
	{
		EventsManager manager = EventsUtils.createEventsManager();
		manager.addHandler(new EventsWriterNitrite("rocks.db"));
		manager.initProcessing();
		EventsUtils.readEvents(manager,"/Users/steffenaxer/Downloads/004.output_events.xml.gz");
		manager.finishProcessing();
	}

	@Override
	public void closeFile() {
		transmit(this.queue);
		this.db.close();
	}

	@Override
	public void handleEvent(Event event) {
		queue.add(createEventDocument(event));

		if(queue.size()>5_000)
		{
			transmit(this.queue);
			this.queue.clear();
		}
	}

	private Document createEventDocument(Event event)
	{
		Document doc = Document.createDocument();
		doc.put("time", event.getTime());
		doc.put("type", event.getEventType());

		event.getAttributes().forEach(doc::put);
		return doc;
	}

	void transmit(Queue<Document> documents)
	{
		try (Session session = db.createSession()) {
			try (Transaction transaction = session.beginTransaction()) {
				NitriteCollection collection = transaction.getCollection("events");
				documents.stream().forEach(collection::insert);
				transaction.commit();
			}
		}
	}

}
