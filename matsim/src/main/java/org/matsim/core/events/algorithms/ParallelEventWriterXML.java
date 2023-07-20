/* *********************************************************************** *
 * project: org.matsim.*
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2023 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */

package org.matsim.core.events.algorithms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.events.Event;
import org.matsim.core.events.handler.BasicEventHandler;
import org.matsim.core.utils.collections.Tuple;
import org.matsim.core.utils.io.IOUtils;
import org.matsim.core.utils.io.UncheckedIOException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class ParallelEventWriterXML implements EventWriter, BasicEventHandler {
	private static final Logger LOG = LogManager.getLogger(ParallelEventWriterXML.class);

	private final BufferedWriter out;

	private final BlockingQueue<Tuple<Event, CompletableFuture<String>>> workerQueue = new LinkedBlockingQueue<>(50_000);
	private final BlockingQueue<CompletableFuture<String>> writerQueue = new LinkedBlockingQueue<>(50_000);

	static final CompletableFuture<String> END_MARKER = new CompletableFuture<>();
	private final EventWriter writer;
	private Thread[] workerThreads = null;
	private Thread writerThread = null;

	public ParallelEventWriterXML(final String outfilename) {
		this.out = IOUtils.getBufferedWriter(outfilename);
		this.writer = new EventWriter(this.writerQueue, this.out);
	}

	/**
	 * Constructor so you can pass System.out or System.err to the writer to see the result on the console.
	 */
	public ParallelEventWriterXML(final OutputStream stream ) {
		this.out = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
		this.writer = new EventWriter(this.writerQueue, this.out);
	}

	@Override
	public void handleEvent(Event event) {
		initThreadsIfNecessary();
		CompletableFuture<String> future = new CompletableFuture<>();
		try {
			this.workerQueue.put(new Tuple<>(event, future));
			this.writerQueue.put(future);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public void closeFile() {
		try {
			if (this.workerThreads != null) {
				for (Thread ignored : this.workerThreads) {
					this.workerQueue.put(new Tuple<>(null, END_MARKER));
				}
				this.writerQueue.put(END_MARKER);

				for (Thread t : this.workerThreads) {
					t.join();
				}
				this.writerThread.join();
			}
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private void initThreadsIfNecessary() {
		if (this.workerThreads == null) {
			int threadCount = 3;
			this.workerThreads = new Thread[threadCount];
			for (int i = 0; i < threadCount; i++) {
				Thread t = new Thread(new Worker(this.workerQueue), "events-writer-worker-" + i);
				t.setDaemon(true);
				t.start();
				this.workerThreads[i] = t;
			}
		}
		if (this.writerThread == null) {
			this.writerThread = new Thread(this.writer, "events-writer");
			this.writerThread.setDaemon(true);
			this.writerThread.start();
		}
	}

	public static class Worker implements Runnable {

		private final BlockingQueue<Tuple<Event, CompletableFuture<String>>> queue;
		private final StringBuilder xml = new StringBuilder(1000);

		public Worker(BlockingQueue<Tuple<Event, CompletableFuture<String>>> queue) {
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				while (true) {
					Tuple<Event, CompletableFuture<String>> tuple = this.queue.take();
					Event event = tuple.getFirst();
					CompletableFuture<String> future = tuple.getSecond();
					if (future == ParallelEventWriterXML.END_MARKER) {
						break;
					}
					handleEvent(event);
					future.complete(this.xml.toString());
				}
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}
		}

		private void handleEvent(final Event event) {
			this.xml.setLength(0);
			this.xml.append("\t<event ");
			Map<String, String> attr = event.getAttributes();
			for (Map.Entry<String, String> entry : attr.entrySet()) {
				this.xml.append(entry.getKey());
				this.xml.append("=\"");
				this.xml.append(encodeAttributeValue(entry.getValue()));
				this.xml.append("\" ");
			}
			this.xml.append(" />\n");
		}


		// the following method was taken from MatsimXmlWriter in order to correctly encode attributes, but
		// to forego the overhead of using the full MatsimXmlWriter.
		/**
		 * Encodes the given string in such a way that it no longer contains
		 * characters that have a special meaning in xml.
		 *
		 * @see <a href="http://www.w3.org/International/questions/qa-escapes#use">http://www.w3.org/International/questions/qa-escapes#use</a>
		 * @param attributeValue the value of the attribute
		 * @return String with some characters replaced by their xml-encoding.
		 */
		private String encodeAttributeValue(final String attributeValue) {
			if (attributeValue == null) {
				return null;
			}
			int len = attributeValue.length();
			boolean encode = false;
			for (int pos = 0; pos < len; pos++) {
				char ch = attributeValue.charAt(pos);
				if (ch == '<') {
					encode = true;
					break;
				} else if (ch == '>') {
					encode = true;
					break;
				} else if (ch == '\"') {
					encode = true;
					break;
				} else if (ch == '&') {
					encode = true;
					break;
				}
			}
			if (encode) {
				StringBuilder bf = new StringBuilder(attributeValue.length() + 30);
				for (int pos = 0; pos < len; pos++) {
					char ch = attributeValue.charAt(pos);
					if (ch == '<') {
						bf.append("&lt;");
					} else if (ch == '>') {
						bf.append("&gt;");
					} else if (ch == '\"') {
						bf.append("&quot;");
					} else if (ch == '&') {
						bf.append("&amp;");
					} else {
						bf.append(ch);
					}
				}
				return bf.toString();
			}
			return attributeValue;

		}

	}

	private static class EventWriter implements Runnable {
		final BlockingQueue<CompletableFuture<String>> queue;
		final BufferedWriter out;

		public EventWriter(BlockingQueue<CompletableFuture<String>> queue, BufferedWriter out) {
			this.queue = queue;
			this.out = out;
		}

		@Override
		public void run() {
			this.writeHeader();
			try {
				while (true) {
					CompletableFuture<String> xmlPart = this.queue.take();
					if (xmlPart == ParallelEventWriterXML.END_MARKER) {
						break;
					}
					String xml = xmlPart.get();
					this.out.write(xml);
				}
			} catch (InterruptedException | ExecutionException | IOException e) {
				LOG.error(e.getMessage(), e);
			}
			this.closeFile();
		}

		private void writeHeader() {
			try {
				this.out.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<events version=\"1.0\">\n");
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		private void closeFile() {
			try {
				this.out.write("</events>");
				this.out.close();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

}
