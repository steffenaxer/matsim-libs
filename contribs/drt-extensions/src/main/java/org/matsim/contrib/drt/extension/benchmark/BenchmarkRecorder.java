package org.matsim.contrib.drt.extension.benchmark;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class BenchmarkRecorder {
    private final List<String[]> results = new ArrayList<>();
    private final NumberFormat usFormat = NumberFormat.getNumberInstance(Locale.US);
    private final String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));

    public void record(String[] row) {
        results.add(row);
    }

    public void writeToFile() {
        Path file = Path.of("benchmark_" + timestamp + ".csv");
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file))) {
            writer.println("agents;requestPartitioner;vehiclePartitioner;Threads;CollectionPeriod;MaxIter;InsertionSearchThreadsPerWorker;DurationSeconds;rejectionRate;emptyRatio;uuid;insertionSearch");
            for (String[] row : results) {
                writer.println(String.join(";", row));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
