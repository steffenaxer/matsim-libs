
package org.matsim.contrib.drt.extension.benchmark.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;

public class CSVUtils {

    /**
     * Extracts the value of a given field from the last row of a CSV file.
     *
     * @param filePath the path to the CSV file
     * @param field the name of the field (column header)
     * @return an Optional containing the value if found, or Optional.empty() otherwise
     */
    public static Optional<String> getValueFromCSV(String filePath, String field) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return Optional.empty(); // No header
            }

            String[] headers = headerLine.split(";");
            int fieldIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].trim().equals(field)) {
                    fieldIndex = i;
                    break;
                }
            }

            if (fieldIndex == -1) {
                return Optional.empty(); // Field not found
            }

            String line;
            String lastLine = null;
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }

            if (lastLine != null) {
                String[] values = lastLine.split(";");
                if (fieldIndex < values.length) {
                    return Optional.of(values[fieldIndex]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Optional.empty(); // In case of error or missing value
    }
}
