package org.matsim.contrib.drt.extension.benchmark;

import java.nio.file.Path;

public record SimulationResult(double durationSeconds, Path outputDir) {
}
