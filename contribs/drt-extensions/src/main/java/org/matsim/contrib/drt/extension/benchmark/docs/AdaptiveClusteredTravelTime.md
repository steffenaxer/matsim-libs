# Adaptive Clustered TravelTime

## Concept

A wrapper around any existing `TravelTime` implementation that clusters link travel time patterns for better cache efficiency. The clusters are rebuilt periodically to adapt to changing traffic conditions.

## Key Features

- **Wraps any TravelTime**: Works with `TravelTimeCalculator`, `FreeSpeedTravelTime`, or any custom implementation
- **Adaptive**: Periodically rebuilds clusters based on current data from the delegate
- **~100x less memory**: Through pattern sharing across similar links
- **Better cache locality**: Patterns fit in L2 cache

## How It Works

```
┌────────────────────────────────────────────────────────────────────────┐
│  Initial State / After rebuildClusters()                               │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. Extract profiles from delegate TravelTime for all links           │
│     → normalize each link's profile to its FreeSpeed                  │
│                                                                        │
│  2. K-Means clustering to find similar patterns                       │
│     → 256 pattern clusters (configurable)                             │
│                                                                        │
│  3. Store per link:                                                   │
│     → patternId (2 bytes) + baseTravelTime (4 bytes)                 │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│  Lookup: getLinkTravelTime(link, time)                                 │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  patternId = linkPatternIds[link.index]     // 1 array access         │
│  baseTT = linkBaseTravelTimes[link.index]   // 1 array access         │
│  bin = time / binSize                       // 1 division             │
│  factor = patterns[patternId][bin]          // 1 array access         │
│  return baseTT * factor                     // 1 multiplication       │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Usage

### Basic Usage

```java
// Wrap existing TravelTime
TravelTime baseTT = travelTimeCalculator.getLinkTravelTimes();
AdaptiveClusteredTravelTime clustered = new AdaptiveClusteredTravelTime(network, baseTT);

// Use in routing
LeastCostPathTree dijkstra = new LeastCostPathTree(graph, clustered, travelDisutility);
```

### With Custom Configuration

```java
AdaptiveClusteredTravelTime.Config config = new AdaptiveClusteredTravelTime.Config()
    .setNumPatterns(512)           // More patterns = more accuracy, less compression
    .setNumBins(96)                // 24h with 15min bins
    .setBinSizeSeconds(900)        // 15 minutes
    .setMaxClusteringIterations(100);

AdaptiveClusteredTravelTime clustered = new AdaptiveClusteredTravelTime(network, baseTT, config);
```

### Periodic Updates

```java
// In a ControlerListener or periodic task:
@Override
public void notifyIterationEnds(IterationEndsEvent event) {
    // Rebuild clusters to adapt to new traffic patterns
    clustered.rebuildClusters();
}
```

### Integration with MATSim Module

```java
// Simple: Use the provided module
controler.addOverridingModule(new ClusteredTravelTimeModule(256));

// Or with periodic rebuild during simulation (every 3600 seconds)
controler.addOverridingModule(new ClusteredTravelTimeModule(256, 3600));
```

### Using in Benchmark

The benchmark supports clustered TravelTime via configuration:

```xml
<module name="drtBenchmark">
    <param name="useClusteredTravelTime" value="true"/>
    <param name="numTravelTimePatterns" value="256"/>
</module>
```

Or via command line:
```bash
--config:drtBenchmark.useClusteredTravelTime true
--config:drtBenchmark.numTravelTimePatterns 512
```

## Performance

| Aspect | Standard TravelTime | AdaptiveClusteredTravelTime |
|--------|--------------------|-----------------------------|
| Memory (100k links) | 76.8 MB | ~0.7 MB |
| Cache locality | Poor | Very good |
| Lookup overhead | Map + checks | 3 arrays + multiply |
| **Expected speedup** | - | **2-4x** |

## When to Rebuild Clusters

- **Every iteration**: Most accurate, but adds clustering overhead
- **Every N iterations**: Good balance between accuracy and overhead
- **When traffic patterns change significantly**: Event-driven approach

## Thread Safety

- `getLinkTravelTime()` is thread-safe (reads volatile reference)
- `rebuildClusters()` is thread-safe (uses atomic flag to prevent concurrent rebuilds)
- Cluster rebuild creates new data structure, then atomically swaps reference

## Author

Steffen Axer

## Date

2025-02-12


