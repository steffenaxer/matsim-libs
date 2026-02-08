# Konzept: Asynchrone Request-Insertion für DRT

## Problem

Der aktuelle `ParallelUnplannedRequestInserter` sammelt Requests für eine konfigurierbare Zeit (`collectionPeriod`) und verarbeitet sie dann batch-weise. Das Problem:

1. **Blockierende Berechnung**: Während der Berechnung steht die Mobsim
2. **Verschwendete Mobsim-Zeit**: Die Zeit zwischen den Batches könnte für Berechnungen genutzt werden
3. **Race Condition bei echter Asynchronität**: Wenn wir asynchron rechnen, bewegen sich Fahrzeuge weiter

## Analyse: Was ändert sich während der Berechnung?

### Volatile (ändern sich ständig):
- **Position des Fahrzeugs** auf dem aktuellen DriveTask
- **DiversionPoint** des aktuellen DriveTask (bewegt sich mit dem Fahrzeug)
- **Aktuelle Zeit** in der Simulation

### Stabil (ändern sich nur durch Scheduling):
- **Zukünftige Tasks** im Schedule (Stops, Drives)
- **Gebuchte Passagiere** an den Stops
- **Kapazitäten** nach jedem Stop

### Abgeleitet (hängen von Volatile ab):
- **SlackTimes** - reduzieren sich mit der Zeit
- **Arrival Times** - ändern sich wenn Start sich ändert

## Lösungsansatz: DiversionPoint-basierte Berechnung

### Kernidee

Der `OnlineDriveTaskTracker.getDiversionPoint()` liefert den **frühesten Punkt**, an dem ein 
fahrendes Fahrzeug noch umgeleitet werden kann. Dies ist der perfekte Anker für asynchrone 
Berechnungen:

```
     Fahrzeug-Position zum Zeitpunkt t0        DiversionPoint (t0)
              │                                       │
              ▼                                       ▼
    ══════════●━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━●━━━━━━━━━━━━━▶  Route
              │                                       │
              └──── "verbrannt" ─────────────────────┘
                    (Fahrzeug ist schon vorbei)
                    
     Während der Berechnung bewegt sich das Fahrzeug weiter:
     
     Fahrzeug-Position zum Zeitpunkt t1        DiversionPoint (t1)
                        │                             │
                        ▼                             ▼
    ══════════━━━━━━━━━●━━━━━━━━━━━━━━━━━━━━━━━━━━━━━●━━━━━━━━━━━━▶  Route
```

**Warum ist das elegant?**
1. Der DiversionPoint ist immer der **frühestmögliche Umkehrpunkt**
2. Alles was vor dem DiversionPoint liegt, ist irrelevant - das Fahrzeug kann dort nicht mehr hin
3. Die Berechnung muss nur sicherstellen, dass die Insertion ab dem DiversionPoint funktioniert

### Grundidee der Async-Berechnung

```
       t0 (Berechnung startet)              t1 (Berechnung endet)
        │                                    │
        ▼                                    ▼
┌───────────────────────────────────────────────────────────────┐
│   Asynchrone Berechnung                                       │
│   - Snapshot der VehicleEntries inkl. DiversionPoints         │
│   - Berechnung basiert auf DiversionPoint als Start           │
│   - Schedule ab DiversionPoint ist stabil (ändert sich nicht) │
└───────────────────────────────────────────────────────────────┘
                                             │
                                             ▼
                                ┌─────────────────────────────┐
                                │  Synchrone Validierung      │
                                │  - Hat sich der Schedule    │
                                │    verändert? (neue Stops?) │
                                │  - Falls nein: Schedule     │
                                │  - Falls ja: Re-queue       │
                                └─────────────────────────────┘
```

### Schritt 1: DiversionPoint-basierter VehicleEntry

Der entscheidende Unterschied: Wir berechnen Insertionen nicht ab der aktuellen Position,
sondern **ab dem DiversionPoint**. Das ist der Punkt, ab dem wir das Fahrzeug noch
beeinflussen können.

```java
class DiversionPointBasedVehicleEntry extends VehicleEntry {
    private final LinkTimePair diversionPoint;
    private final double diversionSlackReduction;
    
    // Der Start ist nicht die aktuelle Position, sondern der DiversionPoint
    @Override
    public Waypoint.Start getStart() {
        return new Waypoint.Start(
            currentTask,
            diversionPoint.link,    // Start ab DiversionPoint
            diversionPoint.time,    // Zeit am DiversionPoint
            currentOccupancy
        );
    }
    
    @Override
    public double getSlackTime(int index) {
        // SlackTime reduziert sich um die Zeit bis zum DiversionPoint
        return Math.max(0, super.getSlackTime(index) - diversionSlackReduction);
    }
}
```

**Warum funktioniert das?**
- Der DiversionPoint bewegt sich mit dem Fahrzeug mit
- Aber: Der **Schedule ab dem ersten Stop** ändert sich nicht!
- Wenn wir ab dem DiversionPoint rechnen, ist die Berechnung stabil

### Schritt 2: Was genau ist "stabil"?

Der Schlüssel zum Verständnis:

```
Schedule eines fahrenden Fahrzeugs:
                                                            
  [DRIVE]──────────────────▶[STOP 1]──────▶[STOP 2]──────▶[STAY]
       │                        │              │
       │ DiversionPoint         │              │
       ▼                        │              │
  ═════●━━━━━━━━━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━▶
       │                        │              │
       └──── VOLATIL ──────────┴──── STABIL ──┴────────────────
```

**Was ist volatil (ändert sich)?**
- Position auf dem aktuellen DRIVE Task
- DiversionPoint (Link + Time)
- Zeit bis zum ersten Stop

**Was ist stabil (ändert sich nur durch Scheduling)?**
- Alle Stops (STOP 1, STOP 2, ...)
- Passagiere an den Stops
- Die relative Reihenfolge und Abstände der Stops

**Die Erkenntnis:**
Wenn wir eine Insertion bei Index > 0 machen (also nicht VOR dem ersten Stop),
ist die Berechnung **vollständig stabil**! Der DiversionPoint spielt dann
nur für die SlackTime-Berechnung eine Rolle.

### Schritt 3: Asynchrones Pipeline-Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ASYNC CALCULATION THREAD(S)                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐   │
│  │  Request    │────>│  Insertion Search   │────>│  Solution   │   │
│  │  Queue      │     │  (DiversionPoint-   │     │  Queue      │   │
│  │             │     │   basierte Entries) │     │  +Timestamp │   │
│  └─────────────┘     └─────────────────────┘     └─────────────┘   │
│        ▲                                               │            │
│        │                                               │            │
└────────┼───────────────────────────────────────────────┼────────────┘
         │                                               │
         │         ┌─────────────────────────────────────┘
         │         │
┌────────┴─────────┼──────────────────────────────────────────────────┐
│                  │     MOBSIM THREAD (synchron)                     │
├──────────────────┼──────────────────────────────────────────────────┤
│                  ▼                                                  │
│  ┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐   │
│  │  Poll       │────>│  Validate:          │────>│  Schedule   │   │
│  │  Solutions  │     │  - Schedule changed?│     │  Request    │   │
│  └─────────────┘     │  - Capacity ok?     │     └─────────────┘   │
│                      └─────────────────────┘                        │
│                              │                                      │
│                              │ (invalid)                            │
│                              ▼                                      │
│                        ┌───────────┐                                │
│                        │ Re-queue  │                                │
│                        │ (async)   │                                │
│                        └───────────┘                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Schritt 4: Validierungskriterien (vereinfacht durch DiversionPoint)

Eine Insertion ist **gültig** wenn:

1. **Schedule-Struktur unverändert**:
   - Keine neuen Stops wurden hinzugefügt (durch andere Requests)
   - Kein Stop wurde entfernt (Passagier storniert)
   
2. **Kapazität passt noch**:
   - Keine neuen Passagiere an relevanten Stops hinzugefügt

**Beachte:** SlackTime muss NICHT erneut geprüft werden!
Die DiversionPoint-basierte Berechnung hat bereits berücksichtigt,
dass sich das Fahrzeug weiterbewegt. Solange der Schedule gleich ist,
ist die SlackTime-Berechnung noch gültig.

### Schritt 5: Spezialfall - Insertion vor erstem Stop

Der einzige komplizierte Fall: Wenn die Insertion VOR dem ersten Stop erfolgt
(pickup.index == 0), dann ist die genaue Position des DiversionPoints wichtig.

**Lösung:** Für solche Insertionen eine kurze "Gültigkeitsdauer" definieren
oder synchron nachrechnen wenn die Berechnung > X Sekunden alt ist.

```java
enum ValidationResult {
    VALID,                    // Kann sofort gescheduled werden
    NEEDS_SLACK_RECHECK,      // Insertion vor erstem Stop - SlackTime prüfen
    INVALID_STRUCTURE,        // Schedule hat sich geändert → Neuberechnung
    INVALID_CAPACITY          // Kapazität geändert → Neuberechnung
}
```

## Implementierungs-Roadmap

### Phase 1: DiversionPoint-basierter VehicleEntry
- [ ] `DiversionPointVehicleEntry` extends `VehicleEntry`
- [ ] Integration mit `OnlineDriveTaskTracker.getDiversionPoint()`
- [ ] SlackTime-Anpassung basierend auf DiversionPoint-Zeit
- [ ] Tests für verschiedene Fahrzeugzustände (DRIVE, STOP, STAY)

### Phase 2: Leichtgewichtige Validierung
- [ ] `InsertionValidator` Klasse erstellen
- [ ] Schedule-Struktur-Vergleich (nur Stop-Anzahl, keine Deep-Copy)
- [ ] Kapazitäts-Validierung
- [ ] Spezialfall-Handling für Insertion vor erstem Stop

### Phase 3: Async Pipeline
- [ ] `ConcurrentLinkedQueue` für Requests (Thread-safe, lock-free)
- [ ] Solution Queue mit Berechnungs-Timestamp
- [ ] Worker-Threads für Insertion-Berechnung
- [ ] Mobsim-Thread pollt und validiert

### Phase 4: Optimierungen
- [ ] Batch-Validierung (mehrere Lösungen auf einmal prüfen)
- [ ] Prioritäts-Queue für dringende Requests
- [ ] Adaptive Worker-Thread-Anzahl
- [ ] Metriken und Monitoring

## Erwartete Vorteile

1. **Parallele Verarbeitung**: CPU-Cores werden besser genutzt
2. **Geringere Latenz**: Requests werden schneller verarbeitet
3. **Skalierbarkeit**: Berechnungslast kann ausgelagert werden
4. **Einfache Validierung**: Durch DiversionPoint-Basis ist die Validierung leichtgewichtig

## Potenzielle Probleme

1. **Komplexität**: Async-Code ist schwerer zu debuggen
2. **Spezialfall Index 0**: Insertionen vor dem ersten Stop brauchen Sonderbehandlung
3. **Race Condition bei Schedule-Änderungen**: Wenn ein anderer Request den Schedule ändert

## Handling von Race Conditions bei Schedule-Änderungen

### Das Problem

Während die asynchrone Berechnung läuft, kann ein anderer Request (der früher berechnet wurde)
den Schedule desselben Fahrzeugs ändern:

```
Zeit ─────────────────────────────────────────────────────────────────────▶

     t0                    t1                    t2
     │                     │                     │
     ▼                     ▼                     ▼
┌─────────┐          ┌─────────┐          ┌─────────┐
│Request A│          │Request B│          │Request A│
│ startet │          │ wird    │          │ will    │
│Berechnung│         │gescheduled│        │schedulen│
└─────────┘          └─────────┘          └─────────┘
     │                     │                     │
     │    Vehicle X        │                     │
     │    Schedule:        │                     │
     │    [Stop1, Stop2]   │   [Stop1, NEW, Stop2]  ← Schedule geändert!
     │                     │                     │
     └─────────────────────┴─────────────────────┘
                           
     Request A's Berechnung basiert auf [Stop1, Stop2]
     aber der Schedule ist jetzt [Stop1, NEW, Stop2]
     → Insertion-Indizes sind verschoben!
```

### Lösung 1: Schedule-Version (einfach & effektiv)

Jeder Schedule bekommt eine **Versionsnummer**, die bei jeder Änderung inkrementiert wird:

```java
class VehicleScheduleVersion {
    private final AtomicLong version = new AtomicLong(0);
    
    // Wird bei jedem scheduleRequest() aufgerufen
    public long incrementAndGet() {
        return version.incrementAndGet();
    }
    
    public long get() {
        return version.get();
    }
}

// Bei Berechnung: Version merken
class InsertionCalculationResult {
    final InsertionWithDetourData insertion;
    final long scheduleVersionAtCalculation;
    final double calculationTimestamp;
}

// Bei Validierung: Version prüfen
boolean isValid(InsertionCalculationResult result, long currentVersion) {
    return result.scheduleVersionAtCalculation == currentVersion;
}
```

**Vorteile:**
- Extrem leichtgewichtig (nur ein Long-Vergleich)
- Keine komplexe Struktur-Analyse nötig
- Fängt ALLE Arten von Schedule-Änderungen ab

**Nachteile:**
- Jede kleine Änderung invalidiert alle laufenden Berechnungen für dieses Fahrzeug
- Potenziell mehr "verschwendete" Berechnungen

### Lösung 2: Struktureller Fingerprint (präziser)

Statt einer einfachen Version berechnen wir einen **Fingerprint** der Schedule-Struktur:

```java
class ScheduleFingerprint {
    final int stopCount;
    final int[] stopLinkHashes;  // Hash der Link-IDs an jeder Position
    final long totalOccupancyHash;
    
    static ScheduleFingerprint compute(VehicleEntry entry) {
        int[] hashes = new int[entry.stops.size()];
        long occupancyHash = 0;
        for (int i = 0; i < entry.stops.size(); i++) {
            hashes[i] = entry.stops.get(i).getTask().getLink().getId().hashCode();
            occupancyHash ^= entry.stops.get(i).getOutgoingOccupancy().hashCode() * (i + 1);
        }
        return new ScheduleFingerprint(entry.stops.size(), hashes, occupancyHash);
    }
    
    boolean isCompatibleWith(ScheduleFingerprint other, int pickupIdx, int dropoffIdx) {
        // Prüfe nur die relevanten Positionen
        if (this.stopCount != other.stopCount) return false;
        
        for (int i = 0; i < Math.min(dropoffIdx, stopCount); i++) {
            if (this.stopLinkHashes[i] != other.stopLinkHashes[i]) return false;
        }
        return true;
    }
}
```

**Vorteile:**
- Präziser: Änderungen an irrelevanten Schedule-Teilen invalidieren nicht
- Weniger verschwendete Berechnungen

**Nachteile:**
- Etwas mehr Overhead bei Berechnung des Fingerprints
- Komplexere Logik

### Lösung 3: Optimistisches Locking mit Retry (empfohlen)

Kombination aus Version-Check und automatischem Retry:

```java
class AsyncInsertionScheduler {
    
    private final Map<Id<DvrpVehicle>, AtomicLong> scheduleVersions;
    private final int maxRetries = 3;
    
    public void scheduleAsync(DrtRequest request, InsertionCalculationResult result) {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            long currentVersion = scheduleVersions.get(result.vehicleId).get();
            
            if (result.scheduleVersionAtCalculation != currentVersion) {
                // Version hat sich geändert → Neuberechnung
                result = recalculateInsertion(request, result.vehicleId);
                if (result == null) {
                    // Kein gültiges Fahrzeug mehr → Re-queue für anderen Fahrzeug-Pool
                    requeue(request);
                    return;
                }
                continue;
            }
            
            // Versuche zu schedulen (atomar)
            synchronized (getVehicleLock(result.vehicleId)) {
                // Double-Check nach Lock-Akquisition
                if (scheduleVersions.get(result.vehicleId).get() == currentVersion) {
                    doSchedule(result);
                    scheduleVersions.get(result.vehicleId).incrementAndGet();
                    return; // Erfolg!
                }
            }
            // Lock-Konflikt → nächster Versuch
        }
        
        // Alle Retries fehlgeschlagen → Re-queue
        requeue(request);
    }
}
```

### Lösung 4: Partitionierung (bereits vorhanden!)

Der bestehende `ParallelUnplannedRequestInserter` nutzt bereits **Partitionierung**:
Fahrzeuge werden in Partitionen aufgeteilt, und jede Partition wird von einem Thread bearbeitet.

```
Partition 1: [Vehicle A, Vehicle B, Vehicle C]  → Thread 1
Partition 2: [Vehicle D, Vehicle E, Vehicle F]  → Thread 2
Partition 3: [Vehicle G, Vehicle H, Vehicle I]  → Thread 3
```

**Innerhalb einer Partition** gibt es keine Race Conditions, da nur ein Thread Zugriff hat!

**Erweiterung für Async:**
```java
class AsyncPartitionedInserter {
    
    // Jede Partition hat ihre eigene Request-Queue und Solution-Queue
    class PartitionWorker {
        final ConcurrentLinkedQueue<DrtRequest> requestQueue;
        final ConcurrentLinkedQueue<InsertionResult> solutionQueue;
        final Set<DvrpVehicle> vehicles;  // Exklusiv für diese Partition
        
        // Kein Race Condition möglich - nur dieser Worker 
        // bearbeitet diese Fahrzeuge!
    }
}
```

### Empfohlener Ansatz: Kombination

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ASYNC INSERTION SYSTEM                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. PARTITIONIERUNG (Erste Verteidigungslinie)                          │
│     → Fahrzeuge exklusiv einer Partition zugeordnet                     │
│     → Keine Cross-Partition Race Conditions                             │
│                                                                         │
│  2. SCHEDULE-VERSION (Innerhalb einer Partition)                        │
│     → Leichtgewichtiger Check vor dem Scheduling                        │
│     → Fängt Änderungen durch andere Requests derselben Partition ab     │
│                                                                         │
│  3. OPTIMISTIC RETRY (Bei Konflikten)                                   │
│     → Schnelle Neuberechnung bei Version-Mismatch                       │
│     → Maximal 2-3 Retries, dann Re-queue                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Implementierung: VersionedVehicleEntry

```java
public class VersionedVehicleEntry extends VehicleEntry {
    private final long scheduleVersion;
    
    public VersionedVehicleEntry(VehicleEntry base, long scheduleVersion) {
        super(base);
        this.scheduleVersion = scheduleVersion;
    }
    
    public long getScheduleVersion() {
        return scheduleVersion;
    }
}

// Usage in InsertionCalculationResult
record InsertionCalculationResult(
    InsertionWithDetourData insertion,
    long scheduleVersion,     // Version bei Berechnung
    double calculationTime,   // Zeitstempel
    int pickupIdx,
    int dropoffIdx
) {
    boolean isVersionValid(long currentVersion) {
        return this.scheduleVersion == currentVersion;
    }
}
```

### Fazit: Race Conditions sind beherrschbar!

| Strategie | Overhead | Präzision | Empfehlung |
|-----------|----------|-----------|------------|
| Partitionierung | Niedrig | Hoch (keine Cross-Partition RC) | ✅ Basis |
| Schedule-Version | Sehr niedrig | Mittel (invalidiert alles) | ✅ Zusätzlich |
| Struktureller Fingerprint | Mittel | Hoch | Optional |
| Optimistic Retry | Niedrig | Hoch | ✅ Bei Konflikten |

**Die Kombination aus Partitionierung + Schedule-Version + Retry** ist robust, 
leichtgewichtig und einfach zu implementieren.

## Warum DiversionPoint besser ist als reine SlackTime-Projektion

| Aspekt | SlackTime-Projektion | DiversionPoint-basiert |
|--------|---------------------|------------------------|
| Präzision | Geschätzte Berechnungszeit | Exakte physische Position |
| Gültigkeit | Verfall nach Zeit | Stabil solange Schedule unverändert |
| Validierung | SlackTime muss neu geprüft werden | Nur Struktur-Check nötig |
| Komplexität | Tuning der Projektionszeit nötig | Natürlicher Anker |
| Konservativität | Muss konservativ sein | Kann exakt sein |

Der DiversionPoint-Ansatz nutzt eine **physische Invariante**: Das Fahrzeug kann nicht 
mehr zurück. Dies ist stabiler als eine zeitbasierte Projektion.

## Metriken zum Tracken

- `validationSuccessRate`: Anteil gültiger Insertionen
- `averageCalculationTime`: Mittlere Berechnungszeit
- `averageRequestLatency`: Zeit von Request-Eingang bis Scheduling
- `wastedCalculations`: Anzahl invalidierter Berechnungen
