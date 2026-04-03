## Plan: CCH/CATCHUp Time-Dependent Router für MATSim

Implementierung eines Customizable Contraction Hierarchies (CCH)-Routers mit CATCHUp-Erweiterung
für zeitabhängiges Routing in MATSim. Basierend auf dem State-of-the-Art (Dibbelt/Strasser/Wagner
2016–2020) statt dem veralteten ATCH-Paper (2010). Drei-Phasen-Architektur: topologisches
Preprocessing (einmalig), Gewichts-Customization (pro Iteration, ~1–3s), bidirektionale Query
(~0,05–0,2ms). Integriert sich in das bestehende SpeedyGraph-Ökosystem und
`LeastCostPathCalculator`-Interface. Erwartet 10–30× Query-Speedup gegenüber SpeedyALT bei großen
Netzwerken.

---

### Steps

#### 1. `SpeedyCHData` – Topologische Vorverarbeitung (einmalig)

**Datei:** `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyCHData.java`

Verantwortlich für die metrik-unabhängige Phase:

- **Node-Ordering** per Edge-Difference-Heuristik (zunächst einfacher als Nested Dissection):
  `priority = #shortcuts_added - #edges_removed + #already_contracted_neighbors`
- **Kontraktion** mit Witness-Search (Hop-Limit k=5), Shortcut-Erkennung
- Pro Shortcut speichern: `fromNode`, `toNode`, `childEdge1`, `childEdge2` (für Pfad-Entpackung)
- Nutzt `SpeedyGraph` als Eingabe, erzeugt `int[]`-basierte Overlay-Daten
  (Aufwärts-/Abwärts-Adjazenzlisten) im gleichen Cache-freundlichen Stil wie `SpeedyGraph`
  (`int[]` statt Objekte)
- **Parallele Kontraktion** über Independent Sets (nicht-benachbarte Knoten gleichzeitig
  kontrahieren) mit `ExecutorService` analog zu `SpeedyALTData.calcLandmarks()`
- Optionaler Disk-Cache (Netzwerk-Topologie-Hash → serialisierte CH-Struktur), da sich die
  Topologie zwischen MATSim-Runs nicht ändert

**Referenz-Pattern:** `SpeedyALTData.java` – Array-Layout, ExecutorService-Parallelisierung, SpeedyGraph-Nutzung

---

#### 2. `SpeedyCHCustomizer` – Gewichts-Customization (pro Iteration)

**Datei:** `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyCHCustomizer.java`

- Implementiert `IterationStartsListener` für automatische Re-Customization wenn sich `TravelTime` ändert
- **Bottom-Up-Traversierung** der CH-Hierarchie: für jeden Shortcut den Kostenwert aus den zwei
  Kind-Kanten berechnen
- **CATCHUp-Erweiterung:** Statt Skalarwert pro Shortcut werden Travel-Time-Functions (TTFs) als
  `double[]`-Arrays mit einem Eintrag pro Time-Bin gespeichert
  (Standard: 96 Bins à 15min = 24h, konfigurierbar über
  `TravelTimeCalculatorConfigGroup.getTraveltimeBinSize()`)
- TTFs für Shortcuts: `ttf_shortcut(t) = ttf_child1(t) + ttf_child2(t + ttf_child1(t))`
  (Link-Verknüpfung mit Zeitverschiebung)
- **Lower-Bound** pro Shortcut: `min(ttf_shortcut)` über alle Bins – wird für Rückwärtssuche benötigt
- Thread-sicher und teilbar wie `SpeedyALTData`
- Customization parallelisierbar (Level-weise Bottom-Up, alle Shortcuts eines Levels unabhängig)

---

#### 3. `SpeedyCH` implements `LeastCostPathCalculator` – Bidirektionale Query

**Datei:** `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyCH.java`

- **Vorwärtssuche:** nur Aufwärtskanten relaxieren, zeitkorrekte TTF-Evaluation mit akkumulierter Startzeit
- **Rückwärtssuche:** nur Abwärtskanten relaxieren, Lower-Bound-Kosten (Minimum der TTFs) nutzen
- **Stall-on-Demand:** Wenn ein settled Knoten über Abwärtskanten günstiger erreichbar wäre, wird er
  ignoriert (~30–60% Pruning)
- **Meeting-Point:** Minimale Summe aus Vorwärts- und Rückwärtskosten, exakte
  Kosten-Neuberechnung am Treffpunkt
- **Pfad-Entpackung:** Rekursive Shortcut-Auflösung in `constructPath()`, lazy – nur wenn
  `path.links`/`path.nodes` tatsächlich abgefragt werden (analog zu `lazyPathCreation` in
  `OneToManyPathSearch`)
- Beide `calcLeastCostPath`-Methoden (Link-basiert + Node-basiert) implementieren
- **Turn-Restrictions:** `TurnRestrictionsContext` / Colored Nodes im Overlay-Graph berücksichtigen
  (analog zu `SpeedyALT.calcLeastCostPath(Link, Link, ...)`)
- Nutzt `DAryMinHeap` mit d=6 für beide Suchrichtungen (zwei Instanzen)
- Alle Arrays im Konstruktor vorab allozieren (nicht thread-safe pro Instanz, thread-safe shared
  Data analog zum SpeedyALT-Pattern)

**Referenz-Pattern:** `SpeedyALT.java` – `calcLeastCostPath`-Methoden, `constructPath()`, Turn-Restriction-Handling, Iteration-Tracking

---

#### 4. `SpeedyCHFactory` implements `LeastCostPathCalculatorFactory`

**Datei:** `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyCHFactory.java`

- `@Singleton` (analog zu `SpeedyALTFactory.java`)
- `ConcurrentHashMap`-Caches für `SpeedyGraph`, `SpeedyCHData` und `SpeedyCHCustomizer` pro Network
- `@Inject`-Konstruktor mit `GlobalConfigGroup` (Threads), `RoutingConfigGroup` und
  `TravelTimeCalculatorConfigGroup` (BinSize)
- `createPathCalculator()`: Baut Graph → CH-Topologie → Customizer → gibt `SpeedyCH`-Instanz zurück
- Customizer wird bei erster `createPathCalculator()`-Anfrage mit der aktuellen
  `TravelDisutility`/`TravelTime` initialisiert

---

#### 5. Config-Integration und Guice-Binding

| Datei | Änderung |
|-------|----------|
| `ControllerConfigGroup.java` (Zeile 37) | `SpeedyCH` zum `RoutingAlgorithmType`-Enum hinzufügen |
| `LeastCostPathCalculatorModule.java` (Zeile 44) | Neuer `else if`-Branch: `SpeedyCH → SpeedyCHFactory.class` |
| `VspConfigConsistencyCheckerImpl.java` (Zeile 525) | `switch`-Case um `SpeedyCH` erweitern (kein Warning) |

---

#### 6. Tests

Alle in `matsim/src/test/java/org/matsim/core/router/speedy/`:

| Testdatei | Beschreibung |
|-----------|-------------|
| `SpeedyCHTest.java` | `extends AbstractLeastCostPathCalculatorTestWithTurnRestrictions` – analog zu `SpeedyALTTest.java` |
| `SpeedyCHCorrectnessTest.java` | 1000 zufällige OD-Paare, Ergebnisse identisch mit SpeedyDijkstra (±ε). Auch mit zeitabhängigen TravelTimes. |
| `SpeedyCHCustomizerTest.java` | Shortcut-TTFs korrekt, Lower Bounds stimmen, Re-Customization korrekt. |
| `SpeedyCHDataTest.java` | Node-Ordering-Invarianten, korrekte Overlay-Graph-Struktur. |

Geänderte Test-Dateien:

| Datei | Änderung |
|-------|----------|
| `ReRoutingIT.java` | Neuer `testReRoutingSpeedyCH()` analog zu `testReRoutingSpeedyALT()` |
| `TripRouterModuleTest.java` | Deckt neuen Enum automatisch ab |

---

### Neue Dateien

| Datei | Paket | Beschreibung |
|-------|-------|-------------|
| `SpeedyCHData.java` | `o.m.core.router.speedy` | Topologische CH-Vorverarbeitung |
| `SpeedyCHCustomizer.java` | `o.m.core.router.speedy` | TTF-basierte Gewichts-Customization |
| `SpeedyCH.java` | `o.m.core.router.speedy` | Bidirektionale Query mit Stall-on-Demand |
| `SpeedyCHFactory.java` | `o.m.core.router.speedy` | Factory + Caching, `@Singleton` |
| `SpeedyCHTest.java` | Test | Erbt `AbstractLeastCostPathCalculatorTestWithTurnRestrictions` |
| `SpeedyCHCorrectnessTest.java` | Test | Vergleich gegen SpeedyDijkstra |
| `SpeedyCHCustomizerTest.java` | Test | TTF-Korrektheit |
| `SpeedyCHDataTest.java` | Test | Ordering/Overlay-Invarianten |

### Geänderte Dateien

| Datei | Änderung |
|-------|----------|
| `ControllerConfigGroup.java` | `SpeedyCH` zum Enum |
| `LeastCostPathCalculatorModule.java` | Neuer Binding-Branch |
| `VspConfigConsistencyCheckerImpl.java` | Switch-Case erweitern |
| `ReRoutingIT.java` | Neuer Testfall |
| `TripRouterModuleTest.java` | for-each über `values()` |

---

### Further Considerations

1. **Implementierungsreihenfolge:** Erst **statische CCH** (nur `getLinkMinimumTravelDisutility`,
   Skalarwerte) implementieren und gegen alle Tests validieren. Dann **TTF-Erweiterung (CATCHUp)**
   als zweiten Schritt. Reduziert Debug-Komplexität erheblich.

2. **Speicher-Budget für TTFs:** 200K Shortcuts × 96 Time-Bins × 8 Bytes = ~150 MB.
   Deutschland-Netzwerke (~2M Knoten, ~1M Shortcuts) ~750 MB. Konfigurierbare Bin-Size oder
   ε-Approximation als Sicherheitsventil. Alternativ: Nur Shortcuts ab Level k mit TTFs.

3. **Nested Dissection vs. Edge-Difference:** Nested Dissection liefert ~30% weniger Shortcuts.
   **Empfehlung:** Zunächst Edge-Difference (einfacher), Nested Dissection als optionale Verbesserung.
   Edge-Difference: `priority = #shortcuts_added - #edges_removed + #already_contracted_neighbors`.

