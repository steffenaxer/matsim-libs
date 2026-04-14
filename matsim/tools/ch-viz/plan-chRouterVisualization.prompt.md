# Plan: CH-Router Visualisierung — Berlin-Netz, D3.js, Video-Export

Zwei Komponenten: (1) Java-Exporter (`CHQueryExporter`) der auf dem Berlin-Netzwerk einen instrumentierten CH-Query **und** einen Dijkstra-Query gleichzeitig ausführt und als JSON exportiert, (2) eine standalone `ch-visualization.html` mit D3.js + Canvas, dunklem Theme, Split-Screen Dijkstra-vs-CH-Vergleich, Shortcut-Unpack-Animation, Statistik-Counter und nativem WebM-Video-Export via `MediaRecorder`.

## Steps

### 1. `CHQueryExporter.java`

**Location:** `matsim/src/main/java/org/matsim/core/router/speedy/CHQueryExporter.java`

`main()` lädt Berlin-Netzwerk via `NetworkUtils.readNetwork("examples/scenarios/berlin/network.xml.gz")`, baut `SpeedyGraph` + `CHGraph` (via `CHBuilder` + `CHTTFCustomizer`). Führt zwei instrumentierte Queries vom gleichen OD-Paar aus (z.B. Kreuzberg→Charlottenburg):

- **Instrumentierter CH-Query**: Kopie der Loop-Logik aus `CHRouter.calcLeastCostPathImpl()` (Zeilen 152–321). Pro PQ-Poll ein JSON-Event: `{step, dir:"fwd"|"bwd", nodeIdx, cost, parentIdx, stalled, isMeeting}`.
- **Instrumentierter Dijkstra-Query**: Kopie der Loop-Logik aus `SpeedyDijkstra.calcLeastCostPathImpl()` (Zeilen 110–178). Pro PQ-Poll ein Event: `{step, nodeIdx, cost}`.
- **Shortcut-Unpack-Baum**: Für den CH-Pfad die rekursive `edgeLower1`/`edgeLower2`-Struktur als verschachtelte JSON-Objekte exportieren (`{gIdx, isShortcut, children: [{...}, {...}]}`), damit das Frontend die Entfaltung animieren kann.
- **Knoten-Export**: Alle Netzwerk-Knoten mit Koordinaten (via `TransformationFactory.getCoordinateTransformation("GK4", "WGS84")` → lon/lat), `nodeLevel[i]` aus `CHGraph.nodeLevel` (Zeile 114), und Netzwerk-Kanten als fromNode/toNode-Paare.
- **Finaler Pfad**: Link-Sequence mit Koordinaten für den entpackten Shortest Path.
- Ausgabe: `ch-query-data.json`. Optional BBox-Filter (z.B. 5km um Mitte) um das Netz auf ~2000–4000 sichtbare Knoten zu reduzieren.

### 2. `ch-visualization.html`

**Location:** `matsim/tools/ch-viz/ch-visualization.html`

Single-file HTML mit D3.js (v7 CDN), `<canvas>` als Haupt-Rendering-Layer. Aufbau:

- **Split-Screen-Layout (16:9)**: Links ein Dijkstra-Canvas, rechts ein CH-Canvas, synchron animiert vom gleichen Step-Counter. Dijkstra-Seite zeigt die wachsende "Wolke" aller settled Knoten (rot/orange). CH-Seite zeigt den schlanken bidirektionalen Diamond (Forward blau, Backward orange). Header: "Dijkstra" vs "Contraction Hierarchies".
- **Karten-Layer**: Berlin-Netzwerk-Kanten als dünne dunkelgraue Linien via `d3.geoMercator()` Projektion, auf beide Canvases gerendert.
- **Animation-Loop** (`requestAnimationFrame`): Pro Frame N Events abarbeiten (steuerbar via Speed-Slider). Settled nodes als Kreise (Radius proportional zu Level bei CH), Kanten zum Parent-Node als Linien. Stall-on-demand bei CH als kurzer grauer Flash. Meeting-Point als goldener Glow-Pulse.
- **2.5D-Toggle**: Optional Knoten-Y nach `nodeLevel` nach oben versetzt — zeigt die Kegelform der CH-Hierarchie.

### 3. Shortcut-Unpack-Animation

Phase 2 nach dem Meeting-Point:

- CH-Canvas zoomt auf den Pfad-Bereich.
- Zeigt den CH-Pfad zunächst als wenige gestrichelte "Shortcut-Bögen" in der Hierarchie (hoch über dem Netzwerk).
- Entpackt schrittweise: jeder Shortcut spaltet sich in zwei Kinder-Kanten, bis nur noch reale Kanten übrig sind. Animation mit smooth Morph-Transitions.
- Finaler Pfad als durchgehende grüne animated-dash-Linie.

### 4. Statistik-Overlay & Erklärungsboxen

- **Live-Counter** auf beiden Seiten: "Settled: X" + Prozent des Gesamtnetzes. CH-Seite zeigt zusätzlich "Forward: Y / Backward: Z".
- **Finaler Vergleich**: Am Ende großes Overlay "Dijkstra: 4.823 Knoten besucht — CH: 127 Knoten besucht → **97.4% weniger Search Space**".
- **Info-Boxen** die bei Phasenwechsel einblenden: "→ Forward-Suche expandiert aufwärts in der Hierarchie", "→ Stall-on-demand: Knoten v gepruned", "→ Meeting-Point gefunden", "→ Shortcuts werden entpackt", mit Fade-in/out.

### 5. Styling & Branding

- Dunkles Theme: Hintergrund `#0d1117`, Netzwerk-Kanten `#1e2937`, Text weiß/grau.
- Akzentfarben: Forward `#58a6ff` (blau), Backward `#f78166` (orange), Dijkstra `#da3633` (rot), Finaler Pfad `#3fb950` (grün), Meeting `#d29922` (gold).
- iteratively.io Logo-Watermark unten rechts, dezent semi-transparent.
- Smooth easing (cubic-bezier) auf allen Transitions.

### 6. UI-Controls & Video-Export

- **Controls**: Play/Pause, Step-Forward, Speed-Slider (1x/5x/10x/20x), Fortschrittsbalken, Reset-Button, 2.5D-Toggle.
- **Auto-Play-Modus**: Ein "Cinema Mode"-Button der Controls versteckt und die Animation automatisch mit vordefinierten Pausen zwischen Phasen abspielt (ideal für Recording).
- **Video-Export**: "🔴 Record"-Button nutzt `canvas.captureStream(60)` + `MediaRecorder` (VP9/WebM, 60fps). Beide Canvases werden auf einen Off-Screen Composite-Canvas gezeichnet. Start bei Play, Stop bei Ende der Animation → automatischer Download `ch-router-berlin.webm`. Kein html2canvas nötig — Canvas `captureStream` ist nativ und performant.

## Further Considerations

1. **OD-Paar-Auswahl**: Default-Route Kreuzberg→Charlottenburg (~8km, produziert ~80–150 CH-settled vs ~3000–5000 Dijkstra-settled). Zusätzlich ein Dropdown mit 3–4 vorberechneten OD-Paaren unterschiedlicher Distanz (kurz/mittel/lang) um den Effekt bei verschiedenen Entfernungen zu zeigen.
2. **Performance bei 10k+ Knoten**: Canvas-Rendering der Netzwerk-Basiskarte nur einmal in ein Off-Screen-Canvas, dann per `drawImage()` in jedem Frame blitten. Settled-Knoten inkrementell drüberzeichnen. So bleibt 60fps auch bei dichtem Berlin-Netz.
3. **Datei-Größe**: Die JSON mit ~10k Knoten + ~25k Kanten + ~5000 Dijkstra-Events + ~150 CH-Events wird ~2–4MB. Kann als gzip inline Base64 in die HTML eingebettet werden für echte Single-file-Distribution, oder als separate Datei geladen.

## Key Source References

| Artifact | Path |
|---|---|
| CH-Query-Loop (static) | `matsim/src/main/java/org/matsim/core/router/speedy/CHRouter.java` (L152–321) |
| CH-Query-Loop (time-dep) | `matsim/src/main/java/org/matsim/core/router/speedy/CHRouterTimeDep.java` (L178–380) |
| CH-Graph (nodeLevel, edges, shortcuts) | `matsim/src/main/java/org/matsim/core/router/speedy/CHGraph.java` |
| CH-Builder (contraction) | `matsim/src/main/java/org/matsim/core/router/speedy/CHBuilder.java` |
| CH-Factory (build pipeline) | `matsim/src/main/java/org/matsim/core/router/speedy/CHRouterFactory.java` |
| TTF-Customizer | `matsim/src/main/java/org/matsim/core/router/speedy/CHTTFCustomizer.java` |
| Static Customizer | `matsim/src/main/java/org/matsim/core/router/speedy/CHCustomizer.java` |
| Dijkstra reference | `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyDijkstra.java` (L110–178) |
| SpeedyGraph (node/link access) | `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyGraph.java` |
| SpeedyGraphBuilder (spatial ordering) | `matsim/src/main/java/org/matsim/core/router/speedy/SpeedyGraphBuilder.java` |
| Berlin network | `examples/scenarios/berlin/network.xml.gz` (GK4 coord system) |
| Berlin config | `examples/scenarios/berlin/config.xml` |
| Edge unpacking (shortcuts→real edges) | `CHRouter.unpackEdge()` (L368–394), `CHRouterTimeDep.unpackEdge()` (L429–448) |
| Coord transformation | `TransformationFactory.getCoordinateTransformation("GK4", "WGS84")` |

