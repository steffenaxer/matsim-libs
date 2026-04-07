/* *********************************************************************** *
 * project: org.matsim.*
 * HeapBenchmark.java
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2025 by the members listed in the COPYING,        *
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
package org.matsim.core.router.speedy;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Micro-benchmark comparing the old {@code NodeMinHeap} (O(n) linear-scan
 * decreaseKey) vs the new {@link DAryMinHeap} (O(1) pos[] lookup decreaseKey,
 * d-ary branching).
 *
 * <p>Simulates a Dijkstra-like workload: for a graph of N nodes with ~2.4
 * outgoing edges per node (typical Berlin road network), measure the total
 * time for many Dijkstra-equivalent operations (insert, decreaseKey, poll).
 *
 * <p>Run with:
 * <pre>
 *   java -cp ... org.matsim.core.router.speedy.HeapBenchmark
 * </pre>
 *
 * @author Steffen Axer
 */
public class HeapBenchmark {

	private static final int NODE_COUNT   = 88_000;   // Berlin-scale
	private static final int AVG_DEGREE   = 3;        // ~2.4 in Berlin, round up to 3
	private static final int DIJKSTRA_RUNS = 50;      // full Dijkstra simulations
	private static final int WARMUP_RUNS   = 20;

	public static void main(String[] args) {
		System.out.printf("Heap Benchmark: %,d nodes, avg degree %d, %d Dijkstra runs (+ %d warmup)%n%n",
				NODE_COUNT, AVG_DEGREE, DIJKSTRA_RUNS, WARMUP_RUNS);

		// Generate a random sparse graph adjacency list
		int[][] adj = generateRandomGraph(NODE_COUNT, AVG_DEGREE, 42);

		// Warm up
		System.out.println("Warming up ...");
		for (int i = 0; i < WARMUP_RUNS; i++) {
			runDijkstraNodeMinHeap(adj, NODE_COUNT, i);
			runDijkstraDAryMinHeap(adj, NODE_COUNT, 4, i);
		}

		// Benchmark NodeMinHeap (old)
		System.out.println("Benchmarking NodeMinHeap (old, O(n) decreaseKey) ...");
		long totalOld = 0;
		for (int i = 0; i < DIJKSTRA_RUNS; i++) {
			long t0 = System.nanoTime();
			runDijkstraNodeMinHeap(adj, NODE_COUNT, 1000 + i);
			totalOld += System.nanoTime() - t0;
		}
		double oldMs = totalOld / (DIJKSTRA_RUNS * 1_000_000.0);

		// Benchmark DAryMinHeap with d=4
		System.out.println("Benchmarking DAryMinHeap (new, d=4) ...");
		long totalD4 = 0;
		for (int i = 0; i < DIJKSTRA_RUNS; i++) {
			long t0 = System.nanoTime();
			runDijkstraDAryMinHeap(adj, NODE_COUNT, 4, 1000 + i);
			totalD4 += System.nanoTime() - t0;
		}
		double d4Ms = totalD4 / (DIJKSTRA_RUNS * 1_000_000.0);

		// Benchmark DAryMinHeap with d=6
		System.out.println("Benchmarking DAryMinHeap (new, d=6) ...");
		long totalD6 = 0;
		for (int i = 0; i < DIJKSTRA_RUNS; i++) {
			long t0 = System.nanoTime();
			runDijkstraDAryMinHeap(adj, NODE_COUNT, 6, 1000 + i);
			totalD6 += System.nanoTime() - t0;
		}
		double d6Ms = totalD6 / (DIJKSTRA_RUNS * 1_000_000.0);

		// Benchmark DAryMinHeap with d=2 (binary, like NodeMinHeap but with O(1) lookup)
		System.out.println("Benchmarking DAryMinHeap (new, d=2, binary) ...");
		long totalD2 = 0;
		for (int i = 0; i < DIJKSTRA_RUNS; i++) {
			long t0 = System.nanoTime();
			runDijkstraDAryMinHeap(adj, NODE_COUNT, 2, 1000 + i);
			totalD2 += System.nanoTime() - t0;
		}
		double d2Ms = totalD2 / (DIJKSTRA_RUNS * 1_000_000.0);

		// Print results
		System.out.println();
		System.out.println("╔══════════════════════════════════════════════════════════════╗");
		System.out.println("║              Heap Benchmark Results                          ║");
		System.out.println("╠══════════════════════════════════════════════════════════════╣");
		System.out.printf( "║  NodeMinHeap (old, binary, O(n) decreaseKey) : %8.1f ms  ║%n", oldMs);
		System.out.printf( "║  DAryMinHeap (d=2, binary, O(1) decreaseKey) : %8.1f ms  ║%n", d2Ms);
		System.out.printf( "║  DAryMinHeap (d=4, O(1) decreaseKey)         : %8.1f ms  ║%n", d4Ms);
		System.out.printf( "║  DAryMinHeap (d=6, O(1) decreaseKey)         : %8.1f ms  ║%n", d6Ms);
		System.out.println("╠══════════════════════════════════════════════════════════════╣");
		System.out.printf( "║  Speedup d=2 vs NodeMinHeap : %.2fx                        ║%n", oldMs / d2Ms);
		System.out.printf( "║  Speedup d=4 vs NodeMinHeap : %.2fx                        ║%n", oldMs / d4Ms);
		System.out.printf( "║  Speedup d=6 vs NodeMinHeap : %.2fx                        ║%n", oldMs / d6Ms);
		System.out.println("╚══════════════════════════════════════════════════════════════╝");
	}

	// ----- Graph generation -----

	private static int[][] generateRandomGraph(int n, int avgDegree, long seed) {
		Random rng = new Random(seed);
		int[][] adj = new int[n][];
		for (int i = 0; i < n; i++) {
			// Poisson-like: random number of edges around avgDegree
			int deg = Math.max(1, avgDegree + rng.nextInt(3) - 1);
			adj[i] = new int[deg];
			for (int j = 0; j < deg; j++) {
				adj[i][j] = rng.nextInt(n);
			}
		}
		return adj;
	}

	// ----- Dijkstra simulation with OLD NodeMinHeap -----

	private static void runDijkstraNodeMinHeap(int[][] adj, int n, long seed) {
		Random rng = new Random(seed);
		int source = rng.nextInt(n);

		double[] dist = new double[n];
		Arrays.fill(dist, Double.MAX_VALUE);
		dist[source] = 0;

		OldNodeMinHeap heap = new OldNodeMinHeap(n, i -> dist[i], (i, c) -> dist[i] = c);
		heap.insert(source);

		boolean[] settled = new boolean[n];
		int settledCount = 0;

		while (!heap.isEmpty() && settledCount < n) {
			int u = heap.poll();
			if (settled[u]) continue;
			settled[u] = true;
			settledCount++;

			double uDist = dist[u];
			if (uDist == Double.MAX_VALUE) break;

			for (int v : adj[u]) {
				double edgeWeight = 1.0 + (u ^ v) % 10; // deterministic pseudo-random weight
				double newDist = uDist + edgeWeight;
				if (newDist < dist[v]) {
					if (dist[v] == Double.MAX_VALUE) {
						dist[v] = newDist;
						heap.insert(v);
					} else {
						heap.decreaseKey(v, newDist);
					}
				}
			}
		}
	}

	// ----- Dijkstra simulation with NEW DAryMinHeap -----

	private static void runDijkstraDAryMinHeap(int[][] adj, int n, int d, long seed) {
		Random rng = new Random(seed);
		int source = rng.nextInt(n);

		DAryMinHeap heap = new DAryMinHeap(n, d);
		heap.insert(source, 0);

		double[] dist = new double[n];
		Arrays.fill(dist, Double.MAX_VALUE);
		dist[source] = 0;

		boolean[] settled = new boolean[n];
		int settledCount = 0;

		while (!heap.isEmpty() && settledCount < n) {
			int u = heap.poll();
			if (settled[u]) continue;
			settled[u] = true;
			settledCount++;

			double uDist = dist[u];
			if (uDist == Double.MAX_VALUE) break;

			for (int v : adj[u]) {
				double edgeWeight = 1.0 + (u ^ v) % 10;
				double newDist = uDist + edgeWeight;
				if (newDist < dist[v]) {
					if (dist[v] == Double.MAX_VALUE) {
						dist[v] = newDist;
						heap.insert(v, newDist);
					} else {
						dist[v] = newDist;
						heap.decreaseKey(v, newDist);
					}
				}
			}
		}
	}

	// ========================
	// Old NodeMinHeap (embedded copy from before the migration)
	// ========================

	private static class OldNodeMinHeap {
		private final int[] heap;
		private final int[] pos;
		private int size = 0;
		private final CostGetter costGetter;
		private final CostSetter costSetter;

		interface CostGetter { double getCost(int index); }
		interface CostSetter { void setCost(int index, double cost); }

		OldNodeMinHeap(int nodeCount, CostGetter costGetter, CostSetter costSetter) {
			this.heap = new int[nodeCount];
			this.pos = new int[nodeCount];
			this.costGetter = costGetter;
			this.costSetter = costSetter;
		}

		void insert(int node) {
			int i = this.size;
			this.size++;
			double nodeCost = this.costGetter.getCost(node);
			int parent = (i - 1) / 2;
			while (i > 0 && nodeCost < this.costGetter.getCost(this.heap[parent])) {
				this.heap[i] = this.heap[parent];
				this.pos[this.heap[parent]] = i;
				i = parent;
				parent = (parent - 1) / 2;
			}
			this.heap[i] = node;
			this.pos[node] = i;
		}

		void decreaseKey(int node, double cost) {
			// O(n) linear scan to find node -- this is the bottleneck!
			int i;
			for (i = 0; i < this.size; i++) {
				if (this.heap[i] == node) {
					break;
				}
			}
			if (this.costGetter.getCost(this.heap[i]) < cost) {
				throw new IllegalArgumentException("existing cost is already smaller than new cost.");
			}
			this.costSetter.setCost(node, cost);
			int parent = (i - 1) / 2;
			while (i > 0 && cost < this.costGetter.getCost(this.heap[parent])) {
				this.heap[i] = this.heap[parent];
				this.pos[this.heap[parent]] = i;
				i = parent;
				parent = (parent - 1) / 2;
			}
			this.heap[i] = node;
			this.pos[node] = i;
		}

		int poll() {
			if (this.size == 0) throw new NoSuchElementException("heap is empty");
			if (this.size == 1) { this.size--; return this.heap[0]; }
			int root = this.heap[0];
			int lastNode = this.heap[this.size - 1];
			this.size--;
			this.heap[0] = lastNode;
			this.pos[lastNode] = 0;
			minHeapify(0);
			return root;
		}

		boolean isEmpty() { return this.size == 0; }

		private void minHeapify(int i) {
			int left = 2 * i + 1;
			int right = 2 * i + 2;
			int smallest = i;
			if (left <= (this.size - 1) && this.costGetter.getCost(this.heap[left]) < this.costGetter.getCost(this.heap[i])) {
				smallest = left;
			}
			if (right <= (this.size - 1) && this.costGetter.getCost(this.heap[right]) < this.costGetter.getCost(this.heap[smallest])) {
				smallest = right;
			}
			if (smallest != i) {
				int tmp = this.heap[smallest];
				this.heap[smallest] = this.heap[i];
				this.pos[this.heap[i]] = smallest;
				this.heap[i] = tmp;
				this.pos[tmp] = i;
				minHeapify(smallest);
			}
		}
	}
}
