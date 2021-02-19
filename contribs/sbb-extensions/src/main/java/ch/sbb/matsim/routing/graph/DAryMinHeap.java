package ch.sbb.matsim.routing.graph;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Implementation of a d-ary min-heap.
 *
 * A d-ary min-heap is a generalization of the binary min-heap,
 * which provides better cpu cache locality and has faster decrease-key operations
 * but slower poll operations. But in Dijkstra's algorithm, decrease-key is more common
 * than poll, so it should still be beneficial.
 *
 * @author mrieser / Simunto
 */
class DAryMinHeap {
	private final int[] heap;
	private final int[] pos;
	private int size = 0;
	private final int d;
	private final CostGetter costGetter;
	private final CostSetter costSetter;

	public interface CostGetter {
		double getCost(int index);
	}

	public interface CostSetter {
		void setCost(int index, double cost);
	}

	DAryMinHeap(int nodeCount, int d, CostGetter costGetter, CostSetter costSetter) {
		this.heap = new int[nodeCount]; // worst case: every node is part of the heap
		this.pos = new int[nodeCount]; // worst case: every node is part of the heap
		this.d = d;
		this.costGetter = costGetter;
		this.costSetter = costSetter;
	}

	void insert(int node) {
		int i = this.size;
		this.size++;

		double nodeCost = this.costGetter.getCost(node);
		// sift up
		int parent = parent(i);
		while (i > 0 && nodeCost < this.costGetter.getCost(this.heap[parent])) {
			this.heap[i] = this.heap[parent];
			this.pos[this.heap[parent]] = i;
			i = parent;
			parent = parent(parent);
		}
		this.heap[i] = node;
		this.pos[node] = i;
	}

	void decreaseKey(int node, double cost) {
		int i = this.pos[node];
		if (i < 0 || this.heap[i] != node) {
			System.err.println("oops "  + node + "   " + i + "   " + (i < 0 ? "" : this.heap[i]));
			throw new NoSuchElementException();
		}
		if (this.costGetter.getCost(this.heap[i]) < cost) {
			throw new IllegalArgumentException("existing cost is already smaller than new cost.");
		}

		this.costSetter.setCost(node, cost);

		// sift up
		int parent = parent(i);
		while (i > 0 && cost < this.costGetter.getCost(this.heap[parent])) {
			this.heap[i] = this.heap[parent];
			this.pos[this.heap[parent]] = i;
			i = parent;
			parent = parent(parent);
		}
		this.heap[i] = node;
		this.pos[node] = i;
	}

	int poll() {
		if (this.size == 0) {
			throw new NoSuchElementException("heap is empty");
		}
		if (this.size == 1) {
			this.size--;
			return this.heap[0];
		}

		int root = this.heap[0];
		this.pos[root] = -1;

		// remove the last item, set it as new root
		this.size--;
		this.heap[0] = this.heap[this.size];
		this.pos[this.heap[0]] = 0;

		// sift down
		minHeapify(0);

		return root;
	}

	int peek() {
		if (this.size == 0) {
			throw new NoSuchElementException("heap is empty");
		}
		return this.heap[0];
	}

	public boolean remove(int node) {
		int i = this.pos[node];
		if (i < 0) {
			return false;
		}

		this.decreaseKey(node, Double.NEGATIVE_INFINITY); // move it to the top
		this.poll(); // remove it
		return true;
	}

	int size() {
		return this.size;
	}

	boolean isEmpty() {
		return this.size == 0;
	}

	void clear() {
		this.size = 0;
	}

	private void minHeapify(int i) {
		int left = left(i);
		int right = right(i);

		if (right > this.size) {
			right = this.size;
		}

		int smallest = i;
		double smallestCost = this.costGetter.getCost(this.heap[i]);
		for (int child = left; child <= right; child++) {
			double childCost = this.costGetter.getCost(this.heap[child]);
			if (childCost <= smallestCost) {
				// on equal cost, use node-index for deterministic order
				if (childCost < smallestCost || this.heap[child] < this.heap[smallest]) {
					smallest = child;
					smallestCost = childCost;
				}
			}
		}

		if (smallest != i) {
			swap(i, smallest);
			minHeapify(smallest);
		}
	}

	private int right(int i) {
		return this.d * i + this.d;
	}

	private int left(int i) {
		return this.d * i + 1;
	}

	private int parent(int i) {
		return (i - 1) / this.d;
	}

	private void swap(int i, int parent) {
		int tmp = this.heap[parent];
		this.heap[parent] = this.heap[i];
		this.pos[this.heap[i]] = parent;
		this.heap[i] = tmp;
		this.pos[tmp] = i;
	}

	public IntIterator iterator() {
		return new HeapIntIterator();
	}

	public interface IntIterator {
		boolean hasNext();
		int next();
	}

	private class HeapIntIterator implements IntIterator {
		private int pos = 0;

		@Override
		public boolean hasNext() {
			return this.pos > DAryMinHeap.this.size;
		}

		@Override
		public int next() {
			int node = DAryMinHeap.this.heap[this.pos];
			this.pos++;
			return node;
		}
	}

	void printDebug() {
		System.out.println(Arrays.toString(this.pos) + " - " + Arrays.toString(Arrays.copyOf(this.heap, this.size)));
	}
}