package org.epics.pvds.util;

/**
 * A heap-based priority queue (min-heap) of <code>long</code> elements.
 * The class currently uses a standard array-based
 * heap, as described in, for example, Sedgewick's Algorithms text.
 * In addition every element manages its position. This way you can combine it
 * with a map to enable efficient look-ups, and consequently this class
 * allows efficient remove and increment operations.
 **/
public class LongDynaHeap {

	public static class HeapMapElement {
		private long value;
		private int pos;
		
		public long getValue() {
			return value;
		}
	}
	
	/**
	 * The tree nodes, packed into an array.
	 */
	protected HeapMapElement[] elements;

	/**
	 * Number of used slots.
	 */
	protected int count = 0;

	/**
	 * Create a Heap with the given initial capacity.
	 * 
	 * @param capacity initial capacity.
	 * @exception IllegalArgumentException if capacity less or equal to zero.
	 **/
	public LongDynaHeap(int capacity) {
		
		if (capacity <= 0)
			throw new IllegalArgumentException();
		
		elements = new HeapMapElement[capacity];
		for (int i = 0; i < capacity; i++)
			elements[i] = new HeapMapElement();
	}

	/**
	 * Get parent index.
	 */
	protected static final int parent(int k) {
		return (k - 1) / 2;
	}

	/**
	 * Get left child.
	 */
	protected static final int left(int k) {
		return 2 * k + 1;
	}

	/**
	 * Get right child.
	 */
	protected static final int right(int k) {
		return 2 * (k + 1);
	}

	/**
	 * Insert an element, resize if necessary.
	 * @param x an element to insert.
	 **/
	public HeapMapElement insert(long x) {
		if (count >= elements.length) {
			int newcap = 3 * elements.length / 2 + 1;
			HeapMapElement[] newnodes = new HeapMapElement[newcap];
			// positions are preserved
			System.arraycopy(elements, 0, newnodes, 0, elements.length);
			for (int i = elements.length; i < newcap; i++)
				newnodes[i] = new HeapMapElement();
			elements = newnodes;
		}

		// take last element and shift-up
		int k = count;
		HeapMapElement newElement = elements[k];
		newElement.value = x;
		++count;
		while (k > 0) {
			int par = parent(k);
			if (x < elements[par].value) {
				elements[par].pos = k;
				elements[k] = elements[par];
				k = par;
			} else
				break;
		}
		newElement.pos = k;
		elements[k] = newElement;
		
		return newElement;
	}

	/**
	 * Extracts (removes) min element.
	 **/
	public HeapMapElement extract() {
		HeapMapElement min = peek();
		remove(min);
		return min;
	}
	
	/**
	 * Remove an element.
	 **/
	public void remove(HeapMapElement element) {
		// swap last element with the element to remove
		--count;
		HeapMapElement last = elements[count];
		int k = element.pos;
		last.pos = k;
		elements[k] = last;
		
		element.pos = count;
		elements[count] = element;
		
		// and shift-down
		shiftDown(k);
	}

	private final void shiftDown(int k) {
		HeapMapElement element = elements[k];
		long x = element.value;
		for (;;) {
			int l = left(k);
			if (l >= count)
				break;
			else {
				int r = right(k);
				int child = (r >= count || elements[l].value < elements[r].value) ? l : r;
				if (x > elements[child].value) {
					elements[child].pos = k;
					elements[k] = elements[child];
					k = child;
				} else
					break;
			}
		}
		element.pos = k;
		elements[k] = element;
	}

	/**
	 * Increment a value of existing element to a new value.
	 * @param element existing element.
	 * @param value new value of an element.
	 * @return min element.
	 * @exception IndexOutOfBoundsException if heap is empty.
	 **/
	public HeapMapElement increment(HeapMapElement element, long value) {
		element.value = value;
		shiftDown(element.pos);
		return elements[0];
	}
	
	/**
	 * Return least element without removing it.
	 * @return least element.
	 * @exception IndexOutOfBoundsException if heap is empty.
	 **/
	public HeapMapElement peek() {
		if (count > 0)
			return elements[0];
		else
			throw new IndexOutOfBoundsException("empty heap");
	}

	/**
	 * Return number of elements.
	 * @return number of elements.
	 **/
	public int size() {
		return count;
	}

	/**
	 * Remove all elements.
	 **/
	public void clear() {
		count = 0;
	}
	
}
