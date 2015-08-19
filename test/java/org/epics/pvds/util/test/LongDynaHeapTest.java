package org.epics.pvds.util.test;

import java.util.ArrayList;
import java.util.List;

import org.epics.pvds.util.LongDynaHeap;
import org.epics.pvds.util.LongDynaHeap.HeapMapElement;

import junit.framework.TestCase;

public class LongDynaHeapTest extends TestCase {

	public void testBuild() {
		LongDynaHeap lhm = new LongDynaHeap(10);
		
		final long SIZE = 100;
		// increment order
		for (long i = 0; i < SIZE; i++)
			assertNotNull(lhm.insert(i));
		assertEquals(SIZE, lhm.size());
		
		for (long i = 0; i < SIZE; i++)
			assertEquals(i, lhm.extract().getValue());
		assertEquals(0, lhm.size());

		// decrement order
		for (long i = SIZE-1; i >= 0; i--)
			assertNotNull(lhm.insert(i));
		assertEquals(SIZE, lhm.size());
		
		for (long i = 0; i < SIZE; i++)
			assertEquals(i, lhm.extract().getValue());
		assertEquals(0, lhm.size());
	}

	public void testIncrementValueAndRemove() {
		final long SIZE = 100;
		LongDynaHeap lhm = new LongDynaHeap(100);

		// slow for lookups, but OK for test - allows duplicates
		List<Long> set = new ArrayList<Long>((int)SIZE+5);

		// decrement order
		for (long i = SIZE-1; i >= 0; i--)
		{
			assertNotNull(lhm.insert(i*2));
			set.add(i*2);
		}
		assertEquals(SIZE, lhm.size());

		HeapMapElement element = lhm.insert(51);
		lhm.increment(element, SIZE*2);
		set.add(SIZE*2);
		
		element = lhm.insert(1);
		lhm.increment(element, 13);
		set.add((long)13);

		element = lhm.insert(11);
		lhm.increment(element, 40);
		set.add((long)40);
		
		element = lhm.insert(SIZE*3);
		lhm.increment(element, SIZE*4);
		set.add(SIZE*4);

		element = lhm.insert(21);
		lhm.increment(element, 22);
		set.add((long)22);

		element = lhm.insert(20);
		lhm.increment(element, 50);
		set.add((long)50);
		
		element = lhm.insert(80);
		lhm.remove(element);

		element = lhm.insert(12);
		lhm.remove(element);

		element = lhm.insert(44);
		lhm.remove(element);

		// check consistency
		long last = -1;
		while (lhm.size() > 0)
		{
			element = lhm.extract();
			assertTrue(element.getValue() >= last);
			assertTrue(set.remove(element.getValue()));
			last = element.getValue();
		}
		assertEquals(0, set.size());
	}

}
