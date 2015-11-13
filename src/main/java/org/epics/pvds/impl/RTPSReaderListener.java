package org.epics.pvds.impl;

public interface RTPSReaderListener {
	
	/**
	 * Notify about missed sequences (packets).
	 * WARNING: that this method is called from RTPS read thread, so the
	 * code must complete as soon as possible (quickly).
	 * @param start first missed sequence (start of the interval, included).
	 * @param end last missed sequence (end of the interval, included).
	 */
	void missedSequencesNotify(long start, long end);
	
	/**
	 * Notify about presence of a writer.
	 */
	void writerPresent();
	
	/**
	 * Notify about absence of a writer.
	 */
	void writerAbsent();
}