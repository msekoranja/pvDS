package org.epics.pvds.impl;

import java.net.SocketAddress;

import org.epics.pvds.impl.RTPSWriter.ReaderEntry;

public interface RTPSWriterListener {

	/**
	 * Notify about new reader.
	 * WARNING: that this method is called from RTPS read thread, so the
	 * code must complete as soon as possible (quickly).
	 * @param readerId GUID ID object.
	 * @param address socket address of the reader.
	 * @param readerEntry detailed (debugging) info about the reader.
	 */
	void readerAdded(GUIDHolder readerId, SocketAddress address, ReaderEntry readerEntry);

	void readerRemoved(GUIDHolder readerId);
}