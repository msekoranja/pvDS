package org.epics.pvds.impl.test;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.impl.QoS;
import org.epics.pvds.impl.RTPSParticipant;
import org.epics.pvds.impl.RTPSReader;
import org.epics.pvds.impl.RTPSReader.SharedBuffer;
import org.epics.pvds.impl.RTPSWriter;

public class RTPSParticipantTest extends TestCase {

	public void testConstruction()
	{
		try
		{
			new RTPSParticipant(null, -1, true);
			fail("negative domainId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}
		
		try
		{
			new RTPSParticipant(null, Protocol.MAX_DOMAIN_ID + 1, true);
			fail("out-of-range domainId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}

		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, true);
				RTPSParticipant p2 = new RTPSParticipant(null, 0, true)) {
			assertNotNull(p1.getGUIDPrefix());
			assertNotNull(p2.getGUIDPrefix());
			assertSame(p1.getGUIDPrefix(), p1.getGUIDPrefix());
			assertSame(p2.getGUIDPrefix(), p2.getGUIDPrefix());
			assertFalse(p1.getGUIDPrefix().equals(p2.getGUIDPrefix()));
		}

	}

	public void testDuplicateEntityIdCreation() {
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, false)) {
			GUID guid = new GUID(new GUIDPrefix(), new EntityId(0));
			p1.createReader(0, guid, 16, 1);
			p1.createReader(1, guid, 16, 1);
			
			// allowed, since id is w/o entityKind mask
			p1.createWriter(0, 16, 1);
			
			try {
				p1.createReader(1, guid, 16, 1);
				fail("duplicate readerId allowed");
			} catch (RuntimeException rte) {
				// OK
			}
			
			try {
				p1.createWriter(0, 16, 1);
				fail("duplicate writerId allowed");
			} catch (RuntimeException rte) {
				// OK
			}
		}
	}


	public void testReaderOnWriterOnlyParticipant() {
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, true)) {
			GUID guid = new GUID(new GUIDPrefix(), new EntityId(0));
			try {
				p1.createReader(0, guid, 16, 1);
				fail("reader created on writer-only participant");
			} catch (IllegalStateException ise) {
				// OK
			}
		}
	}

	public void testClose() {
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, false)) {
			p1.start();
			
			p1.createWriter(0, 16, 1);
			p1.createWriter(1, 16, 1);
			p1.createWriter(2, 16, 1);
			GUID guid = new GUID(new GUIDPrefix(), new EntityId(0));
			p1.createReader(3, guid, 16, 1);
			p1.createReader(4, guid, 16, 1);
			p1.createReader(5, new GUID(new GUIDPrefix(), new EntityId(1)), 16, 1);
		}
	}
	
	public void testBasicCommuniction() throws InterruptedException
	{
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, true)) {
			
			readerParticipant.start();
			writerParticipant.start();
			
			final int MESSAGE_SIZE = Long.BYTES;
			final int QUEUE_SIZE = 3;
			final int TIMEOUT_MS = 1000;
			
			RTPSWriter writer = writerParticipant.createWriter(
					0, MESSAGE_SIZE, QUEUE_SIZE,
					new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND }, null);
			writer.start();
			
			RTPSReader reader = readerParticipant.createReader(0, writer.getGUID(), MESSAGE_SIZE, QUEUE_SIZE);

			ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
			
			for (int i = 0; i < QUEUE_SIZE; i++)
			{
				writeBuffer.clear();
				writeBuffer.putLong(i);
				writeBuffer.flip();
				
				long seqNo = writer.send(writeBuffer);
				assertTrue(seqNo != 0);
			}

			for (int i = 0; i < QUEUE_SIZE; i++)
			{
				try (SharedBuffer sharedBuffer = reader.waitForNewData(TIMEOUT_MS))
				{
					assertNotNull(sharedBuffer);
					assertEquals(i, sharedBuffer.getBuffer().getLong());
				}
			}
		}
		
	}
}
