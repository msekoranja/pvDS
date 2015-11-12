package org.epics.pvds.impl.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.impl.QoS;
import org.epics.pvds.impl.QoS.QOS_SEND_SEQNO_FILTER.SeqNoFilter;
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
			p1.createWriter(0, 16, 1);
			p1.createWriter(1, 16, 1);
			p1.createWriter(2, 16, 1);
			GUID guid = new GUID(new GUIDPrefix(), new EntityId(0));
			p1.createReader(3, guid, 16, 1);
			p1.createReader(4, guid, 16, 1);
			p1.createReader(5, new GUID(new GUIDPrefix(), new EntityId(1)), 16, 1);
		}
	}
	
	public void testLossLessReliableOrderedCommunication() throws InterruptedException
	{
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, true)) {
			
			final int MESSAGE_SIZE = Long.BYTES;
			final int QUEUE_SIZE = 3;
			final int TIMEOUT_MS = 1000;
			
			RTPSWriter writer = writerParticipant.createWriter(
					0, MESSAGE_SIZE, QUEUE_SIZE,
					new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND }, null);
			
			RTPSReader reader = readerParticipant.createReader(
					0, writer.getGUID(), 
					MESSAGE_SIZE, QUEUE_SIZE,
					QoS.RELIABLE_ORDERED_QOS, null);

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

	/*
	private void lossyReliableOrderedCommunication(SeqNoFilter filter, int queueSize) throws InterruptedException
	{
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, true)) {
			
			readerParticipant.start();
			writerParticipant.start();
			
			final int MESSAGE_SIZE = Long.BYTES;
			final int TIMEOUT_MS = 1000;
			
			RTPSWriter writer = writerParticipant.createWriter(
					0, MESSAGE_SIZE, queueSize,
					new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND, new QoS.QOS_SEND_SEQNO_FILTER(filter) }, null);
			
			RTPSReader reader = readerParticipant.createReader(
					0, writer.getGUID(), 
					MESSAGE_SIZE, queueSize,
					QoS.RELIABLE_ORDERED_QOS, null);

			ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
			
			for (int i = 0; i < queueSize; i++)
			{
				writeBuffer.clear();
				writeBuffer.putLong(i);
				writeBuffer.flip();
				
				long seqNo = writer.send(writeBuffer);
				assertTrue(seqNo != 0);
			}

			for (int i = 0; i < queueSize; i++)
			{
				try (SharedBuffer sharedBuffer = reader.waitForNewData(TIMEOUT_MS))
				{
					System.out.println("checking for: " + i);
					assertNotNull(sharedBuffer);
					assertEquals(i, sharedBuffer.getBuffer().getLong());
				}
			}
		}
	}
	*/

	private void lossyReliableOrderedCommunication(
			int id,
			RTPSParticipant readerParticipant, RTPSParticipant writerParticipant,
			SeqNoFilter filter, int queueSize) throws InterruptedException
	{
			final int MESSAGE_SIZE = Long.BYTES;
			final int TIMEOUT_MS = 1000;
			
			try (
				RTPSWriter writer = writerParticipant.createWriter(
						id, MESSAGE_SIZE, queueSize,
						new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND, new QoS.QOS_SEND_SEQNO_FILTER(filter) }, null);
				RTPSReader reader = readerParticipant.createReader(
						id, writer.getGUID(), 
						MESSAGE_SIZE, queueSize,
						QoS.RELIABLE_ORDERED_QOS, null))
			{
					
			ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
			
			for (int i = 0; i < queueSize; i++)
			{
				writeBuffer.clear();
				writeBuffer.putLong(i);
				writeBuffer.flip();
				
				long seqNo = writer.send(writeBuffer);
				assertTrue(seqNo != 0);
			}

			for (int i = 0; i < queueSize; i++)
			{
				try (SharedBuffer sharedBuffer = reader.waitForNewData(TIMEOUT_MS))
				{
					assertNotNull(sharedBuffer);
					assertEquals(i, sharedBuffer.getBuffer().getLong());
				}
			}
		}
	}

	private void lossyFragmentedReliableOrderedCommunication(
			int id,
			RTPSParticipant readerParticipant, RTPSParticipant writerParticipant,
			SeqNoFilter filter, int queueSize) throws InterruptedException
	{
			final int MESSAGE_SIZE = 3*Long.BYTES;
			final int TIMEOUT_MS = 1000;

			try (
				RTPSWriter writer = writerParticipant.createWriter(
						id, MESSAGE_SIZE, queueSize,
						new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND, new QoS.QOS_SEND_SEQNO_FILTER(filter) }, null);
				RTPSReader reader = readerParticipant.createReader(
						id, writer.getGUID(), 
						MESSAGE_SIZE, queueSize,
						QoS.RELIABLE_ORDERED_QOS, null))
			{
					
			ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
			
			for (int i = 0; i < queueSize; i++)
			{
				writeBuffer.clear();
				writeBuffer.putLong(i);
				writeBuffer.putLong(i);
				writeBuffer.putLong(i);
				writeBuffer.flip();
				
				long seqNo = writer.send(writeBuffer);
				assertTrue(seqNo != 0);
			}

			for (int i = 0; i < queueSize; i++)
			{
				try (SharedBuffer sharedBuffer = reader.waitForNewData(TIMEOUT_MS*1000))
				{
					assertNotNull(sharedBuffer);
					final ByteBuffer buf = sharedBuffer.getBuffer();
					assertEquals(i, buf.getLong());
					assertEquals(i, buf.getLong());
					assertEquals(i, buf.getLong());
				}
			}
		}
		
	}

	private static class SeqNoFilterImpl implements SeqNoFilter
	{
		private final Set<Long> filterOutSet;
		
		public SeqNoFilterImpl(Set<Long> set) {
			filterOutSet = set;
		}
		
		@Override
		public boolean checkSeqNo(long seqNo) {
			return !filterOutSet.remove(seqNo);
		}
		
	}
	
	public void testLossyReliableOrderedCommunication() throws InterruptedException {
		
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, true)) {
			
			final int queueSize = 11;

			// test all combinations of missing packets
	
			List<Long> seqNumbers = new ArrayList<Long>(queueSize);
			// do not miss first seqNo == 2 - to have consistent subscription
			for (long i = 0; i < queueSize-1; i++)
				seqNumbers.add(i + 3);
	
			int n = seqNumbers.size();
			long combinations = 1 << n;
			for (int setNumber = 0; setNumber < combinations; setNumber++)
			{
				Set<Long> set = new HashSet<Long>(queueSize);
				for (int digit = 0; digit < n; digit++)
				{
					if ((setNumber & (1 << digit)) > 0)
						set.add(seqNumbers.get(digit));
				}
				lossyReliableOrderedCommunication(setNumber, readerParticipant, writerParticipant, new SeqNoFilterImpl(set), queueSize);
			}
		}
	}

	public void testFragmentedLossyReliableOrderedCommunication() throws InterruptedException {

		System.getProperties().put("PVDS_MAX_UDP_PACKET_SIZE", "64");
		
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, true)) {
			
			final int queueSize = 5;

			// test all combinations of missing packets
			// we expect 3 packets for a message
			List<Long> seqNumbers = new ArrayList<Long>(queueSize*3);
			// do not miss first message seqNo == 2,3,4 - to have consistent subscription
			for (long i = 1*3; i < (queueSize*3)-1; i++)
				seqNumbers.add(i + 2);
	
			int n = seqNumbers.size();
			long combinations = 1 << n;
			for (int setNumber = 0; setNumber < combinations; setNumber++)
			{
				Set<Long> set = new HashSet<Long>(queueSize);
				for (int digit = 0; digit < n; digit++)
				{
					if ((setNumber & (1 << digit)) > 0)
						set.add(seqNumbers.get(digit));
				}
				lossyFragmentedReliableOrderedCommunication(setNumber, readerParticipant, writerParticipant, new SeqNoFilterImpl(set), queueSize);
			}
		}
		finally 
		{
			System.getProperties().remove("PVDS_MAX_UDP_PACKET_SIZE");
		}
		
	}
}
