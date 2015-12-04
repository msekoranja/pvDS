package org.epics.pvds.impl.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.impl.QoS;
import org.epics.pvds.impl.QoS.ReaderQOS;
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
			new RTPSParticipant(null, -1, 0, true);
			fail("negative domainId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}
		
		try
		{
			new RTPSParticipant(null, Protocol.MAX_DOMAIN_ID + 1, 0, true);
			fail("out-of-range domainId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}

		try
		{
			new RTPSParticipant(null, 0, -1, true);
			fail("negative groupId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}
		
		try
		{
			new RTPSParticipant(null, 0, Protocol.MAX_GROUP_ID + 1, true);
			fail("out-of-range groupId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}

		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, 0, true);
				RTPSParticipant p2 = new RTPSParticipant(null, 0, 0, true)) {
			assertNotNull(p1.getGUIDPrefix());
			assertNotNull(p2.getGUIDPrefix());
			assertSame(p1.getGUIDPrefix(), p1.getGUIDPrefix());
			assertSame(p2.getGUIDPrefix(), p2.getGUIDPrefix());
			assertFalse(p1.getGUIDPrefix().equals(p2.getGUIDPrefix()));
		}

	}

	public void testDuplicateEntityIdCreation() {
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, 0, false)) {
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
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, 0, true)) {
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
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, 0, false)) {
			p1.createWriter(0, 16, 1);
			p1.createWriter(1, 16, 1);
			p1.createWriter(2, 16, 1);
			GUID guid = new GUID(new GUIDPrefix(), new EntityId(0));
			p1.createReader(3, guid, 16, 1);
			p1.createReader(4, guid, 16, 1);
			p1.createReader(5, new GUID(new GUIDPrefix(), new EntityId(1)), 16, 1);
		}
	}
	
	private void lossLessCommunication(ReaderQOS[] readerQoS) throws InterruptedException
	{
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, 0, true)) {
			
			final int MESSAGE_SIZE = Long.BYTES;
			final int MESSAGES = 100;
			final int TIMEOUT_MS = 1000;
			final int UNRELIABLE_TIMEOUT_MS = 10;
			
			for (int readerQueueSize = 1; readerQueueSize <= MESSAGES; readerQueueSize++)
			{
				try (RTPSWriter writer = writerParticipant.createWriter(
						readerQueueSize, MESSAGE_SIZE, MESSAGES,
						new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND }, null);
	
					RTPSReader reader = readerParticipant.createReader(
							readerQueueSize, writer.getGUID(), 
							MESSAGE_SIZE, readerQueueSize,
							readerQoS, null))
				{
					ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
					
					for (int i = 0; i < MESSAGES; i++)
					{
						writeBuffer.clear();
						writeBuffer.putLong(i);
						writeBuffer.flip();
						
						long seqNo = writer.write(writeBuffer);
						assertTrue(seqNo != 0);
					}
		
					// NOTE: by ref compare, OK for this test
					if (readerQoS == QoS.RELIABLE_ORDERED_QOS)
					{
						for (int i = 0; i < MESSAGES; i++)
						{
							try (SharedBuffer sharedBuffer = reader.read(TIMEOUT_MS, TimeUnit.MILLISECONDS))
							{
								assertNotNull(sharedBuffer);
								assertEquals(i, sharedBuffer.getBuffer().getLong());
							}
						}
					}
					else if (readerQoS == QoS.UNRELIABLE_ORDERED_QOS)
					{
						// duplicate, order test
						long lastValue = Long.MIN_VALUE;
						Set<Long> valueSet = new HashSet<Long>(MESSAGES, 1);
						while (true)
						{
							try (SharedBuffer sharedBuffer = reader.read(UNRELIABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
							{
								if (sharedBuffer == null)
									break;
								long value = sharedBuffer.getBuffer().getLong();
								assertTrue(value > lastValue);
								lastValue = value;

								assertFalse(valueSet.contains(value));
								valueSet.add(value);
							}
						}
					}
					else if (readerQoS == QoS.UNRELIABLE_UNORDERED_QOS)
					{
						// duplicate test
						Set<Long> valueSet = new HashSet<Long>(MESSAGES, 1);
						while (true)
						{
							try (SharedBuffer sharedBuffer = reader.read(UNRELIABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
							{
								if (sharedBuffer == null)
									break;
								long value = sharedBuffer.getBuffer().getLong();

								assertFalse(valueSet.contains(value));
								valueSet.add(value);
							}
						}
					}
					else
						throw new RuntimeException("unsupported ReaderQoS[]");
				}
			}
		}
	}

	public void testLossLessReliableOrderedCommunication() throws InterruptedException
	{
		lossLessCommunication(QoS.RELIABLE_ORDERED_QOS);
	}

	public void testLossLessUnReliableOrderedCommunication() throws InterruptedException
	{
		lossLessCommunication(QoS.UNRELIABLE_ORDERED_QOS);
	}

	public void testLossLessUnReliableUnOrderedCommunication() throws InterruptedException
	{
		lossLessCommunication(QoS.UNRELIABLE_UNORDERED_QOS);
	}

	private void lossyCommunication(
			int id,
			RTPSParticipant readerParticipant, RTPSParticipant writerParticipant,
			SeqNoFilter filter, int queueSize, ReaderQOS[] readerQoS) throws InterruptedException
	{
			final int MESSAGE_SIZE = Long.BYTES;
			final int TIMEOUT_MS = 1000;
			final int UNRELIABLE_TIMEOUT_MS = 10;
			
			for (int readerQueueSize = 1; readerQueueSize <= queueSize; readerQueueSize++)
			{
				try (
					RTPSWriter writer = writerParticipant.createWriter(
							readerQueueSize*1000000 + id, MESSAGE_SIZE, queueSize,
							new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND, new QoS.QOS_SEND_SEQNO_FILTER(filter) }, null);
					RTPSReader reader = readerParticipant.createReader(
							readerQueueSize*1000000 + id, writer.getGUID(), 
							MESSAGE_SIZE, readerQueueSize,
							readerQoS, null))
				{
						
				ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
				
				for (int i = 0; i < queueSize; i++)
				{
					writeBuffer.clear();
					writeBuffer.putLong(i);
					writeBuffer.flip();
					
					long seqNo = writer.write(writeBuffer);
					assertTrue(seqNo != 0);
				}
	
				// NOTE: by ref compare, OK for this test
				if (readerQoS == QoS.RELIABLE_ORDERED_QOS)
				{
					for (int i = 0; i < queueSize; i++)
					{
						try (SharedBuffer sharedBuffer = reader.read(TIMEOUT_MS, TimeUnit.MILLISECONDS))
						{
							assertNotNull(sharedBuffer);
							assertEquals(i, sharedBuffer.getBuffer().getLong());
						}
					}
				}
				else if (readerQoS == QoS.UNRELIABLE_ORDERED_QOS)
				{
					// duplicate, order test
					long lastValue = Long.MIN_VALUE;
					Set<Long> valueSet = new HashSet<Long>(queueSize, 1);
					while (true)
					{
						try (SharedBuffer sharedBuffer = reader.read(UNRELIABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
						{
							if (sharedBuffer == null)
								break;
							long value = sharedBuffer.getBuffer().getLong();
							assertTrue(value > lastValue);
							lastValue = value;

							assertFalse(valueSet.contains(value));
							valueSet.add(value);
						}
					}
				}
				else if (readerQoS == QoS.UNRELIABLE_UNORDERED_QOS)
				{
					// duplicate test
					Set<Long> valueSet = new HashSet<Long>(queueSize, 1);
					while (true)
					{
						try (SharedBuffer sharedBuffer = reader.read(UNRELIABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
						{
							if (sharedBuffer == null)
								break;
							long value = sharedBuffer.getBuffer().getLong();

							assertFalse(valueSet.contains(value));
							valueSet.add(value);
						}
					}
				}
				else
					throw new RuntimeException("unsupported ReaderQoS[]");
			}
		}
	}

	private void lossyFragmentedCommunication(
			int id,
			RTPSParticipant readerParticipant, RTPSParticipant writerParticipant,
			SeqNoFilter filter, int queueSize, ReaderQOS[] readerQoS) throws InterruptedException
	{
			final int MESSAGE_SIZE = 3*Long.BYTES;
			final int TIMEOUT_MS = 1000;
			final int UNRELIABLE_TIMEOUT_MS = 10;

			for (int readerQueueSize = 1; readerQueueSize <= queueSize; readerQueueSize++)
			{
				try (
					RTPSWriter writer = writerParticipant.createWriter(
							readerQueueSize*1000000 + id, MESSAGE_SIZE, queueSize,
							new QoS.WriterQOS[] { QoS.QOS_ALWAYS_SEND, new QoS.QOS_SEND_SEQNO_FILTER(filter) }, null);
					RTPSReader reader = readerParticipant.createReader(
							readerQueueSize*1000000 + id, writer.getGUID(), 
							MESSAGE_SIZE, readerQueueSize,
							readerQoS, null))
				{
						
				ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
				
				for (int i = 0; i < queueSize; i++)
				{
					writeBuffer.clear();
					writeBuffer.putLong(i);
					writeBuffer.putLong(i);
					writeBuffer.putLong(i);
					writeBuffer.flip();
					
					long seqNo = writer.write(writeBuffer);
					assertTrue(seqNo != 0);
				}
	
				// NOTE: by ref compare, OK for this test
				if (readerQoS == QoS.RELIABLE_ORDERED_QOS)
				{
					for (int i = 0; i < queueSize; i++)
					{
						try (SharedBuffer sharedBuffer = reader.read(TIMEOUT_MS, TimeUnit.MILLISECONDS))
						{
							assertNotNull(sharedBuffer);
							assertEquals(i, sharedBuffer.getBuffer().getLong());
							assertEquals(i, sharedBuffer.getBuffer().getLong());
							assertEquals(i, sharedBuffer.getBuffer().getLong());
						}
					}
				}
				else if (readerQoS == QoS.UNRELIABLE_ORDERED_QOS)
				{
					// duplicate, order test
					long lastValue = Long.MIN_VALUE;
					Set<Long> valueSet = new HashSet<Long>(queueSize, 1);
					while (true)
					{
						try (SharedBuffer sharedBuffer = reader.read(UNRELIABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
						{
							if (sharedBuffer == null)
								break;
							long value = sharedBuffer.getBuffer().getLong();
							assertEquals(value, sharedBuffer.getBuffer().getLong());
							assertEquals(value, sharedBuffer.getBuffer().getLong());
							assertTrue(value > lastValue);
							lastValue = value;

							assertFalse(valueSet.contains(value));
							valueSet.add(value);
						}
					}
				}
				else if (readerQoS == QoS.UNRELIABLE_UNORDERED_QOS)
				{
					// duplicate test
					Set<Long> valueSet = new HashSet<Long>(queueSize, 1);
					while (true)
					{
						try (SharedBuffer sharedBuffer = reader.read(UNRELIABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
						{
							if (sharedBuffer == null)
								break;
							long value = sharedBuffer.getBuffer().getLong();
							assertEquals(value, sharedBuffer.getBuffer().getLong());
							assertEquals(value, sharedBuffer.getBuffer().getLong());

							assertFalse(valueSet.contains(value));
							valueSet.add(value);
						}
					}
				}
				else
					throw new RuntimeException("unsupported ReaderQoS[]");
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
	
	private void lossyCommunicationCombinations(ReaderQOS[] readerQoS, int queueSize) throws InterruptedException {
		
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, 0, true)) {
			
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
				lossyCommunication(setNumber, readerParticipant, writerParticipant, new SeqNoFilterImpl(set), queueSize, readerQoS);
			}
		}
	}

	private static final int TEST_SPEED_UP = 1;
	
	public void testLossyReliableOrderedCommunication() throws InterruptedException {
		lossyCommunicationCombinations(QoS.RELIABLE_ORDERED_QOS, 11-TEST_SPEED_UP);
	}
		
	public void testLossyUnReliableOrderedCommunication() throws InterruptedException {
		lossyCommunicationCombinations(QoS.UNRELIABLE_ORDERED_QOS, 8-TEST_SPEED_UP);
	}

	public void testLossyUnReliableUnOrderedCommunication() throws InterruptedException {
		lossyCommunicationCombinations(QoS.UNRELIABLE_UNORDERED_QOS, 8-TEST_SPEED_UP);
	}

	private void fragmentedLossyCommunicationCombinations(ReaderQOS[] readerQoS, int queueSize) throws InterruptedException {

		System.getProperties().put("PVDS_MAX_UDP_PACKET_SIZE", "64");
		
		try (RTPSParticipant readerParticipant = new RTPSParticipant(null, 0, 0, false);
			 RTPSParticipant writerParticipant = new RTPSParticipant(null, 0, 0, true)) {
			
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
				lossyFragmentedCommunication(setNumber, readerParticipant, writerParticipant, new SeqNoFilterImpl(set), queueSize, readerQoS);
			}
		}
		finally 
		{
			System.getProperties().remove("PVDS_MAX_UDP_PACKET_SIZE");
		}
	}

	public void testFragmentedLossyReliableOrderedCommunication() throws InterruptedException {
		fragmentedLossyCommunicationCombinations(QoS.RELIABLE_ORDERED_QOS, 5);
	}
		
	public void testFragmentedLossyUnReliableOrderedCommunication() throws InterruptedException {
		fragmentedLossyCommunicationCombinations(QoS.UNRELIABLE_ORDERED_QOS, 4);
	}

	public void testFragmentedLossyUnReliableUnOrderedCommunication() throws InterruptedException {
		fragmentedLossyCommunicationCombinations(QoS.UNRELIABLE_UNORDERED_QOS, 4);
	}

}
