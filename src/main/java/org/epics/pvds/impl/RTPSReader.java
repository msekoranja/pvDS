package org.epics.pvds.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.Protocol.SequenceNumberSet;
import org.epics.pvds.Protocol.SubmessageHeader;

/**
 * RTPS message receiver implementation.
 * The class itself is not thread-safe, i.e. processSubMessage() method should be called from only one thread. 
 * @author msekoranja
 */
public class RTPSReader
{
	protected final MessageReceiver receiver;
    protected final MessageReceiverStatistics stats;

    // TODO
    protected final DatagramChannel discoveryUnicastChannel;

    /**
     * This instance reader ID.
     */
    private final int readerId;

    /**
     * Remote (data source) writer ID.
     */
    private final int writerId;
		
	    // non-synced list of free buffers
	    private final ArrayDeque<FragmentationBufferEntry> freeFragmentationBuffers;
	    
	    // seqNo -> fragmentation buffer mapping, ordered by seqNo
	    // TODO consider using "xyz<long, FragmenatinBufferEntry>" alternative, with preallocated size
	    private final TreeMap<Long, FragmentationBufferEntry> activeFragmentationBuffers;

	    // TODO consider using "xyz<long, SharedBuffer>" alternative, with preallocated size
	    // TODO consider using TreeSet
	    private final TreeMap<Long, SharedBuffer> completedBuffers;
	
	    // TODO consider disruptor
	    private final ArrayBlockingQueue<SharedBuffer> newDataQueue;

	    /**
	     * Constructor.
	     * Total allocated buffer size = messageQueueSize * maxMessageSize;
	     * @param readerId reader ID.
	     * @param writerId remote (data source) writer ID.
	     * @param maxMessageSize maximum message size.
	     * @param messageQueueSize message queue size (number of slots).
	     */
	    public RTPSReader(RTPSParticipant processor,
	    		int readerId, int writerId, int maxMessageSize, int messageQueueSize) {
			
	    	this.receiver = processor.getReceiver();
	    	this.stats = processor.getStatistics();
	    	this.discoveryUnicastChannel = processor.getDiscoveryUnicastChannel();
			this.readerId = readerId;
			this.writerId = writerId;
			
			freeFragmentationBuffers = new ArrayDeque<FragmentationBufferEntry>(messageQueueSize);
		    activeFragmentationBuffers = new TreeMap<Long, FragmentationBufferEntry>();
		    completedBuffers = new TreeMap<Long, SharedBuffer>(); 
		    
		    newDataQueue = new ArrayBlockingQueue<SharedBuffer>(messageQueueSize);
			
		    ByteBuffer buffer = ByteBuffer.allocate(messageQueueSize*maxMessageSize);
			
		    synchronized (freeFragmentationBuffers) {
			    int pos = 0;
			    for (int i = 0; i < messageQueueSize; i++)
			    {
			    	buffer.position(pos);
			    	pos += maxMessageSize;
			    	buffer.limit(pos);

			    	freeFragmentationBuffers.addLast(new FragmentationBufferEntry(buffer.slice()));
			    }
			}
		    
		    // TODO use logging
		    System.out.println("Receiver: fragmentation buffer size = " + messageQueueSize + " packets of " + maxMessageSize + " bytes (max payload size)");

		}
	    
	    // used by addAckNackSubmessage method only
	    private int ackNackCounter = 0;

	    // send it:
	    //    - when some packets are missing at the reader side and are available at the writer side
	    //    - no new data available received, same lastSN, different heartbeatCount
	    //      and there are some missing packets that are available
	    //    - every N messages (N = 100?)
	    //	  - immediately when heartbeat with final flag is received
	    // TODO try to piggyback
	    protected void addAckNackSubmessage(ByteBuffer buffer, SequenceNumberSet readerSNState)
	    {
	    	// big endian flag
	    	Protocol.addSubmessageHeader(buffer, SubmessageHeader.RTPS_ACKNACK, (byte)0x00, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;
		    
		    // readerId
		    buffer.putInt(readerId);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // SequenceNumberSet readerSNState
		    readerSNState.serialize(buffer);
		    
		    // count
		    buffer.putInt(ackNackCounter++);

		    // set message size
		    int octetsToNextHeader = buffer.position() - octetsToNextHeaderPos - 2;
		    buffer.putShort(octetsToNextHeaderPos, (short)(octetsToNextHeader & 0xFFFF));
	    }

	    // "pvMSpvMS"
	    private static final long FREE_MARK = 0x70764D5370764D53L;

	    private FragmentationBufferEntry getFragmentationBufferEntry(long firstSegmentSeqNo, int dataSize, int fragmentSize)
	    {
	    	FragmentationBufferEntry entry = activeFragmentationBuffers.get(firstSegmentSeqNo);
	    	if (entry != null)
	    		return entry;
	    	
		    // out-of-order or duplicate (late) fragments will recreate buffer
	    	if (completedBuffers.containsKey(firstSegmentSeqNo));
	    	// TODO !!!
	    		
	    	
	    	// take next
	    	// TODO !!! take free first, then oldest non-free and mark message as lost; now we are just cycling
	    	synchronized (freeFragmentationBuffers) {
		    	entry = freeFragmentationBuffers.pollLast();
		    	if (entry != null)
		    	{
		    		// sanity check
		    		if (entry.seqNo != 0)
			    		throw new AssertionError(entry.seqNo != 0);
		    		
		    		entry.reset(firstSegmentSeqNo, dataSize, fragmentSize);
		    	}
			}
	    	
	    	if (entry == null)
	    	{
	    		// if QoS.RELIABLE
	    		return null;
	    		// TODO non-RELIABLE !!!
	    		// else
	    		/*
	    		if (completedBuffers.isEmpty())
	    			return null;
	    		
	    		// ignore missing
	    		long nextSN = completedBuffers.firstKey();
	    		stats.ignoredSN += nextSN - nextExpectedSequenceNumber;
	    		nextExpectedSequenceNumber = nextSN;
				updateMinAvailableSeqNo(nextExpectedSequenceNumber, true);

	    		processNextExpectedSequenceNumbers(true);

	    		synchronized (freeFragmentationBuffers) {
			    	entry = freeFragmentationBuffers.pollLast();
	    			entry.reset(firstSegmentSeqNo, dataSize, fragmentSize);
				}
				*/
	    	}
	    	
	    	activeFragmentationBuffers.put(firstSegmentSeqNo, entry);
	    	
	    	return entry;
	    }

	    public interface NoExceptionCloseable extends AutoCloseable {
	    	void close();
	    }
	    
	    public interface SharedBuffer extends NoExceptionCloseable {
	    	public ByteBuffer getBuffer();
	    }
	    
	    private class SharedByteBuffer implements SharedBuffer
	    {
	    	private final ByteBuffer buffer;
	    	
	    	public SharedByteBuffer(ByteBuffer buffer)
	    	{
	    		this.buffer = buffer;
	    	}

			@Override
			public ByteBuffer getBuffer() {
				return buffer;
			}

			@Override
			public void close() {
				// noop
			}
	    }
	    
    	static final int calculateFragmentCount(int dataSize, int fragmentSize)
    	{
	    	return dataSize / fragmentSize + (((dataSize % fragmentSize) != 0) ? 1 : 0);
    	}

    	private class FragmentationBufferEntry implements SharedBuffer {
	    	final ByteBuffer buffer;
	    	long seqNo = 0;	// 0 means unused, reserved; synced on freeFramentationBuffers
	    	int fragmentSize;
	    	int fragments;
	    	int fragmentsReceived;
	    	
	    	FragmentationBufferEntry(ByteBuffer buffer) {
	    		this.buffer = buffer;
	    	}
	    	
	    	
	    	void reset(long seqNo, int dataSize, int fragmentSize)
	    	{
		    	if (dataSize > buffer.capacity())
		    		throw new RuntimeException("dataSize > buffer.capacity()");	// TODO different exception
//System.out.println(seqNo + " acquire, # of free buffers left:" + freeFragmentationBuffers.size());
		    	this.seqNo = seqNo;
		    	this.fragmentSize = fragmentSize;
		    	this.fragments = calculateFragmentCount(dataSize, fragmentSize);
	    		this.fragmentsReceived = 0;

	    		buffer.limit(dataSize);
	    		
		    	// buffer initialization
	    		// mark start of each fragment as free
		    	int pos = 0;
		    	while (pos < dataSize)
		    	{
		    		buffer.putLong(pos, FREE_MARK);
		    		pos += fragmentSize;
		    	}
	    		
	    	}
	    	
	    	boolean addFragment(int fragmentStartingNum, ByteBuffer fragmentData)
	    	{
	    		if (fragmentStartingNum > fragments)
	    			throw new IndexOutOfBoundsException("fragmentStartingNum > fragments"); // TODO log!!!
//	    			return false;
	    			
		    	int bufferPosition = (fragmentStartingNum - 1) * fragmentSize;		// starts from 1 on...
		    	if (buffer.getLong(bufferPosition) != FREE_MARK)
		    		return false;			// duplicate fragment received
		    	
		    	// copy all data to buffer @ bufferPosition
		    	// and increment received fragment count
		    	buffer.position(bufferPosition);
		    	buffer.put(fragmentData);
		    	fragmentsReceived++;
		    	
		    	// all fragments received?
		    	if (fragmentsReceived == fragments)
		    	{
		    		buffer.position(0);
		    		
		    		return true;
		    	}
		    	else
		    		return false;
	    	}
	    	
	    	// TODO can be called from other thread, more than once
	    	void release()
	    	{
//System.out.println(seqNo + " release");
	    		synchronized (freeFragmentationBuffers) {
	    			if (seqNo == 0) return;
					seqNo = 0;
	    			freeFragmentationBuffers.addLast(this);
				}
	    	}
	    	
	    	// called to release obsolete fragment (message that will never be completed)
	    	void release(Iterator<FragmentationBufferEntry> iterator)
	    	{
//System.out.println(seqNo + " release via iter");
	    		iterator.remove();
	    		synchronized (freeFragmentationBuffers) {
		    		seqNo = 0;
	    			freeFragmentationBuffers.addLast(this);
				}
	    	}

	    	@Override
	    	public ByteBuffer getBuffer()
	    	{
	    		return buffer;
	    	}

			@Override
			public void close() {
				release();
			}
	    	
	    }
	    
	    private int lastHeartbeatCount = -1;
	    
	    private final SequenceNumberSet readerSNState = new SequenceNumberSet();
	    
	    // max seqNo received
	    private long maxReceivedSequenceNumber = 0;
	    
	    // max seqNo known to exist (includes transmitter side)
	    private long lastKnownSequenceNumber = 0;
	    
	    // sn < ignoreSequenceNumberPrior are ignored
	    private long ignoreSequenceNumbersPrior = 0;

	    // last (maximum) seqNo that was passed to the client
	    private long lastAcceptedSequenceNumber = 0;
	    private long nextExpectedSequenceNumber = 0;
	    
	    
	    // TODO find better (array based) BST, or at least TreeSet<long>
	    private final TreeSet<Long> missingSequenceNumbers = new TreeSet<Long>();
	    
	    private long lastHeartbeatLastSN = 0;
	    
	    //private static final int ACKNACK_MISSING_COUNT_THRESHOLD = 64;
	    private long lastAckNackTimestamp = Long.MIN_VALUE;
	    
	    // TODO initialize (message header only once), TODO calculate max size (72?)
	    private final ByteBuffer ackNackBuffer = ByteBuffer.allocate(128);
	    private boolean sendAckNackResponse(long timestamp)
	    {
	    	if (missingSequenceNumbers.isEmpty())
	    	{
	    		// we ACK all sequence numbers until maxReceivedSequenceNumber, or ignoreSequenceNumbersPrior
		    	readerSNState.reset(Math.max(maxReceivedSequenceNumber + 1, ignoreSequenceNumbersPrior));
	    	}
	    	else
	    	{
	    		long first = missingSequenceNumbers.first();
		    	readerSNState.reset(first);
		    	for (Long sn : missingSequenceNumbers)
		    	{
		    		if (sn - first >= 255)			// TODO constant
		    		{
		    			// TODO imagine this case 100, 405...500
		    			// in this case readerSNState can report only 100!!!
//System.out.println("sn (" + sn + ") - first (" + first + ") >= 255 ("+ (sn - first) + ")");	
		    			// send NACK only message
		    			break;
		    		}
		    		
		    		readerSNState.set(sn); 
		    	}
		    	// TODO what if we have more !!!
	    	}
	    	
	    	ackNackBuffer.clear();
	    	Protocol.addMessageHeader(ackNackBuffer);
	    	addAckNackSubmessage(ackNackBuffer, readerSNState);
	    	ackNackBuffer.flip();

	    	// TODO
		    try {
				discoveryUnicastChannel.send(ackNackBuffer, receiver.receivedFrom);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		    
	    	lastAckNackTimestamp = timestamp;
	    	
	    	return true;
	    }
	    
	    private boolean sendAckNackResponse()
	    {
	    	return sendAckNackResponse(System.currentTimeMillis());
		}	

	    private void checkAckNackCondition()
	    {
	    	// TODO !!!if (System.currentTimeMillis() - lastAckNackTimestamp > 3)
	    		sendAckNackResponse();
	    }
	    
	    
	    void processDataSubMessage(int octetsToInlineQos, ByteBuffer buffer)
	    {
			long seqNo = buffer.getLong();

			stats.receivedSN++;
			//System.out.println("rx: " + seqNo);
			
			if (seqNo < ignoreSequenceNumbersPrior)
			{
				stats.ignoredSN++;
				return;
			}
			else if (lastHeartbeatLastSN == 0)
			{
				// RTPS_HEARTBEAT message must be received first or ignore (seqNo obsolete for this receiver) 
				// NOTE: we accept lastHeartbeatLastSN == 0 && seqNo == 1 not to skip first seqNo
				// i.e. when receiver is started before transmitter
				if (seqNo == 1)
				{
					lastHeartbeatLastSN = 1; // == seqNo;
				}
				else
				{
					stats.ignoredSN++;
					return;
				}
			}

			if (seqNo > maxReceivedSequenceNumber)
			{
				// mark [max(ignoreSequenceNumbersPrior, maxReceivedSequenceNumber + 1), seqNo - 1] as missing
				long newMissingSN = 0;
				for (long sn = Math.max(ignoreSequenceNumbersPrior, maxReceivedSequenceNumber + 1); sn < seqNo; sn++)
					if (missingSequenceNumbers.add(sn))
						newMissingSN++;
				
				if (newMissingSN > 0)
				{
					stats.missedSN += newMissingSN;
					// notify about first missing seqNo immediately
					if (missingSequenceNumbers.size() == newMissingSN)
						sendAckNackResponse();
					else
						checkAckNackCondition();
				}
				
				maxReceivedSequenceNumber = seqNo;
			}
			
			if (seqNo > lastKnownSequenceNumber)
				lastKnownSequenceNumber = seqNo;
			else
			{
				// missingSequenceNumbers can only contains SN <= lastKnownSequenceNumber
				// might be a missing SN, remove received seqNo
				boolean missed = missingSequenceNumbers.remove(seqNo);
				if (missed)
					stats.recoveredSN++;
			}
			
			boolean isData = (receiver.submessageId == SubmessageHeader.RTPS_DATA);
			if (isData)
			{
				// jump to inline InlineQoS (or Data)
				buffer.position(buffer.position() + octetsToInlineQos - 16);	// 16 = 4+4+8
				
				// InlineQoS present
				boolean flagQ = (receiver.submessageFlags & 0x02) == 0x02;
				// Data present
				boolean flagD = (receiver.submessageFlags & 0x04) == 0x04;
				
				/*
				// Key present
				boolean flagK = (receiver.submessageFlags & 0x08) == 0x08;
				*/
				
				if (flagQ)
				{
					// ParameterList inlineQos
					// TODO
				}
				
				// TODO resolve appropriate reader
				// and decode data there
				
				// flagD and flagK are exclusive
				if (flagD)
				{
					// Data

					/*
					int serializedDataLength = 
						submessageDataStartPosition + receiver.submessageSize - buffer.position();
					*/
					
					// TODO out-of-order QoS
					// do not report newData if order QoS is set
					// wait or throw away older messages

					// TODO same as for fragmented
					if (true) throw new RuntimeException("not yet implemented");
					
					// TODO ok for non-ordered policy
					lastAcceptedSequenceNumber = Math.max(lastAcceptedSequenceNumber, seqNo);
					ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, lastAcceptedSequenceNumber+1);
					updateMinAvailableSeqNo(ignoreSequenceNumbersPrior, true);
				
					// TODO byteBuffer pool
					ByteBuffer bufferCopy = ByteBuffer.allocate(buffer.capacity());	// not to fragment
					buffer.put(bufferCopy);
					newDataNotify(new SharedByteBuffer(bufferCopy));
				}
				/*
				else if (flagK)
				{
					// Key
				}
				*/
				
			}
			else		// DataFrag
			{
			    // fragmentStartingNum (unsigned integer, starting from 1)
				// TODO revise: unsigned overflow !!!
				int fragmentStartingNum = buffer.getInt();

				// fragmentsInSubmessage (unsigned short)
				int fragmentsInSubmessage = buffer.getShort() & 0xFFFF;

			    // fragmentSize (unsigned short)
				int fragmentSize = buffer.getShort() & 0xFFFF;
			    
			    // sampleSize or dataSize (unsigned integer)
				int dataSize = buffer.getInt();
				
				// jump to inline InlineQoS (or Data)
				buffer.position(buffer.position() + octetsToInlineQos - 16);	// 16 = 4+4+8

				// calculate fist fragment seqNo; we might not receive first fragment as first
				long firstFragmentSeqNo = (seqNo - fragmentStartingNum + 1);
				
				if (firstFragmentSeqNo < ignoreSequenceNumbersPrior)
				{
					stats.ignoredSN++;
					
					// this implies all the fragments can be ignored, raise ignoreSequenceNumbersPrior if needed
					long lastFragmentSeqNoPlusOne = firstFragmentSeqNo + calculateFragmentCount(dataSize, fragmentSize);
					ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, lastFragmentSeqNoPlusOne);
					
					// ignore older fragments
					updateMinAvailableSeqNo(ignoreSequenceNumbersPrior, false);
					
					// remove fragmentation buffer, if already allocated
					FragmentationBufferEntry entry = activeFragmentationBuffers.remove(firstFragmentSeqNo);
					if (entry != null)
					{
						//System.out.println(firstFragmentSeqNo + " passed");
						entry.release();
					}
					
					// notify about ignoreSequenceNumbersPrior
					sendAckNackResponse();
				}
				else
				{
					FragmentationBufferEntry entry = getFragmentationBufferEntry(firstFragmentSeqNo, dataSize, fragmentSize);
					if (entry != null)
					{
						for (int i = 0; i < fragmentsInSubmessage; i++)
						{
							if (entry.addFragment(fragmentStartingNum, buffer))
							{
								// all fragments received

								// remove from active fragmentation buffers map
								activeFragmentationBuffers.remove(firstFragmentSeqNo);
System.out.println(firstFragmentSeqNo + " completed");

								
								// TODO out-of-order QoS
								// do not report newData if order QoS is set
								// wait or throw away older messages
								

								// first completed fragment case
								if (nextExpectedSequenceNumber == 0)
								{
									// set this as first sequenceNo
									nextExpectedSequenceNumber = firstFragmentSeqNo;
									
									// discard all the previous (older) fragments
									updateMinAvailableSeqNo(firstFragmentSeqNo, true);
								}

								// is this next? (completed fragment in order)
								if (firstFragmentSeqNo == nextExpectedSequenceNumber)
								{
									lastAcceptedSequenceNumber = nextExpectedSequenceNumber;
									nextExpectedSequenceNumber = firstFragmentSeqNo + entry.fragments;
									newDataNotify(entry);

									processNextExpectedSequenceNumbers(false);
								}
								else
								{
System.out.println(firstFragmentSeqNo + " put on completedBuffers");
									// put in completed buffers
									completedBuffers.put(firstFragmentSeqNo, entry);
								}
								
								ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, nextExpectedSequenceNumber);

								// TODO ignoreSequenceNumbersPrior update acknack?
								
								/** ordered, best-effort
								lastAcceptedSequenceNumber = Math.max(lastAcceptedSequenceNumber, seqNo);
								ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, lastAcceptedSequenceNumber+1);
								updateMinAvailableSeqNo(ignoreSequenceNumbersPrior, true);
								*/
								
								break;
							}
							fragmentStartingNum++;
						}
					}
					else
					{
						// no free buffers
						stats.noBuffers++;
						
						// treat seqNo as missed packet
						
						if (seqNo == maxReceivedSequenceNumber)
							maxReceivedSequenceNumber--;
						
						missingSequenceNumbers.add(seqNo);
						// notify about first missing seqNo immediately
						if (missingSequenceNumbers.size() == 1)
							sendAckNackResponse();
						else
							checkAckNackCondition();
					}
				}						
			}
	    	
	    }

	    void processHeartbeatSubMessage(ByteBuffer buffer)
	    {
			long firstSN = buffer.getLong();
			long lastSN = buffer.getLong();
			
			int count = buffer.getInt();
			
			if (firstSN <= 0 || lastSN <= 0 || lastSN < firstSN)
			{
				stats.invalidMessage++;
				return;
			}
			
			// NOTE: warp concise comparison
			if (count - lastHeartbeatCount > 0)
			{
				lastHeartbeatCount = count;
			
				// TODO remove
				//System.out.println("HEARTBEAT: " + firstSN + " -> " + lastSN + " | " + count);
				
				if (lastSN > lastKnownSequenceNumber)
					lastKnownSequenceNumber = lastSN;
				
				long newMissingSN = 0;
				
				boolean sendAckNack = false;

				if (maxReceivedSequenceNumber == 0)
				{
					// at start we accept only fresh (seqNo >= lastSN) sequences 
					ignoreSequenceNumbersPrior = lastSN + 1;
					
					// do not send ackNack here, first received non-fragmented message will ACK it,
					// or ackNack will be sent when ignoreSequenceNumbersPrior will be adjusted for fragmented message
				}
				else
				{
					// add new available (from firstSN on) missed sequence numbers
					for (long sn = Math.max(ignoreSequenceNumbersPrior, Math.max(maxReceivedSequenceNumber + 1, firstSN)); sn <= lastSN; sn++)
						if (missingSequenceNumbers.add(sn))
							newMissingSN++;
					stats.missedSN += newMissingSN;

					// remove obsolete (not available anymore) sequence numbers
					long minAvailableSN = Math.max(firstSN, ignoreSequenceNumbersPrior);
					updateMinAvailableSeqNo(minAvailableSN, true);
					if (firstSN > ignoreSequenceNumbersPrior)
					{
						ignoreSequenceNumbersPrior = firstSN;
						sendAckNack = true;
					}
				}

				// FinalFlag flag (require response)
				boolean flagF = (receiver.submessageFlags & 0x02) == 0x02;
				if (flagF || sendAckNack)
					sendAckNackResponse();
				// first missing seqNo
				else if (newMissingSN > 0 && missingSequenceNumbers.size() == newMissingSN)
					sendAckNackResponse();
				// repetetive HB (no new data)
				else if (lastHeartbeatLastSN == lastSN)
					checkAckNackCondition();
				else if (newMissingSN > 0)
					checkAckNackCondition();

				// LivelinessFlag
				//boolean flagL = (receiver.submessageFlags & 0x04) == 0x04;

				lastHeartbeatLastSN = lastSN;

				// TODO remove
				//System.out.println("\t" + missingSequenceNumbers);
				//System.out.println("\tmissed   : " + stats.missedSN);
				//System.out.println("\treceived : " + stats.receivedSN + " (" + (100*stats.receivedSN/(stats.receivedSN+stats.missedSN))+ "%)");
				//System.out.println("\tlost     : " + stats.lostSN);
				//System.out.println("\trecovered: " + stats.recoveredSN);
			}
	    }

	    
		private void processCompletedSequenceNumbers(long minAvailableSN)
		{
			while (!completedBuffers.isEmpty())
			{
				Entry<Long, SharedBuffer> entry = completedBuffers.firstEntry();
				SharedBuffer sb = entry.getValue();
				if (sb instanceof FragmentationBufferEntry)
				{
					FragmentationBufferEntry fbe = ((FragmentationBufferEntry)sb);
					if (minAvailableSN >= fbe.seqNo)
					{
System.out.println("missed some messages, promoting the following fragment: " + fbe.seqNo);						
						completedBuffers.pollFirstEntry();
						nextExpectedSequenceNumber = fbe.seqNo + fbe.fragments;
						lastAcceptedSequenceNumber = fbe.seqNo;
						missedSequencesNotify();
						newDataNotify(sb);
					}
					else
					{
						// completedBuffers is ordered, therefore no other buffer would match
						break;
					}
				}
				else
				{
					long seqNo = entry.getKey();
					if (minAvailableSN >= seqNo)
					{
System.out.println("missed some messages, promoting the following unfragmented message: " + seqNo);						
						completedBuffers.pollFirstEntry();
						nextExpectedSequenceNumber = seqNo + 1;
						lastAcceptedSequenceNumber = seqNo;
						missedSequencesNotify();
						newDataNotify(sb);
					}
					else
					{
						// completedBuffers is ordered, therefore no other buffer would match
						break;
					}
				}
				
			}
		}

		private void processNextExpectedSequenceNumbers(boolean updateIgnoreSN)
		{
			while (!completedBuffers.isEmpty() &&
					completedBuffers.firstKey() == nextExpectedSequenceNumber)
			{
				lastAcceptedSequenceNumber = nextExpectedSequenceNumber;

				SharedBuffer sb = completedBuffers.pollFirstEntry().getValue();
				if (sb instanceof FragmentationBufferEntry)
				{
					nextExpectedSequenceNumber += ((FragmentationBufferEntry)sb).fragments;
				}
				else
				{
					nextExpectedSequenceNumber++;
				}
				
				newDataNotify(sb);
			}
			
			if (updateIgnoreSN)
			{
				ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, nextExpectedSequenceNumber);
			}	
		}

		private long updateMinAvailableSeqNo(long minAvailableSN, boolean checkObsoleteFragments) {
			
			if (checkObsoleteFragments)
			{
				// cancel fragments that will never be completed
				// FragmentationBufferEntry.{seqNo + fragments} <= minAvailableSN
				Iterator<FragmentationBufferEntry> fragmentIterator =
						activeFragmentationBuffers.values().iterator();
				while (fragmentIterator.hasNext())
				{
					FragmentationBufferEntry fragmentEntry = fragmentIterator.next();
					if ((fragmentEntry.seqNo + fragmentEntry.fragments) <= minAvailableSN)
					{
						missedSequencesNotify();
						fragmentEntry.release(fragmentIterator);
					}
					else
						// no need to check all, entries are ordered
						break;
				}
			
				// release completed fragments that where held (due to ordering)
				processCompletedSequenceNumbers(minAvailableSN);
			}
			
			// remove ones that are not available anymore
			long lostSNCount = 0;
			while (!missingSequenceNumbers.isEmpty() && missingSequenceNumbers.first() < minAvailableSN)
			{
				/*long lostSN = */missingSequenceNumbers.pollFirst();
				lostSNCount++;
			}
			stats.lostSN += lostSNCount;
			
			return lostSNCount;
		}
	    
		
		
		
		
	    private void newDataNotify(SharedBuffer buffer)
	    {
	    	// TODO do we really need to send after every completed?
	    	sendAckNackResponse();
	    	
	    	if (!newDataQueue.offer(buffer))
	    	{
	    		// TODO !!! should not happen
				buffer.close();
	    		System.out.println("buffer lost");
	    		System.err.println("********************************");
	    	}
	    }
	    
	    public SharedBuffer waitForNewData(long timeout) throws InterruptedException
	    {
	    	return newDataQueue.poll(timeout, TimeUnit.MILLISECONDS);
	    }
	    
		// TODO
		private void missedSequencesNotify()
		{
			System.out.println("missedSequencesNotify");
		}

	    public GUID getGUID() {
	    	return new GUID(GUIDPrefix.GUIDPREFIX, new EntityId(readerId));
	    }
	}