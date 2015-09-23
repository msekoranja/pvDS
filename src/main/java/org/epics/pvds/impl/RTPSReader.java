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

    protected final DatagramChannel unicastChannel;

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

	    // map of completed buffers held to preserve order, yet to be given to the client
	    // TODO consider using "xyz<long, SharedBuffer>" alternative, with preallocated size
	    // TODO consider using TreeSet
	    private final TreeMap<Long, SharedBuffer> completedBuffers;
	
	    // TODO consider using "xyz<long>" alternative, with preallocated size
	    private final TreeSet<Long> completedSeqNo;

	    private final ArrayBlockingQueue<SharedBuffer> newDataQueue;

	    private final int maxMessageSize;
	    private final int messageQueueSize;

	    ///
	    /// QoS
	    ///
	    private final static boolean RELIABLE_DEFAULT = false;
	    private final boolean reliable;

	    private final static boolean ORDERED_DEFAULT = false;
	    private final boolean ordered;
	    
	    private final RTPSReaderListener listener;
	    
	    /**
	     * Constructor.
	     * Total allocated buffer size = messageQueueSize * maxMessageSize;
	     * @param readerId reader ID.
	     * @param writerId remote (data source) writer ID.
	     * @param maxMessageSize maximum message size.
	     * @param messageQueueSize message queue size (number of slots).
	     * @param qos QoS array list, can be <code>null</code>.
	     * @param listener this instance listener, can be <code>null</code>.
	     */
	    public RTPSReader(RTPSParticipant participant,
	    		int readerId, int writerId,
	    		int maxMessageSize, int messageQueueSize,
	    		QoS.ReaderQOS[] qos,
	    		RTPSReaderListener listener)
	    {
			
	    	this.receiver = participant.getReceiver();
	    	this.stats = participant.getStatistics();
	    	this.unicastChannel = participant.getUnicastChannel();
			this.readerId = readerId;
			this.writerId = writerId;
			this.listener = listener;
			
			if (maxMessageSize <= 0)
				throw new IllegalArgumentException("maxMessageSize <= 0");
			this.maxMessageSize = maxMessageSize;
			
			if (messageQueueSize <= 0)
				throw new IllegalArgumentException("messageQueueSize <= 0");
			this.messageQueueSize = messageQueueSize;
			
			///
			/// QoS
			/// 
			
		    boolean reliable = RELIABLE_DEFAULT;
		    boolean ordered = ORDERED_DEFAULT;
			if (qos != null)
			{
				for (QoS.ReaderQOS rq : qos)
				{
					if (rq == QoS.QOS_RELIABLE)
					{
						reliable = true;
					}
					else if (rq == QoS.QOS_ORDERED)
					{
						ordered = true;
					}
					else 
					{
						// TODO log
						System.out.println("Unsupported reader QoS: " + rq);
					}
				}
			}
			this.reliable = reliable;
			this.ordered = ordered;
			
			///
			/// allocate and initialize buffer(s)
			///
			// TODO opt: prellocated ADTs, do not allocate if not used (depends on QOS)
			freeFragmentationBuffers = new ArrayDeque<FragmentationBufferEntry>(messageQueueSize);
		    activeFragmentationBuffers = new TreeMap<Long, FragmentationBufferEntry>(); // messageQueueSize
		    completedBuffers = new TreeMap<Long, SharedBuffer>(); // messageQueueSize
		    completedSeqNo = new TreeSet<Long>(); // messageQueueSize + 1
		    
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

	    	// NOTE: it is important to have/save space for nextExpectedSequenceNumber message for QOS_RELIABLE

	    	
	    	// take next
	    	synchronized (freeFragmentationBuffers) {
	    		
    			// QOS_ORDERED + QOS_RELIABLE: save space for nextExpectedSequenceNumber message check (negated)
	    		// we need QOS_RELIABLE here to avoid getting out of buffers
	    		if (!reliable || /* !ordered || - nextExpectedSequenceNumber handles this */
	    			nextExpectedSequenceNumber == 0 ||
	    			freeFragmentationBuffers.size() != 1 ||
	    			activeFragmentationBuffers.containsKey(nextExpectedSequenceNumber))
	    		{
			    	entry = freeFragmentationBuffers.pollLast();
			    	if (entry != null)
			    	{
			    		// sanity check
			    		if (entry.seqNo != 0)
				    		throw new AssertionError(entry.seqNo != 0);
			    		
			    		entry.reset(firstSegmentSeqNo, dataSize, fragmentSize);
			    	}
	    		}
			}
	    	
	    	if (entry == null)
	    	{
	    		// QOS_RELIABLE
	    		// do not drop anything, and we have one buffer slot for nextExpectedSequenceNumber reserved
	    		if (reliable)
	    			return null;
	    		
	    		// !QOS_RELIABLE

	    		// there is no nextExpectedSequenceNumber reservation here (not reliable)
	    		
	    		if (ordered)
	    		{
		    		// QOS_ORDERED

		    		if (!completedBuffers.isEmpty())
		    		{
		    			// promote completed, ignore (drop) older 
			    		long nextSN = completedBuffers.firstKey();
			    		stats.ignoredSN += nextSN - nextExpectedSequenceNumber;
			    		nextExpectedSequenceNumber = nextSN;
						updateMinAvailableSeqNo(nextExpectedSequenceNumber, true);

			    		processNextExpectedSequenceNumbers(true);
			    		entry = pollFreeFragmenrationBuffer(firstSegmentSeqNo, dataSize, fragmentSize);
		    		}
		    		else
		    		{
		    			// note: completedBuffer is empty

		    			// no buffer free, drop packet
		    			if (activeFragmentationBuffers.size() == 0)
		    				return null;
		    			
		    			nextExpectedSequenceNumber = dropOldestFragment();
						ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, nextExpectedSequenceNumber);

						entry = pollFreeFragmenrationBuffer(firstSegmentSeqNo, dataSize, fragmentSize);
		    		}
	    		}
	    		else
	    		{
	    			// !QOS_ORDERED
	    			
	    			// no buffer free, drop packet
	    			if (activeFragmentationBuffers.size() == 0)
	    				return null;
	    			
	    			dropOldestFragment();
		    		entry = pollFreeFragmenrationBuffer(firstSegmentSeqNo, dataSize, fragmentSize);
	    		}
	    	}
	    	
	    	if (entry != null)
	    		activeFragmentationBuffers.put(firstSegmentSeqNo, entry);
	    	
	    	return entry;
	    }

		private FragmentationBufferEntry pollFreeFragmenrationBuffer(
				long firstSegmentSeqNo, int dataSize, int fragmentSize)
				throws AssertionError {
			FragmentationBufferEntry entry;
			synchronized (freeFragmentationBuffers) {
				entry = freeFragmentationBuffers.pollLast();

				// can be null in case of no free buffers (e.g. !QOS_RELIABLE && QOS_ORDERED)
				if (entry == null)
					return null;
				// sanity check
				else if (entry.seqNo != 0)
					throw new AssertionError(entry.seqNo != 0);

				entry.reset(firstSegmentSeqNo, dataSize, fragmentSize);
			}
			return entry;
		}

	    public interface NoExceptionCloseable extends AutoCloseable {
	    	void close();
	    }
	    
	    public interface SharedBuffer extends NoExceptionCloseable {
	    	public ByteBuffer getBuffer();
	    }
	    /*
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
	    */
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
	    //private long lastAcceptedSequenceNumber = 0;
	    private long nextExpectedSequenceNumber = 0;
	    
	    
	    // TODO find better (array based) BST, or at least TreeSet<long>
	    private final TreeSet<Long> missingSequenceNumbers = new TreeSet<Long>();
	    
	    private long lastHeartbeatLastSN = 0;
	    
	    //private static final int ACKNACK_MISSING_COUNT_THRESHOLD = 64;
	    //private long lastAckNackTimestamp = Long.MIN_VALUE;
	    
	    // TODO initialize (message header only once), TODO calculate max size (72?)
	    private final ByteBuffer ackNackBuffer = ByteBuffer.allocate(128);
	    private boolean sendAckNackResponse(long timestamp)
	    {
	    	if (!reliable)
	    	{
	    		// ack all seqNo
	    		// TODO possible opt (set this only once)
		    	readerSNState.reset(Long.MAX_VALUE);
	    	}
	    	else if (missingSequenceNumbers.isEmpty())
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
		    		if (sn - first >= SequenceNumberSet.MAX_RANGE)
		    		{
		    			// TODO imagine this case 100, 405...500 (range limit)
		    			// in this case readerSNState can report only 100!!!
//System.out.println("sn (" + sn + ") - first (" + first + ") >= 255 ("+ (sn - first) + ")");	
		    			// send NACK only message
		    			break;
		    		}
		    		
		    		readerSNState.set(sn); 
		    	}
		    	// TODO optimize: what if we have more !!!
	    	}
//System.out.println("sending ACKNACK: " + readerSNState.bitmapBase + ", " + readerSNState.bitmap);

	    	ackNackBuffer.clear();
	    	Protocol.addMessageHeader(ackNackBuffer);
	    	addAckNackSubmessage(ackNackBuffer, readerSNState);
	    	ackNackBuffer.flip();

		    try {
				unicastChannel.send(ackNackBuffer, receiver.receivedFrom);
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		    
	    	//lastAckNackTimestamp = timestamp;
	    	
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
	    
	    
	    void processDataSubMessage(int submessageDataStartPosition, int octetsToInlineQos, ByteBuffer buffer)
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
				if (reliable)
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
				}
				
				maxReceivedSequenceNumber = seqNo;
			}
			
			if (seqNo > lastKnownSequenceNumber)
				lastKnownSequenceNumber = seqNo;
			else
			{
				if (reliable)
				{
					// missingSequenceNumbers can only contains SN <= lastKnownSequenceNumber
					// might be a missing SN, remove received seqNo
					boolean missed = missingSequenceNumbers.remove(seqNo);
					if (missed)
						stats.recoveredSN++;
				}
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
					// TODO not supported (not used)
				}
				
				// flagD and flagK are exclusive
				if (flagD)
				{
					// Data

					int serializedDataLength = 
						submessageDataStartPosition + receiver.submessageSize - buffer.position();
					
					// TODO non-fragment messages could be optimized 
					processDataBuffer(buffer, seqNo, 1, 
							1, serializedDataLength, serializedDataLength);						
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
				// TODO NOTE: this limits us to Integer.MAX_INTEGER (unsigned overflow)
				int fragmentStartingNum = buffer.getInt();

				// fragmentsInSubmessage (unsigned short)
				int fragmentsInSubmessage = buffer.getShort() & 0xFFFF;

			    // fragmentSize (unsigned short)
				int fragmentSize = buffer.getShort() & 0xFFFF;
			    
			    // sampleSize or dataSize (unsigned integer)
				int dataSize = buffer.getInt();
				
				// jump to inline InlineQoS (or Data)
				buffer.position(buffer.position() + octetsToInlineQos - 16);	// 16 = 4+4+8

				processDataBuffer(buffer, seqNo, fragmentStartingNum,
						fragmentsInSubmessage, fragmentSize, dataSize);						
			}
	    	
	    }

		private void processDataBuffer(ByteBuffer buffer, long seqNo,
				int fragmentStartingNum, int fragmentsInSubmessage,
				int fragmentSize, int dataSize) {
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
				
		    	// too large fragment (does not fit our fragments buffers), ignore it
		    	if (dataSize > maxMessageSize)
		    	{
		    		stats.fragmentTooLarge++;
		    		return;
		    	}
		    	
			    // out-of-order and/or duplicate (late) fragments can recreate buffer
		    	// QOS_ORDERED
		    	if (ordered && completedBuffers.containsKey(firstFragmentSeqNo))
		    		return;

		    	/*
		    	// possible optimization
		    	// we make an assumption on this message is similar to all other messages
		    	if (nextExpectedSequenceNumber != 0)
		    	{
		    		// this should be guared by ignoreSequenceNumbersPrior condition
		    		//if (firstSegmentSeqNo < nextExpectedSequenceNumber);
		    			;
		    		
		    		long messagesAdvance = (firstSegmentSeqNo - nextExpectedSequenceNumber) / fragments;
		    		if (messagesAdvance > messageQueueSize - (isExpectedFragmentActive ? 0 : 1))
		    			return null;		// first wait (accept) older messages
		    	}
		    	*/
		    	
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
							
							// !QOS_ORDERED
							if (!ordered)
							{
								if (reliable)
								{
									// remove duplicates
									if (!completedSeqNo.add(firstFragmentSeqNo))
									{
										entry.release();
										return;
									}
									
									// remove too old
									if (completedSeqNo.size() > messageQueueSize)
										completedSeqNo.remove(completedSeqNo.first());
								}
								
								newDataNotify(entry);
								return;
							}
							
							// do not report newData if order QoS is set immediately
							// check if ordering is OK

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
								//lastAcceptedSequenceNumber = nextExpectedSequenceNumber;
								nextExpectedSequenceNumber = firstFragmentSeqNo + entry.fragments;
								newDataNotify(entry);

								processNextExpectedSequenceNumbers(false);
							}
							else
							{
//System.out.println(firstFragmentSeqNo + " put on completedBuffers");
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
					
					if (reliable)
					{
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
					// unless there is no data (or first message) in remote buffers
					sendAckNack = (firstSN == lastSN);
				}
				else
				{
					if (reliable)
					{
						// add new available (from firstSN on) missed sequence numbers
						for (long sn = Math.max(ignoreSequenceNumbersPrior, Math.max(maxReceivedSequenceNumber + 1, firstSN)); sn <= lastSN; sn++)
							if (missingSequenceNumbers.add(sn))
								newMissingSN++;
						stats.missedSN += newMissingSN;
					}
					
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

				// TODO log
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
//System.out.println("missed some messages, promoting the following fragment: " + fbe.seqNo);						
						completedBuffers.pollFirstEntry();
						nextExpectedSequenceNumber = fbe.seqNo + fbe.fragments;
						//lastAcceptedSequenceNumber = fbe.seqNo;
						missedSequencesNotify(fbe.seqNo, nextExpectedSequenceNumber - 1);
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
//System.out.println("missed some messages, promoting the following unfragmented message: " + seqNo);						
						completedBuffers.pollFirstEntry();
						nextExpectedSequenceNumber = seqNo + 1;
						//lastAcceptedSequenceNumber = seqNo;
						missedSequencesNotify(seqNo, seqNo);
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
				//lastAcceptedSequenceNumber = nextExpectedSequenceNumber;

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

		// NOTE: pre-condition: activeFragmentationBuffers.size() > 0
		private long dropOldestFragment() {
			FragmentationBufferEntry fragmentEntry = activeFragmentationBuffers.remove(activeFragmentationBuffers.firstKey());
			long nextSeq = fragmentEntry.seqNo + fragmentEntry.fragments;
			missedSequencesNotify(fragmentEntry.seqNo, nextSeq - 1);
			fragmentEntry.release();
			return nextSeq;
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
					long nextSeqNo = fragmentEntry.seqNo + fragmentEntry.fragments; 
					if (nextSeqNo <= minAvailableSN)
					{
						missedSequencesNotify(fragmentEntry.seqNo, nextSeqNo - 1);
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
				buffer.close();
				// this should never happen, 
				// since number of buffers equals number of newDataQueue size
				throw new AssertionError("buffer lost");
	    	}
	    }
	    
	    public SharedBuffer waitForNewData(long timeout) throws InterruptedException
	    {
	    	return newDataQueue.poll(timeout, TimeUnit.MILLISECONDS);
	    }
	    
	    private void missedSequencesNotify(long start, long end)
		{
			if (listener != null)
			{
	    		try {
					missedSequencesNotify(start, end);
	    		} catch (Throwable th) {
	    			// TODO log
	    			th.printStackTrace();
	    		}
			}
		}

	    public GUID getGUID() {
	    	return new GUID(GUIDPrefix.GUIDPREFIX, new EntityId(readerId));
	    }
	}