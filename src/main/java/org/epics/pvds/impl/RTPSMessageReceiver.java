package org.epics.pvds.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.epics.pvdata.pv.PVField;
import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.ProtocolVersion;
import org.epics.pvds.Protocol.SequenceNumberSet;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.Protocol.VendorId;
import org.epics.pvds.util.CityHash64;
import org.epics.pvds.util.LongDynaHeap;

/**
 * RTPS message receiver implementation.
 * The class itself is not thread-safe, i.e. processMessage() method should be called from only one thread. 
 * @author msekoranja
 */
public class RTPSMessageReceiver extends RTPSMessageEndPoint
	{
		private final int readerId;
		
		private final MessageReceiver receiver = new MessageReceiver();
	    private final MessageReceiverStatistics stats = new MessageReceiverStatistics();

	    public MessageReceiverStatistics getStatistics()
	    {
	    	return stats;
	    }
	    
	    // non-synced list of free buffers
	    private final ArrayDeque<FragmentationBufferEntry> freeFragmentationBuffers;
	    
	    // seqNo -> fragmentation buffer mapping, ordered by seqNo
	    // TODO consider using "xyz<long, FragmenatinBufferEntry>" alternative, with preallocated size
	    private final TreeMap<Long, FragmentationBufferEntry> activeFragmentationBuffers;

	    // TODO consider using "xyz<long, SharedBuffer>" alternative, with preallocated size
	    // TODO consider using TreeSet
	    private final TreeMap<Long, SharedBuffer> completedBuffers;
	    

	    public RTPSMessageReceiver(String multicastNIF, int domainId, int readerId) throws Throwable {
			super(multicastNIF, domainId);
			
			this.readerId = readerId;
			
			// TODO configurable, number of slots (messages)
			int bufferCount = 3;
			freeFragmentationBuffers = new ArrayDeque<FragmentationBufferEntry>(bufferCount);
		    activeFragmentationBuffers = new TreeMap<Long, FragmentationBufferEntry>();
		    completedBuffers = new TreeMap<Long, SharedBuffer>(); 
			
			// TODO max data size, depends on a structure!!!
			final int MAX_PAYLOAD_SIZE = 64*1024*1024;
		    ByteBuffer buffer = ByteBuffer.allocate(bufferCount*MAX_PAYLOAD_SIZE);
			
		    synchronized (freeFragmentationBuffers) {
			    int pos = 0;
			    for (int i = 0; i < bufferCount; i++)
			    {
			    	buffer.position(pos);
			    	pos += MAX_PAYLOAD_SIZE;
			    	buffer.limit(pos);

			    	freeFragmentationBuffers.addLast(new FragmentationBufferEntry(buffer.slice()));
			    }
			}
		    
		    // TODO use logging
		    System.out.println("Receiver: fragmentation buffer size = " + bufferCount + " packets of " + MAX_PAYLOAD_SIZE + " bytes (max payload size)");

		}
	    
	    // used by addAckNackSubmessage method only
	    private int ackNackCounter = 0;

	    // send it:
	    //    - periodically when some packets are missing at the reader side and are available at the writer side
	    //    - when heartbeat + (no new data available received; same lastSN, different count)
	    //      and there are some missing packets that are available
	    //    - every N messages (N = 100?)
	    //	  - immediately when heartbeat with final flag is received
	    // TODO try to piggyback
	    protected void addAckNackSubmessage(ByteBuffer buffer, SequenceNumberSet readerSNState)
	    {
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_ACKNACK, (byte)0x00, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;
		    
		    // readerId
		    buffer.putInt(readerId);		
		    // writerId
		    buffer.putInt(0);
		    
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

	    // TODO out-of-order or duplicate fragments will recreate buffer
	    private FragmentationBufferEntry getFragmentationBufferEntry(long seqNo, int dataSize, int fragmentSize)
	    {
	    	FragmentationBufferEntry entry = activeFragmentationBuffers.get(seqNo);
	    	if (entry != null)
	    		return entry;
	    	
	    	// take next
	    	// TODO !!! take free first, then oldest non-free and mark message as lost; now we are just cycling
	    	synchronized (freeFragmentationBuffers) {
		    	entry = freeFragmentationBuffers.pollLast();
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
				}
				*/
	    	}
	    	else if (entry.seqNo != 0)
	    	{
	    		// TODO logic error
System.out.println("req. for new buffer w/seqNo = " + seqNo + ", using non-free fragment buffer w/ seqNo = " + entry.seqNo + ", finished: " + entry.fragmentsReceived + "/" + entry.fragments);
System.err.println("****************");
System.err.println(stats);
System.exit(1);
return null;
	    	}
	    	
	    	entry.reset(seqNo, dataSize, fragmentSize);
	    	activeFragmentationBuffers.put(seqNo, entry);
	    	
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
	    	long seqNo = 0;	// 0 means unused, reserved
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
		    	
		    	// else copy all data to buffer @ bufferPosition
		    	// and increment received fragment count
		    	buffer.position(bufferPosition);
		    	buffer.put(fragmentData);
		    	fragmentsReceived++;
		    	
		    	// all fragments received?
		    	if (fragmentsReceived == fragments)
		    	{
		    		buffer.position(0);
		    		
		    		// TODO immediate ack using ACKNACK
		    		// TODO optional behavior (via ParamList)
		    		
		    		return true;
		    	}
		    	else
		    		return false;
	    	}
	    	
	    	// NOTE: to be called only once !!!
	    	// TODO can be called from other thread !!! sync seqNo!!!
	    	void release()
	    	{
//System.out.println(seqNo + " release");
				seqNo = 0;
	    		synchronized (freeFragmentationBuffers) {
	    			freeFragmentationBuffers.addLast(this);
				}
	    	}
	    	
	    	// called to release obsolete fragment (message that will never be completed)
	    	void release(Iterator<FragmentationBufferEntry> iterator)
	    	{
//System.out.println(seqNo + " release via iter");
	    		iterator.remove();
	    		seqNo = 0;
	    		synchronized (freeFragmentationBuffers) {
	    			freeFragmentationBuffers.addLast(this);
				}
	    	}

	    	@Override
	    	public ByteBuffer getBuffer()
	    	{
	    		return buffer;
	    	}

	    	// NOTE: to be called only once !!!
			@Override
			public void close() {
				release();
			}
	    	
	    }
	    
	    private int lastHeartbeatCount = Integer.MIN_VALUE;
	    private int lastAckNackCount = Integer.MIN_VALUE;
	    
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
	    
	    private static final int ACKNACK_MISSING_COUNT_THRESHOLD = 64;
	    private long lastAckNackTimestamp = Long.MIN_VALUE;
	    
	    // TODO initialize (message header only once), TODO calculate max size (72?)
	    private final ByteBuffer ackNackBuffer = ByteBuffer.allocate(128);
	    private boolean sendAckNackResponse()
	    {
	    	System.out.println("missing SN count:" + missingSequenceNumbers.size());
	    	if (missingSequenceNumbers.isEmpty())
	    	{
	    		// we ACK all sequence numbers until maxReceivedSequenceNumber
		    	readerSNState.reset(maxReceivedSequenceNumber + 1);
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
System.out.println("sn (" + sn + ") - first (" + first + ") >= 255 ("+ (sn - first) + ")");	
		    			// send NACK only message
		    			break;
		    		}
		    		
		    		readerSNState.set(sn); 
		    	}
		    	// TODO what if we have more !!!
	    	}
	    	
	    	ackNackBuffer.clear();
	    	addMessageHeader(ackNackBuffer);
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
		    
	    	lastAckNackTimestamp = System.currentTimeMillis();
	    	
	    	return true;
	    }
	    
	    private void checkAckNackCondition()
	    {
	    	checkAckNackCondition(false);
	    }

	    private void checkAckNackCondition(boolean onlyTimeLimitCheck)
	    {
	    	if (onlyTimeLimitCheck)
	    	{
		    	// TODO  check
	    		sendAckNackResponse();
	    		
	    		return;
	    	}
	    	
	    	//System.out.println("new missing: " + missingSequenceNumbers);
	    	//System.out.println("sendAckNack: " + missingSequenceNumbers.size());
	    	
	    	// TODO limit time (frequency, period)
	    	
	    	//if (missingSequenceNumbers.size() >= ACKNACK_MISSING_COUNT_THRESHOLD)
	    		sendAckNackResponse();		// request???
	    }
	    
	    
	    public void noData()
	    {
	    	// TODO
	    	if (missingSequenceNumbers.size() > 0)
	    		checkAckNackCondition();
	    }
	    
	    
	    // not thread-safe
	    public boolean processMessage(SocketAddress receivedFrom, ByteBuffer buffer)
		{
			receiver.reset();
			receiver.receivedFrom = receivedFrom;
			
			if (buffer.remaining() < Protocol.RTPS_HEADER_SIZE)
			{
				stats.messageToSmall++;
				return false;
			}
			
			// header fields consist of octet arrays, use big endian to read it
			// in C/C++ ntoh methods would be used
			buffer.order(ByteOrder.BIG_ENDIAN);
			
			// read message header 
			int protocolId = buffer.getInt();
			receiver.sourceVersion = buffer.getShort();
			receiver.sourceVendorId = buffer.getShort();
			buffer.get(receiver.sourceGuidPrefix, 0, 12);
	
			// check protocolId
			if (protocolId != Protocol.ProtocolId.PVDS_VALUE)
			{
				stats.nonRTPSMessage++;
				return false;
			}
	
			// check version
			if (receiver.sourceVersion != ProtocolVersion.PROTOCOLVERSION_2_1)
			{
				stats.versionMismatch++;
				return false;
			}
			
			// check vendor
			if (receiver.sourceVendorId != VendorId.PVDS_VENDORID)
			{
				stats.vendorMismatch++;
				return false;
			}
			
			// process submessages
			int remaining;
			while ((remaining = buffer.remaining()) > 0)
			{
				if (remaining < Protocol.RTPS_SUBMESSAGE_HEADER_SIZE)
				{
					stats.invalidSubmessageSize++;
					return false;
				}
					
				// check alignment
				if (buffer.position() % Protocol.RTPS_SUBMESSAGE_ALIGNMENT != 0)
				{
					stats.submesssageAlignmentMismatch++;
					return false;
				}
				
				// read submessage header
				receiver.submessageId = buffer.get();
				receiver.submessageFlags = buffer.get();
				
				// apply endianess
				ByteOrder endianess = (receiver.submessageFlags & 0x01) == 0x01 ?
											ByteOrder.LITTLE_ENDIAN :
											ByteOrder.BIG_ENDIAN;
				buffer.order(endianess);
				
				// read submessage size (octetsToNextHeader)
				receiver.submessageSize = buffer.getShort() & 0xFFFF;

		        // "jumbogram" condition: octetsToNextHeader == 0 for all except PAD and INFO_TS
				//
				// this submessage is the last submessage in the message and
				// extends up to the end of the message
		        // in case the octetsToNextHeader==0 and the kind of submessage is PAD or INFO_TS,
		        // the next submessage header starts immediately after the current submessage header OR
		        // the PAD or INFO_TS is the last submessage in the message
		        if (receiver.submessageSize == 0 &&
		        	(receiver.submessageId != SubmessageHeader.RTPS_INFO_TS &&
		        	 receiver.submessageId != SubmessageHeader.RTPS_PAD))
		        {
		        	receiver.submessageSize = buffer.remaining();
		        }
		        else if (buffer.remaining() < receiver.submessageSize)
		        {
		        	stats.invalidSubmessageSize++;
		        	return false;
		        }
				
				// min submessage size check
				if (receiver.submessageSize < Protocol.RTPS_SUBMESSAGE_SIZE_MIN)
				{
					stats.invalidSubmessageSize++;
					return false;
				}

				int submessageDataStartPosition = buffer.position();
	
		        stats.submessageType[(receiver.submessageId & 0xFF)]++;
	
				switch (receiver.submessageId) {
	
				case SubmessageHeader.RTPS_DATA:
				case SubmessageHeader.RTPS_DATA_FRAG:
	
					// extraFlags (not used)
					// do not reorder flags (uncomment when used)
					//buffer.order(ByteOrder.BIG_ENDIAN);
					buffer.getShort();
					//buffer.order(endianess);
					
					int octetsToInlineQos = buffer.getShort() & 0xFFFF;

					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);
					
					long seqNo = buffer.getLong();

					stats.receivedSN++;
					//System.out.println("rx: " + seqNo);
					
					if (seqNo < ignoreSequenceNumbersPrior)
					{
						stats.ignoredSN++;
						break;
					}
					else if (lastHeartbeatLastSN == 0)
					{
						// RTPS_HEARTBEAT message must be received first or ignore (seqNo obsolete for this receiver) 
						// NOTE: we accept lastHeartbeatLastSN == 0 && seqNo == 1 not to skip first seqNo
						// i.e. when receiver is started before transmitter
						if (seqNo == 1)
						{
							lastHeartbeatLastSN = 1; // seqNo;
						}
						else
						{
							stats.ignoredSN++;
							break;
						}
					}

					// TODO handle maxReceivedSequenceNumber wrap !!!
					
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
							checkAckNackCondition();
						}
						
						maxReceivedSequenceNumber = seqNo;
					}
					
					if (seqNo > lastKnownSequenceNumber)
						lastKnownSequenceNumber = seqNo;
					else
					{
						// missingSequenceNumbers can only contains SN <= lastKnownSequenceNumber
						// might be a missing SN, try to remove
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
							
							updateMinAvailableSeqNo(ignoreSequenceNumbersPrior, false);
							
							// remove fragmentation buffer, if already allocated
							FragmentationBufferEntry entry = activeFragmentationBuffers.remove(firstFragmentSeqNo);
							if (entry != null)
							{
								//System.out.println(firstFragmentSeqNo + " passed");
								entry.release();
							}
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
								checkAckNackCondition();
							}
						}						
					}
					

					break;

				case SubmessageHeader.RTPS_HEARTBEAT:
					{
						buffer.order(ByteOrder.BIG_ENDIAN);
						// entityId is octet[3] + octet
						receiver.readerId = buffer.getInt();
						receiver.writerId = buffer.getInt();
						buffer.order(endianess);
						
						long firstSN = buffer.getLong();
						long lastSN = buffer.getLong();
						
						int count = buffer.getInt();
						
						if (firstSN <= 0 || lastSN <= 0 || lastSN < firstSN)
						{
							stats.invalidMessage++;
							return false;
						}
						
						// TODO warp !!!
						if (count > lastHeartbeatCount)
						{
							lastHeartbeatCount = count;
						
							// TODO remove
							System.out.println("HEARTBEAT: " + firstSN + " -> " + lastSN + " | " + count);
							
							if (lastSN > lastKnownSequenceNumber)
								lastKnownSequenceNumber = lastSN;
							
							long newMissingSN = 0;
							
							// TODO report nackAck (for ignoreSequenceNumbersPrior) here... periodically or...!!!

							if (maxReceivedSequenceNumber == 0)
							{
								// at start we accept only fresh (seqNo >= lastSN) sequences 
								ignoreSequenceNumbersPrior = lastSN + 1;
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
								ignoreSequenceNumbersPrior = Math.max(ignoreSequenceNumbersPrior, firstSN);
							}
							
							// FinalFlag flag (require response)
							boolean flagF = (receiver.submessageFlags & 0x02) == 0x02;
							if (flagF)
								sendAckNackResponse();
							// no new data on writer side and we have some SN missing - request them!
							else if (lastHeartbeatLastSN == lastSN /*&& missingSequenceNumbers.size() > 0*/)		// TODO
								checkAckNackCondition(true);
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
					break;
					
				case SubmessageHeader.RTPS_ACKNACK:
				{
					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);

					if (!readerSNState.deserialize(buffer))
					{
						stats.invalidMessage++;
						return false;
					}
					
					int count = buffer.getInt();

					// TODO warp !!!
					if (count > lastAckNackCount)
					{
						lastAckNackCount = count;
						
						nack(readerSNState, (InetSocketAddress)receiver.receivedFrom);
						//System.out.println("ACKNACK: " + readerSNState + " | " + count);

						// ack (or receiver does not care anymore) all before readerSNState.bitmapBase
						ack(readerSNState.bitmapBase);
					}
				}
				break;

				default:
					stats.unknownSubmessage++;
					return false;
				}
	
		        // jump to next submessage position
		        buffer.position(submessageDataStartPosition + receiver.submessageSize);	// exception?
			}
			
			stats.validMessage++;
			
			return true;
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
						fragmentEntry.release(fragmentIterator);
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
	    
	    // TODO to be moved out
	    
	    // receiver side
	    private final ArrayBlockingQueue<SharedBuffer> newDataQueue =
	    		new ArrayBlockingQueue<SharedBuffer>(3);		// TODO config
	    
	    private void newDataNotify(SharedBuffer buffer)
	    {
	    	if (!newDataQueue.offer(buffer))
	    	{
				buffer.close();
	    		System.out.println("buffer lost");
	    		System.err.println("********************************");
	    	}
	    }
	    
	    // TODO to be moved out
	    public PVField waitForNewData(PVField data, long timeout) throws InterruptedException
	    {
	    	SharedBuffer buffer = newDataQueue.poll(timeout, TimeUnit.MILLISECONDS);
	    	if (buffer == null)
	    		return null;
	    	else
	    	{
	    		try (SharedBuffer sb = buffer)
	    		{
	    			data.deserialize(sb.getBuffer(), PVDataSerialization.NOOP_DESERIALIZABLE_CONTROL);
	    			return data;
	    		}
	    	}
	    }
	    
	    
	    private class GUIDHasher {
	    	
	    	// optimized GUID (16-byte byte[] converted to 2 longs)
	    	long p1;
	    	long p2;
	    	
	    	public void set(byte[] guidPrefix, int entityId)
	    	{
	    		p1 = CityHash64.getLong(guidPrefix, 0);
	    		int ip2 = CityHash64.getInt(guidPrefix, 8);
	    		p2 = (ip2 << 32) | entityId;
	    	}

			@Override
			public int hashCode() {
				return (int) (p1 ^ p2);
			}

			@Override
			public boolean equals(Object obj) {
				if (obj instanceof GUIDHasher)
				{
					GUIDHasher o = (GUIDHasher)obj;
					return p1 == o.p1 && p2 == o.p2;
				}
				else
					return false;
			}

			@Override
			protected Object clone() throws CloneNotSupportedException {
				GUIDHasher o = new GUIDHasher();
				o.p1 = p1;
				o.p2 = p2;
				return o;
			}
			
			
	    	
	    }
	    
	    public class ReaderEntry {
	    	long lastAliveTime = 0;
	    	LongDynaHeap.HeapMapElement ackSeqNoHeapElement;
	    }
	    
	    private final GUIDHasher guidHasher = new GUIDHasher();
	    
	    private static final int INITIAL_READER_CAPACITY = 16;
	    private final Map<GUIDHasher, ReaderEntry> readerMap = new HashMap<GUIDHasher, ReaderEntry>(INITIAL_READER_CAPACITY);
	    private final LongDynaHeap minAckSeqNoHeap = new LongDynaHeap(INITIAL_READER_CAPACITY);
	    
	    // transmitter side
	    private final Object ackMonitor = new Object();
	    private long lastAckedSeqNo = 0;
	    private final AtomicLong waitForAckedSeqNo = new AtomicLong(0);
	    private void ack(long ackSeqNo)
	    {
	    	System.out.println("ACK: " + ackSeqNo);
	    	guidHasher.set(receiver.sourceGuidPrefix,  receiver.readerId);

	    	ReaderEntry readerEntry = readerMap.get(guidHasher);
	    	if (readerEntry == null)
	    	{
	    		System.out.println("new reader");
	    		readerEntry = new ReaderEntry();
	    		try {
					readerMap.put((GUIDHasher)guidHasher.clone(), readerEntry);
				} catch (CloneNotSupportedException e) {
					// noop
				}
		    	readerEntry.ackSeqNoHeapElement = minAckSeqNoHeap.insert(ackSeqNo);
	    	}
	    	else
	    	{
		    	// TODO wrap
		    	minAckSeqNoHeap.increment(readerEntry.ackSeqNoHeapElement, ackSeqNo);
	    	}

	    	readerEntry.lastAliveTime = System.currentTimeMillis(); // TODO get from receiverStatistic?
	    	
	    	System.out.println(guidHasher.hashCode() + " : " + ackSeqNo);
	    	
	    	long waitedAckSeqNo = waitForAckedSeqNo.get();
	    	System.out.println("\twaited:" + waitedAckSeqNo);
	    	if (waitedAckSeqNo > 0 && minAckSeqNoHeap.size() > 0)
	    	{
	    		long minSeqNo = minAckSeqNoHeap.peek().getValue();
	    	System.out.println("\tmin:" + minSeqNo);
	    		if (minSeqNo >= waitedAckSeqNo)
	    		{
	    			System.out.println("reporting min: " + minSeqNo);
					synchronized (ackMonitor) {
						lastAckedSeqNo = minSeqNo;
						ackMonitor.notifyAll();
					}
	    		}
	    	}
	    }

	    public boolean waitUntilReceived(long seqNo, long timeout) throws InterruptedException
	    {
	    	//if (seqNo <= lastHeartbeatFirstSN)

	    	synchronized (ackMonitor) {

	    		waitForAckedSeqNo.set(seqNo);
	    		while (seqNo >= lastAckedSeqNo)
	    		{
		    		ackMonitor.wait(timeout);
	    		}
	    		
	    		waitForAckedSeqNo.set(0);
	    		return (seqNo < lastAckedSeqNo) ? true : false;
	    	}
	    }

	    protected void nack(SequenceNumberSet readerSNState, InetSocketAddress recoveryAddress)
	    {
	    	// noop from receiver
	    }
	    
	}