package org.epics.pvds.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.Protocol.SequenceNumberSet;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.impl.RTPSParticipant.PeriodicTimerCallback;
import org.epics.pvds.util.BitSet;
import org.epics.pvds.util.LongDynaHeap;

public class RTPSWriter implements PeriodicTimerCallback {

		protected final MessageReceiver receiver;
		protected final MessageReceiverStatistics stats;

	    protected final DatagramChannel unicastChannel;

	    // this instance (writer) EntityId;
		private final int writerId;
		private final GUID writerGUID;
		
	    //private int lastAckNackCount = -1;
	    
	    private final SequenceNumberSet readerSNState = new SequenceNumberSet();
	    
	    // TODO to be configurable

		// NOTE: Giga means 10^9 (not 1024^3)
	    private final double udpTxRateGbitPerSec = Double.valueOf(System.getProperty("PVDS_MAX_THROUGHPUT", "0.90")); // TODO make configurable, figure out better default!!!
	    private final int MESSAGE_ALIGN = 32;
	    private final int MAX_PACKET_SIZE_BYTES_CONF = Integer.valueOf(System.getProperty("PVDS_MAX_UDP_PACKET_SIZE", "8000"));
	    private final int MAX_PACKET_SIZE_BYTES = ((MAX_PACKET_SIZE_BYTES_CONF + MESSAGE_ALIGN - 1) / MESSAGE_ALIGN) * MESSAGE_ALIGN;
	    private long delay_ns = (long)(MAX_PACKET_SIZE_BYTES * 8 / udpTxRateGbitPerSec);
	
	    private final int MIN_SEND_BUFFER_PACKETS = 2;
	    
	    private final AtomicInteger resendRequestsPending = new AtomicInteger(0);
	    
	    final class BufferEntry {
	    	final ByteBuffer buffer;

	    	long sequenceNo;
	    	InetSocketAddress sendAddress;
	    	
	    	// 0 - none, 1 - unicast, > 1 - multicast
	    	// TODO optimize sync
	    	volatile int resendRequests;
	    	InetSocketAddress resendUnicastAddress;
	    	
	    	final Lock lock = new ReentrantLock();
	    	
	    	public BufferEntry(ByteBuffer buffer) {
	    		this.buffer = buffer;
	    	}
	    	
	    	public void lock()
	    	{
	    		lock.lock();
	    	}
	    	
	    	public void unlock()
	    	{
	    		lock.unlock();
	    	}
	    	
	    	public void resendRequest(long expectedSeqNo, InetSocketAddress address)
	    	{
	    		lock();
	    		try 
	    		{
		    		if (expectedSeqNo != sequenceNo)
		    			return;
		    		
		    		if (resendRequests == 0)
		    			resendUnicastAddress = address;
		    		resendRequests++;
		    		
		    		// NOTE: sum of all resendRequest might not be same as resendRequestsPending for a moment
		    		// we do not care for multiple requests of the same reader
		    		resendRequestsPending.incrementAndGet();
	    		}
	    		finally
	    		{
	    			unlock();
	    		}
	    	}
	    	
	    	// NOTE: no call to resendRequest() must be made after this method is called, until message is actually sent
	    	public synchronized ByteBuffer prepare(long sequenceId, InetSocketAddress sendAddress)
	    	{
	    		// invalidate all resend requests
	    		lock();
	    		try 
	    		{
		    		resendRequestsPending.addAndGet(-resendRequests); resendRequests = 0;
		    		resendUnicastAddress = null;
	
		    		this.sequenceNo = sequenceId;
		    		this.sendAddress = sendAddress;
		    		
		    		buffer.clear();
	    		
		    		return buffer;
	    		}
	    		finally
	    		{
	    			unlock();
	    		}
	    	}
	    	
	    	// TODO optimize this
	    	public void setSequenceNo(long seqNo)
	    	{
	    		lock();
	    		sequenceNo = seqNo;
	    		unlock();
	    	}

	    }
	    

	    final ArrayBlockingQueue<BufferEntry> freeQueue;
	    final ArrayBlockingQueue<BufferEntry> sendQueue;
	    
	    final ConcurrentHashMap<Long, BufferEntry> recoverMap;

	    final InetSocketAddress multicastAddress;

	    public RTPSWriter(RTPSParticipant participant,
	    		int writerId, int maxMessageSize, int messageQueueSize) {
	    	this.receiver = participant.getReceiver();
	    	this.stats = participant.getStatistics();
	    	this.unicastChannel = participant.getUnicastChannel();
			this.writerId = writerId;
			this.writerGUID = new GUID(GUIDPrefix.GUIDPREFIX, new EntityId(writerId));

			if (maxMessageSize <= 0)
				throw new IllegalArgumentException("maxMessageSize <= 0");
			
			if (messageQueueSize <= 0)
				throw new IllegalArgumentException("messageQueueSize <= 0");

			// sender setup
		    try {
				unicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
			    unicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, participant.getMulticastNIF());
			    multicastAddress = new InetSocketAddress(participant.getMulticastGroup(), participant.getMulticastPort());
			} catch (Throwable th) {
				throw new RuntimeException(th);
			}

		    // add space for header(s)
		    int packetSize = maxMessageSize + DATA_HEADER_LEN + DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN;
		    int bufferSlots = messageQueueSize;
		    
		    if (packetSize > MAX_PACKET_SIZE_BYTES)
		    {
			    int slotsPerMessage = (maxMessageSize + MAX_PACKET_SIZE_BYTES - 1) / MAX_PACKET_SIZE_BYTES;
			    bufferSlots *= slotsPerMessage;
			    packetSize = MAX_PACKET_SIZE_BYTES;
		    }
		    	
		    // make sure we have at least MIN_SEND_BUFFER_PACKETS slots
		    bufferSlots = Math.max(bufferSlots, MIN_SEND_BUFFER_PACKETS);
		    
		    freeQueue = new ArrayBlockingQueue<BufferEntry>(bufferSlots);
		    sendQueue = new ArrayBlockingQueue<BufferEntry>(bufferSlots);

		    ByteBuffer buffer = ByteBuffer.allocate(bufferSlots*packetSize);
		
		    //BufferEntry[] bufferPackets = new BufferEntry[bufferPacketsCount];
		    int pos = 0;
		    for (int i = 0; i < bufferSlots; i++)
		    {
		    	buffer.limit(pos + packetSize);
		    	buffer.position(pos);
		    	pos += packetSize;

		    	//bufferPackets[i] = new BufferEntry(buffer.slice());
		    	freeQueue.add(new BufferEntry(buffer.slice()));
		    }
		    
		    recoverMap = new ConcurrentHashMap<Long, BufferEntry>(bufferSlots);
		    
		    participant.addPeriodicTimeSubscriber(new GUIDHolder(writerGUID), this);
		    
		    // add support for noop heartbeats
		    // used when there is no readers
		    writerSequenceNumber.incrementAndGet();
		    lastSentSeqNo = 1;
		    
		    // TODO use logging
		    System.out.println("Transmitter: buffer size = " + bufferSlots + " packets of " + packetSize + 
		    				   " bytes, rate limit: " + udpTxRateGbitPerSec + "Gbit/sec (period: " + delay_ns + " ns)");

		}
	    
	    // TODO activeBuffers can have message header initialized only once
	    /*
	    public void initializeMessage(ByteBuffer buffer)
	    {
		    // MessageHeader
	    	buffer.putLong(Protocol.HEADER_NO_GUID);
		    buffer.put(guidPrefix.value);
	    }
	    
	    public void resetMessage(ByteBuffer buffer)
	    {
	    	// MessageHeader is static (fixed)
	    	buffer.position(Protocol.RTPS_HEADER_SIZE);
	    	buffer.limit(buffer.capacity());
	    }
	    */

	    void processAckNackSubMessage(ByteBuffer buffer)
	    {
			if (!readerSNState.deserialize(buffer))
			{
				stats.invalidMessage++;
				return;
			}
			
			/*int count =*/ buffer.getInt();

			// TODO lastAckNackCount is per reader !!!
			// NOTE: warp concise comparison
//			if (count - lastAckNackCount > 0)
			{
//				lastAckNackCount = count;
				
				//System.out.println("ACKNACK: " + readerSNState + " | " + count);

				nack(readerSNState, (InetSocketAddress)receiver.receivedFrom);

				// ack (or receiver does not care anymore) all before readerSNState.bitmapBase
				ack(readerSNState.bitmapBase);
			}
		}

	    public class ReaderEntry {
	    	long lastAliveTime = 0;
	    	LongDynaHeap.HeapMapElement ackSeqNoHeapElement;
	    }
	    
	    
	    private static final int INITIAL_READER_CAPACITY = 16;
	    private final Map<GUIDHolder, ReaderEntry> readerMap = new HashMap<GUIDHolder, ReaderEntry>(INITIAL_READER_CAPACITY);
	    private final LongDynaHeap minAckSeqNoHeap = new LongDynaHeap(INITIAL_READER_CAPACITY);
	    
	    // transmitter side
	    private final Object ackMonitor = new Object();
	    private long lastAckedSeqNo = 0;
	    private final AtomicLong waitForAckedSeqNo = new AtomicLong(0);
	    private void ack(long ackSeqNo)
	    {
	    	// NOTE: ackSeqNo is acked seqNo + 1
	    	//System.out.println("ACK: " + ackSeqNo);

	    	ReaderEntry readerEntry = readerMap.get(receiver.sourceGuidHolder);
	    	if (readerEntry == null)
	    	{
	    		System.out.println("---------------------------------- new reader");
	    		readerEntry = new ReaderEntry();
	    		try {
					readerMap.put((GUIDHolder)receiver.sourceGuidHolder.clone(), readerEntry);
				} catch (CloneNotSupportedException e) {
					// noop
				}
		    	readerEntry.ackSeqNoHeapElement = minAckSeqNoHeap.insert(ackSeqNo);
		    	readerCount.incrementAndGet();
	    	}
	    	else
	    	{
		    	minAckSeqNoHeap.increment(readerEntry.ackSeqNoHeapElement, ackSeqNo);
	    	}

	    	readerEntry.lastAliveTime = System.currentTimeMillis();
	    	
	    	//System.out.println(receiver.sourceGuidHolder.hashCode() + " : " + ackSeqNo);
	    	
	    	ackSeqNoNotifyCheck();
	    }
	    
	    private void ackSeqNoNotifyCheck()
	    {
	    	long waitedAckSeqNo = waitForAckedSeqNo.get();
	    	//System.out.println("\twaited:" + waitedAckSeqNo);
	    	if (waitedAckSeqNo > 0 && minAckSeqNoHeap.size() > 0)
	    	{
	    		long minSeqNo = minAckSeqNoHeap.peek().getValue();
	    		//System.out.println("\tmin:" + minSeqNo);
	    		if (minSeqNo >= waitedAckSeqNo)
	    		{
	    			//System.out.println("reporting min: " + minSeqNo);
					synchronized (ackMonitor) {
						lastAckedSeqNo = minSeqNo;
						ackMonitor.notifyAll();
					}
	    		}
	    	}
	    }
	    
	    private final long LIVENESS_CHECK_PERIOD_MS = 5*1000;
	    private final long LIVENESS_TIMEOUT_MS = 10*1000;
	    
	    // to be called periodically within the same (as ack()) processing thread 
	    void livenessCheck() {
	    	
	    	if (readerMap.size() == 0)
	    		return;
	    	
	    	boolean updated = false;
	    	long now = System.currentTimeMillis();
	    	Iterator<ReaderEntry> iterator = readerMap.values().iterator(); 
	    	while (iterator.hasNext())
	    	{
	    		ReaderEntry entry = iterator.next();
	    		if (now - entry.lastAliveTime > LIVENESS_TIMEOUT_MS)
	    		{
	    			iterator.remove();
	    			minAckSeqNoHeap.remove(entry.ackSeqNoHeapElement);
			    	readerCount.decrementAndGet();
			    	updated = true;
		    		System.out.println("---------------------------------- dead reader");
	    		}
	    	}
	    	
	    	if (updated)
		    	ackSeqNoNotifyCheck();
	    }
	    

	    long lastLivenessCheck = 0;
	    
	    @Override
		public void onPeriod(long now) {
	    	if (now - lastLivenessCheck >= LIVENESS_CHECK_PERIOD_MS)
	    	{
	    		livenessCheck();
	    		lastLivenessCheck = now;
	    	}
		}

		public boolean waitUntilAcked(long seqNo, long timeout) throws InterruptedException
	    {
		    // NOTE: seqNo is really expectedSeqNo + 1
	    
			// nothing to ack since nothing was sent
			if (seqNo == 0)
				return true;
			
	    	//TODO if (seqNo <= lastHeartbeatFirstSN)
	    	// TODO overridenSN?
	    	
	    	synchronized (ackMonitor) {

	    		if (readerCount.get() == 0 ||
	    			seqNo <= lastAckedSeqNo)
	    			return true;
	    		
	    		waitForAckedSeqNo.set(seqNo);
	    		long l1 = System.currentTimeMillis();
	    		while (seqNo > lastAckedSeqNo)
	    		{
		    		ackMonitor.wait(timeout);
		    		
		    		// spurious wake-up check
		    		if ((System.currentTimeMillis() - l1) >= timeout)
		    			break;
	    		}
	    		
	    		waitForAckedSeqNo.set(0);
	    		return (seqNo <= lastAckedSeqNo) ? true : false;
	    	}
	    }

	    
	    private static final int DATA_HEADER_LEN = 20;
	    private static final int DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN = 24;
	    
	    // no fragmentation
	    private long addDataSubmessage(ByteBuffer buffer, ByteBuffer data, int dataSize)
	    {
	    	// big endian, data flag
	    	Protocol.addSubmessageHeader(buffer, SubmessageHeader.RTPS_DATA, (byte)0x04, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;

		    // extraFlags
		    buffer.putShort((short)0x0000);
		    // octetsToInlineQoS
		    buffer.putShort((short)0x0010);		// 16 = 4+4+8
		    
		    // readerId
		    buffer.putInt(0);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // writerSN
		    long sn = newWriterSequenceNumber();
		    buffer.putLong(sn);
		    
		    // InlineQoS TODO InlineQoS not supported (not needed now)
		    
		    // Data
		    // must fit this buffer, this is not DATA_FRAG message
		    buffer.put(data);
		    
		    // set message size
		    int octetsToNextHeader = buffer.position() - octetsToNextHeaderPos - 2;
		    buffer.putShort(octetsToNextHeaderPos, (short)(octetsToNextHeader & 0xFFFF));
		    
		    return sn;
	    }

	    private int heartbeatCounter = 0;
	    
	    // tells what is currently in send buffers
	    
	    // send it:
	    //    - when no data to send
	    //           - (immediately when queue gets empty (*)) 
	    //           - periodically with back-off (if all acked all, no need)
	    //    - every N messages
	    //    (- manually)
	    // * every message that sends seqNo, in a way sends heartbeat.lastSN
	    private void addHeartbeatSubmessage(ByteBuffer buffer, long firstSN, long lastSN)
	    {
	    	// big endian flag
	    	Protocol.addSubmessageHeader(buffer, SubmessageHeader.RTPS_HEARTBEAT, (byte)0x00, 28);
		    
		    // readerId
		    buffer.putInt(0);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // firstSN
		    buffer.putLong(firstSN);
		    // lastSN
		    buffer.putLong(lastSN);
		    
		    // count
		    buffer.putInt(heartbeatCounter++);
	    }

	    private int fragmentStartingNum = 1;
	    private int fragmentTotalSize = 0;
	    private int fragmentSize = 0;
	    private int fragmentDataLeft = 0;
	    
	    // fragmentation
	    private long addDataFragSubmessageHeader(ByteBuffer buffer)
	    {
	    	boolean firstFragment = (fragmentStartingNum == 1);
	    	
	    	// big endian flag
	    	Protocol.addSubmessageHeader(buffer, SubmessageHeader.RTPS_DATA_FRAG, (byte)0x00, 0x0000);

		    // extraFlags
		    buffer.putShort((short)0x0000);
		    // octetsToInlineQoS
		    buffer.putShort((short)0x0010);		// 16 = 4+4+8
		    
		    // readerId
		    buffer.putInt(0);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // writerSN
		    long sn = newWriterSequenceNumber();
		    buffer.putLong(sn);
		    
		    // fragmentStartingNum (unsigned integer, starting from 1)
		    buffer.putInt(fragmentStartingNum++);
		    
		    // fragmentsInSubmessage (unsigned short)
		    buffer.putShort((short)1);
		    
		    // fragmentSize (unsigned short)
		    // should not change, last actually fragmented data size is < fragmentSize
		    if (firstFragment)
		    {
		    	fragmentSize = buffer.remaining() - 6;	// - fragmentSize - dataSize (TODO InlineQoS)
			    fragmentSize = Math.min(fragmentDataLeft, fragmentSize);
		    }
		    buffer.putShort((short)fragmentSize);
		    
		    fragmentDataLeft -= fragmentSize;

		    // sampleSize (unsigned integer)
		    buffer.putInt(fragmentTotalSize);
		    
		    // InlineQoS TODO InlineQoS not supported (not needed now)

		    // data payload comes next
		    
		    // octetsToNextHeader is left to 0 (until end of the message)
		    // this means this DataFrag message must be the last message 
		    
	    	return sn;
	    }

	    /*
	    public void addAnnounceSubmessage(ByteBuffer buffer, int changeCount, Locator unicastEndpoint,
	    		int entitiesCount, BloomFilter<String> filter)
	    {
	    	addSubmessageHeader(buffer, SubmessageHeader.PVDS_ANNOUNCE, (byte)0x00, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;
		    
		    // unicast discovery locator
		    unicastEndpoint.serialize(buffer);
		    
		    // change count 
		    buffer.putInt(changeCount);

		    // service locators
		    // none for now
		    buffer.putInt(0);
		    
		    // # of discoverable entities (-1 not supported, or dynamic)
		    buffer.putInt(entitiesCount);
		    
		    if (entitiesCount > 0)
		    {
			    buffer.putInt(filter.k());
			    buffer.putInt(filter.m());
			    long[] bitArray = filter.bitSet().getBitArray();
			    buffer.asLongBuffer().put(bitArray);
			    buffer.position(buffer.position() + bitArray.length * 8);
		    }
		    
		    // set message size (generic code) for now
		    int octetsToNextHeader = buffer.position() - octetsToNextHeaderPos - 2;
		    buffer.putShort(octetsToNextHeaderPos, (short)(octetsToNextHeader & 0xFFFF));
	    }
	    */
	    
	    // recovery marker
	    protected final void nack(SequenceNumberSet readerSNState, InetSocketAddress recoveryAddress)
	    {
			//System.out.println("NACK: " + readerSNState + " / LAST OVERRIDEN SEQNO:" + lastOverridenSeqNo.get());

			long base = readerSNState.bitmapBase;
	    	BitSet bitSet = readerSNState.bitmap;
	        int i = -1;
            for (i = bitSet.nextSetBit(i+1); i >= 0; i = bitSet.nextSetBit(i+1))
            {
            	long nackedSN = base + i;
                int endOfRun = bitSet.nextClearBit(i);
                do { 
                	
    	    		BufferEntry be = recoverMap.get(nackedSN);
    	    		if (be != null)
    	    		{
    	    			be.resendRequest(nackedSN, recoveryAddress);
    	    		}
    	    		else
    	    		{
    	    			// recovery works in FIFO manner, therefore also none
    	    			// of the subsequent packets will be available
    	    			return;
    	    		}
                	
                	nackedSN++;
                } while (++i < endOfRun);
            }
	    }
	    
	    // TODO preinitialize, can be fixed
	    final static ByteBuffer heartbeatBuffer = ByteBuffer.allocate(64);
	    private final boolean sendHeartbeatMessage() throws IOException
	    {
	    	// check if the message is valid (e.g. no messages sent or if lastOverridenSeqNo == lastSentSeqNo)
	    	long firstSN = lastOverridenSeqNo.get() + 1;
	    	
	   		if (firstSN > lastSentSeqNo)
	   			return false;
	   		heartbeatBuffer.clear();

	   		Protocol.addMessageHeader(heartbeatBuffer);
    		addHeartbeatSubmessage(heartbeatBuffer, firstSN, lastSentSeqNo);
    		heartbeatBuffer.flip();
 
    		unicastChannel.send(heartbeatBuffer, multicastAddress);

		    return true;
	    }
	    
	    // TODO make configurable
	    private static long MIN_HEARTBEAT_TIMEOUT_MS = 1;
	    private static long MAX_HEARTBEAT_TIMEOUT_MS = 5*1024;		// TODO increase this when subscription is implemented, indealy there should be no HEARTBEAT is there is no clients
	    private static long INITIAL_HEARTBEAT_TIMEOUT_MS = 16;
	    private static long HEARTBEAT_PERIOD_MESSAGES = 100;		// send every 100 messages (if not sent otherwise)
	    
	    // TODO implement these
	    private AtomicInteger readerCount = new AtomicInteger(0);

	    // TODO heartbeat, acknack are not excluded (do not sent lastSendTime)
	    private long lastSendTime = 0;
	    
	    public void sendProcess() throws IOException, InterruptedException
	    {
	    	int messagesSinceLastHeartbeat = 0;
	    	long heartbeatTimeout = INITIAL_HEARTBEAT_TIMEOUT_MS; 
	    	
		    // sender
		    while (true)
		    {
			    BufferEntry be = sendQueue.poll();
			    if (be == null)
			    {
				    //System.out.println("resendRequestsPending: " + resendRequestsPending.get());
			    	if (resendRequestsPending.get() > 0)
			    	{
			    		resendProcess();
				    	heartbeatTimeout = MIN_HEARTBEAT_TIMEOUT_MS;
			    		continue;
			    	}
			    	
			    	be = sendQueue.poll(heartbeatTimeout, TimeUnit.MILLISECONDS);
			    	if (be == null)
			    	{
		    			if (sendHeartbeatMessage())
		    				messagesSinceLastHeartbeat = 0;
		    			
			    		heartbeatTimeout = Math.min(heartbeatTimeout << 1, MAX_HEARTBEAT_TIMEOUT_MS);
			    		continue;
			    	}
			    }
			    
		    	heartbeatTimeout = MIN_HEARTBEAT_TIMEOUT_MS;

		    	// TODO we need at least 1us precision?

		    	// while-loop only version
			    // does not work well on single-core CPUs
/*
			    long endTime = lastSendTime + delay_ns;
				long sleep_ns;
			    while (true)
			    {
					lastSendTime = System.nanoTime();
			    	sleep_ns = endTime - lastSendTime;
			    	if (sleep_ns < 1000)
			    		break;
			    	else if (sleep_ns > 100000)	// NOE: on linux this is ~2000
				    	Thread.sleep(0);
			    	//	Thread.yield();
			    }*/
				long endTime = lastSendTime + delay_ns;
				while (endTime - System.nanoTime() > 0);
				lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?
			    //System.out.println(sleep_ns);

				be.lock();
				try
				{
				    if (be.sequenceNo != 0)
				    {
				    	recoverMap.put(be.sequenceNo, be);
				    	lastSentSeqNo = be.sequenceNo;
				    	//System.out.println("tx: " + be.sequenceNo);
				    }
				    be.buffer.flip();
				    // TODO use unicast if there is only one reader !!!
				    // NOTE: yes, send can send 0 or be.buffer.remaining() 
				    while (be.buffer.remaining() > 0)
				    	unicastChannel.send(be.buffer, be.sendAddress);
				}
				finally
				{
					be.unlock();
				}
			    freeQueue.put(be);

		    	messagesSinceLastHeartbeat++;
		    	if (messagesSinceLastHeartbeat > HEARTBEAT_PERIOD_MESSAGES)
		    	{
		    		// TODO try to piggyback
	    			if (sendHeartbeatMessage())
	    				messagesSinceLastHeartbeat = 0;
		    	}

		    }
	    }
	    
	    private void resendProcess() throws IOException, InterruptedException
	    {
	    	int messagesSinceLastHeartbeat = 0;

	    	for (BufferEntry be : freeQueue)
	    	{
			    if (be.resendRequests > 0)
			    {
			    	be.lock();
			    	try
			    	{
				    	// TODO we need at least 1us precision?
				    	// we do not want to sleep while lock is held, do it here
					    long endTime = lastSendTime + delay_ns;
						while (endTime - System.nanoTime() > 0);

						// recheck if not taken by prepareBuffer() method
					    int resendRequestCount = be.resendRequests; be.resendRequests = 0;
					    resendRequestsPending.getAndAdd(-resendRequestCount);
					    if (be.sequenceNo != 0 && resendRequestCount > 0)
					    {
						    lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?

						    be.buffer.flip();
						    // send on unicast address directly if only one reader is interested in it
						    while (be.buffer.remaining() > 0)
						    	unicastChannel.send(be.buffer, (resendRequestCount == 1) ? be.resendUnicastAddress : be.sendAddress);
		
						    messagesSinceLastHeartbeat++;
					    }
			    	}
			    	finally
			    	{
			    		be.unlock();
		    		}
	    		}

	    		// do not hold lock while sending heartbeat
	    		// (this is why this block is moved outside sync block above; was just after messagesSinceLastHeartbeat++)
	    		if (messagesSinceLastHeartbeat > HEARTBEAT_PERIOD_MESSAGES)
		    	{
		    		// TODO try to piggyback
	    			if (sendHeartbeatMessage())
	    				messagesSinceLastHeartbeat = 0;
		    	}
	    	}
	    }
	    
	    // NOTE: this differs from RTPS spec (here writerSN changes also for every fragment)
	    // do not put multiple Data/DataFrag message into same message (they will report different ids)
		private final AtomicLong writerSequenceNumber = new AtomicLong(0);
		
		// not thread-safe
	    public long send(ByteBuffer data) throws InterruptedException
	    {
	    	// TODO configurable
	    	// no readers, no sending
	    	if (readerCount.get() == 0)
	    	{
	    		// mark buffer as consumed
	    		data.position(data.limit());
	    		return 0;
	    	}
	    	
	    	int dataSize = data.remaining();

	    	BufferEntry be = takeFreeBuffer();
	    	ByteBuffer serializationBuffer = be.buffer;

	    	Protocol.addMessageHeader(serializationBuffer);
		    
		    if ((dataSize + DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN) <= serializationBuffer.remaining())
		    	addDataSubmessage(serializationBuffer, data, dataSize);
		    else
		    {
		    	// TODO check for availability of all buffers? or go ahead and hope for the best?
		    	
		    	fragmentTotalSize = fragmentDataLeft = dataSize;
		    	fragmentSize = 0;		// to be initialized later
		    	fragmentStartingNum = 1;
		    	
		    	// TODO what if all fragments do not fit into the buffer (e.g. only part of them)
		    	while (true)
		    	{
		    		addDataFragSubmessageHeader(serializationBuffer);
		    		
		    		int dataRemaining = data.remaining();
		    		int bufferRemaining = serializationBuffer.remaining();
		    		if (dataRemaining > bufferRemaining)
		    		{
			    		int limit = data.limit();
			    		data.limit(data.position() + bufferRemaining);
			    		serializationBuffer.put(data);
			    		data.limit(limit);
			    		
			    		//be.setSequenceNo(writerSequenceNumber.get());
					    // lock is being held
					    be.sequenceNo = writerSequenceNumber.get();
		    			sendBuffer(be);
		    			be = takeFreeBuffer();
		    	    	serializationBuffer = be.buffer;

		    			Protocol.addMessageHeader(serializationBuffer);
		    		}
		    		else
		    		{
			    		serializationBuffer.put(data);
		    			break;
		    		}
		    	}
		    }
		    
    		//be.setSequenceNo(writerSequenceNumber.get());
		    // lock is being held
		    be.sequenceNo = writerSequenceNumber.get();
		    sendBuffer(be);
		    
		    return writerSequenceNumber.get();
	    }
	    
	    public void waitUntilSent() {
	    	while (!sendQueue.isEmpty())
	    		Thread.yield();
	    }
	    
	    private long lastSentSeqNo = 0;
	    private final AtomicLong lastOverridenSeqNo = new AtomicLong(0);
	    
	    private BufferEntry takeFreeBuffer() throws InterruptedException
	    {
	    	// TODO close existing one?

	    	// TODO non blocking !!!
	    	BufferEntry be = freeQueue.poll(1, TimeUnit.SECONDS);
		    // TODO return null?
		    if (be == null)
		    	throw new RuntimeException("no free buffer");
	    	
		    // TODO option with timeout...
		    
		    // NOTE lock is left held
		    be.lock();

		    long seqNo = be.sequenceNo;
		    if (seqNo != 0)
		    {
		    	recoverMap.remove(seqNo);
		    	lastOverridenSeqNo.set(seqNo);
		    }

		    InetSocketAddress sendAddress = multicastAddress;
		    be.prepare(0, sendAddress);

		    return be;
	    }
	    
	    private final long newWriterSequenceNumber()
	    {
	    	long newWriterSN = writerSequenceNumber.incrementAndGet();
	    	//System.out.println("tx newWriterSN: " + newWriterSN);
	    	return newWriterSN;
	    }
	    
	    private void sendBuffer(BufferEntry be) throws InterruptedException
	    {
	    	// unlocks what takeFreeBuffer locks
	    	be.unlock();
	    	
	    	// enqueue
		    sendQueue.put(be);
	    }
	    
	    private final AtomicBoolean started = new AtomicBoolean();
	    public void start() {
		    new Thread(() -> {
    			if (started.getAndSet(true))
    				return;
	    		try
	    		{
    				sendProcess();
	    		}
	    		catch (Throwable th) 
	    		{
	    			th.printStackTrace();
	    		}
		    }, "writer-thread").start();
	    }

	    public GUID getGUID() {
	    	return writerGUID;
	    }
}