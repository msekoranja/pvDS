package org.epics.pvds.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.SequenceNumberSet;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.impl.QoS.QOS_SEND_SEQNO_FILTER.SeqNoFilter;
import org.epics.pvds.impl.RTPSParticipant.PeriodicTimerCallback;
import org.epics.pvds.util.BitSet;
import org.epics.pvds.util.LongDynaHeap;

public class RTPSWriter implements PeriodicTimerCallback, AutoCloseable {

	private static final Logger logger = Logger.getLogger(RTPSWriter.class.getName());

	protected final RTPSParticipant participant;
	
	protected final MessageReceiver receiver;
	protected final MessageReceiverStatistics stats;
    protected final DatagramChannel unicastChannel;

    // this instance (writer) EntityId;
	private final int writerId;
	private final GUID writerGUID;
	
    //private int lastAckNackCount = -1;
    
    private final SequenceNumberSet readerSNState = new SequenceNumberSet();

    private final int MIN_SEND_BUFFER_PACKETS = 2;
    
    private final AtomicInteger resendRequestsPending = new AtomicInteger(0);
    
    private final RTPSWriterListener listener;
    
    final class BufferEntry {
    	final ByteBuffer buffer;

    	long sequenceNo;
    	InetSocketAddress sendAddress;
    	
    	// 0 - none, 1 - unicast, > 1 - multicast
    	int resendRequests;
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

    	/*
    	// optimize this
    	public void setSequenceNo(long seqNo)
    	{
    		lock();
    		sequenceNo = seqNo;
    		unlock();
    	}
    	 */
    }
    

    final ArrayBlockingQueue<BufferEntry> freeQueue;
    final ArrayBlockingQueue<BufferEntry> sendQueue;
//    final ArrayBlockingQueue<BufferEntry> resendQueue;
    final ArrayDeque<BufferEntry> resendQueue;
       
    final ConcurrentHashMap<Long, BufferEntry> recoverMap;
    final InetSocketAddress multicastAddress;

    ///
    /// QoS
    ///
    private final static int LIMIT_READERS_DEFAULT = Integer.MAX_VALUE;
    private final int limitReaders;
    private final boolean alwaysSend;
    private final SeqNoFilter sendSeqNoFilter;
    
    /**
     * Constructor.
     * @param writerId writer ID.
     * @param maxMessageSize maximum message size.
     * @param messageQueueSize message queue size (number of slots).
     * @param qos QoS array list, can be <code>null</code>.
     * @param listener this instance listener, can be <code>null</code>.
     */
    public RTPSWriter(RTPSParticipant participant, int writerId,
    		int maxMessageSize, int messageQueueSize,
    		QoS.WriterQOS[] qos,
    		RTPSWriterListener listnener) {
    	this.participant = participant;
    	this.receiver = participant.getReceiver();
    	this.stats = participant.getStatistics();
    	this.unicastChannel = participant.getUnicastChannel();
		this.writerId = writerId;
		this.writerGUID = new GUID(participant.guidPrefix, new EntityId(writerId));
		this.listener = listnener;
		
		if (maxMessageSize <= 0)
			throw new IllegalArgumentException("maxMessageSize <= 0");
		
		if (messageQueueSize <= 0)
			throw new IllegalArgumentException("messageQueueSize <= 0");

		///
		/// QoS
		/// 
		
	    int limitReaders = LIMIT_READERS_DEFAULT;
	    boolean alwaysSend = false;
	    SeqNoFilter sendSeqNoFilter = null;
		if (qos != null)
		{
			for (QoS.WriterQOS wq: qos)
			{
				if (wq instanceof QoS.QOS_LIMIT_READERS)
				{
					limitReaders = ((QoS.QOS_LIMIT_READERS)wq).limit;
				}
				else if (wq == QoS.QOS_ALWAYS_SEND)
				{
					alwaysSend = true;
				}
				else if (wq instanceof QoS.QOS_SEND_SEQNO_FILTER)
				{
					sendSeqNoFilter = ((QoS.QOS_SEND_SEQNO_FILTER)wq).filter;
				}
				else 
				{
					logger.warning(() -> "Unsupported writer QoS: " + wq);
				}
			}
		}
		this.limitReaders = limitReaders;
		this.alwaysSend = alwaysSend;
		this.sendSeqNoFilter = sendSeqNoFilter;
		
		// sender setup
	    try {
			unicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
		    unicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, participant.getMulticastNIF());
		    multicastAddress = new InetSocketAddress(participant.getMulticastGroup(), participant.getMulticastPort());
		} catch (Throwable th) {
			throw new RuntimeException(th);
		}

	    ///
	    /// allocate and initialize buffers
	    ///

	    // add space for header(s)
	    int packetSize = maxMessageSize + DATA_HEADER_LEN + DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN;
	    int bufferSlots = messageQueueSize;

	    final int maxPacketSizeBytes = participant.getMaxPacketSize();
	    if (packetSize > maxPacketSizeBytes)
	    {
	    	// NOTE: min maxPacketSizeBytes > (DATA_HEADER_LEN + DATA_FRAG_SUBMESSAGE_NO_QOS_PREFIX_LEN), i.e. 64 > 56
	    	int pureMessageBytes = maxPacketSizeBytes - DATA_HEADER_LEN - DATA_FRAG_SUBMESSAGE_NO_QOS_PREFIX_LEN;	
		    int slotsPerMessage = (maxMessageSize + pureMessageBytes - 1) / pureMessageBytes;
		    bufferSlots *= slotsPerMessage;
		    packetSize = maxPacketSizeBytes;
	    }
	    	
	    // make sure we have at least MIN_SEND_BUFFER_PACKETS slots
	    bufferSlots = Math.max(bufferSlots, MIN_SEND_BUFFER_PACKETS);
	    
	    freeQueue = new ArrayBlockingQueue<BufferEntry>(bufferSlots);
	    sendQueue = new ArrayBlockingQueue<BufferEntry>(bufferSlots);
//	    resendQueue = new ArrayBlockingQueue<BufferEntry>(bufferSlots);
	    resendQueue = new ArrayDeque<BufferEntry>(bufferSlots);

	    final ByteBuffer buffer = ByteBuffer.allocate(bufferSlots*packetSize);
	
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
	    
	    
	    // setup liveliness timer
	    participant.addPeriodicTimeSubscriber(new GUIDHolder(writerGUID), this);
	    
	    // add support for noop heartbeats
	    // used when there is no readers
	    // this implies first seqNo will be 2
	    writerSequenceNumber.incrementAndGet();
	    lastSentSeqNo = 1;

	    if (logger.isLoggable(Level.CONFIG))
	    {
		    logger.config("Transmitter: buffer size = " + bufferSlots + " packets of " + packetSize + " bytes");
	    }
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
			
			//System.out.println("ACKNACK: " + readerSNState + " | " /*+ count*/);

			nack(readerSNState, (InetSocketAddress)receiver.receivedFrom);

			// ack (or receiver does not care anymore) all before readerSNState.bitmapBase
			ack(readerSNState.bitmapBase);
		}
	}

    public class ReaderEntry {
    	long lastAliveTime = 0;
    	LongDynaHeap.HeapMapElement ackSeqNoHeapElement;
    	InetSocketAddress socketAddress;
    	
    	public long lastAliveTime() {
    		return lastAliveTime;
    	}
    	
    	public long lastAckedSeqNo() {
    		return ackSeqNoHeapElement.getValue();
    	}
    	
    	public InetSocketAddress socketAddress() {
    		return socketAddress;
    	}
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
    		// QOS_LIMIT_READERS check
    		if (readerMap.size() >= limitReaders)
    			return;
    		
    		readerEntry = new ReaderEntry();
	    	readerEntry.ackSeqNoHeapElement = minAckSeqNoHeap.insert(ackSeqNo);
	    	readerEntry.socketAddress = (InetSocketAddress)receiver.receivedFrom;

    		try {
				readerMap.put((GUIDHolder)receiver.sourceGuidHolder.clone(), readerEntry);
			} catch (CloneNotSupportedException e) {
				// noop
			}

    		if (readerCount.incrementAndGet() == 1)
	    		singleReaderSocketAddress.set(readerEntry.socketAddress);
	    	else
	    		singleReaderSocketAddress.set(null);
	    	
	    	if (listener != null)
	    	{
	    		try {
	    			listener.readerAdded(receiver.sourceGuidHolder, receiver.receivedFrom, readerEntry);
	    		} catch (Throwable th) {
	    			// TODO log
	    			th.printStackTrace();
	    		}
	    	}
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
    	Iterator<Map.Entry<GUIDHolder, ReaderEntry>> iterator = readerMap.entrySet().iterator(); 
    	while (iterator.hasNext())
    	{
    		Map.Entry<GUIDHolder, ReaderEntry> mapEntry = iterator.next();
    		ReaderEntry entry = mapEntry.getValue();
    		if (now - entry.lastAliveTime > LIVENESS_TIMEOUT_MS)
    		{
    			iterator.remove();
    			minAckSeqNoHeap.remove(entry.ackSeqNoHeapElement);
		    	
    			if (readerCount.decrementAndGet() == 1)
    			{
		    		singleReaderSocketAddress.set(readerMap.entrySet().iterator().next().getValue().socketAddress);
    			}
		    	else
		    		singleReaderSocketAddress.set(null);
    				
    			updated = true;
		    	
		    	if (listener != null)
		    	{
		    		try {
		    			listener.readerRemoved(mapEntry.getKey());
		    		} catch (Throwable th) {
		    			// TODO log
		    			th.printStackTrace();
		    		}
		    	}
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

    public boolean send(ByteBuffer data, long timeout) throws InterruptedException
    {
    	long seqNo = send(data);
    	return waitUntilAcked(seqNo, timeout);
    }
    
    private static final int DATA_HEADER_LEN = 20;
    private static final int DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN = 24;
    private static final int DATA_FRAG_SUBMESSAGE_NO_QOS_PREFIX_LEN = 36;
    
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
    private final ByteBuffer heartbeatBuffer = ByteBuffer.allocate(64);
    private long lastMulticastHeartbeatTime;
    private final boolean sendHeartbeatMessage() throws IOException
    {
    	// check if the message is valid (e.g. no messages sent or if lastOverridenSeqNo == lastSentSeqNo)
	    long firstSN = lastOverridenSeqNo.get() + 1;
	    	
	   	if (firstSN > lastSentSeqNo)
	   		return false;
	   	heartbeatBuffer.clear();

	   	Protocol.addMessageHeader(writerGUID.prefix, heartbeatBuffer);
    	addHeartbeatSubmessage(heartbeatBuffer, firstSN, lastSentSeqNo);
    	heartbeatBuffer.flip();
 
    	// unicast vs multicast
		// multicasting heartbeat is important as they act as announce messages
	    InetSocketAddress sendAddress = singleReaderSocketAddress.get();
	    long now = System.currentTimeMillis();
	    if (sendAddress == null || (now - lastMulticastHeartbeatTime) > MAX_HEARTBEAT_TIMEOUT_MS)
	    {
	    	sendAddress = multicastAddress;
	    	lastMulticastHeartbeatTime = now;
	    }
		
	    while (heartbeatBuffer.hasRemaining())
	    	unicastChannel.send(heartbeatBuffer, sendAddress);

		messagesSinceLastHeartbeat = 0;

		return true;
    }
    
    // TODO make configurable
    private static long MIN_HEARTBEAT_TIMEOUT_MS = 1;
    private static long MAX_HEARTBEAT_TIMEOUT_MS = 5*1024;		// TODO increase this when subscription is implemented, ideally there should be no HEARTBEAT is there is no clients
    private static long INITIAL_HEARTBEAT_TIMEOUT_MS = 128;
    private static long HEARTBEAT_PERIOD_MESSAGES = 100;		// send every 100 messages (if not sent otherwise)
    
    private AtomicInteger readerCount = new AtomicInteger(0);
    private AtomicReference<InetSocketAddress> singleReaderSocketAddress =
    		new AtomicReference<InetSocketAddress>();

	private int messagesSinceLastHeartbeat = 0;
	private long heartbeatTimeout = INITIAL_HEARTBEAT_TIMEOUT_MS; 
	private boolean resendState = false;
	
	final BufferEntry poll() throws IOException, InterruptedException
    {
		if (resendState)
		{
		    BufferEntry be = resendQueue.poll();
		    if (be != null)
		    	return be;
		    
		    resendState = false;
		}
		
		{
		    BufferEntry be = sendQueue.poll();
		    if (be != null)
		    	return be;
		}
		
    	if (resendRequestsPending.get() > 0)
    	{
        	for (BufferEntry be : freeQueue)
        	{
		    	be.lock();

			    if (be.sequenceNo != 0 && be.resendRequests > 0)
			    	resendQueue.add(be);
			    	
		    	be.unlock();
        	}
        	
    		if (resendQueue.size() > 0)
    		{
    			resendState = true;
    			return resendQueue.poll();
    		}
    	}
    	
    	long now = System.currentTimeMillis();
    	if (now >= heartbeatTime)
    	{
    		sendHeartbeatMessage();
    		
			if (heartbeatTimeout == 0)
				heartbeatTimeout = MIN_HEARTBEAT_TIMEOUT_MS;
			else
				heartbeatTimeout = Math.min(heartbeatTimeout << 1, MAX_HEARTBEAT_TIMEOUT_MS);
			heartbeatTime = now + heartbeatTimeout;
		}
		
		return null;
    }

	private long heartbeatTime = System.currentTimeMillis() + INITIAL_HEARTBEAT_TIMEOUT_MS;
	
	final long getHeartbeatTime()
	{
		return heartbeatTime;
	}
	
	final boolean send(BufferEntry be) throws IOException, InterruptedException
	{
		// reset
    	heartbeatTimeout = 0;

    	be.lock();
		try
		{
	    	InetSocketAddress sendAddress;
	    	
		    if (!resendState)
		    {
		    	recoverMap.put(be.sequenceNo, be);
		    	sendAddress = be.sendAddress;
		    	lastSentSeqNo = be.sequenceNo;
		    	//System.out.println("tx: " + be.sequenceNo);
		    }
		    else
		    {
				// recheck if not taken by prepareBuffer() method
		    	int resendRequestCount = be.resendRequests; be.resendRequests = 0;
		    	resendRequestsPending.getAndAdd(-resendRequestCount);
		    	if (resendRequestCount == 0)
		    		return false;
			    // send on unicast address directly if only one reader is interested in it
		    	sendAddress = (resendRequestCount == 1) ? be.resendUnicastAddress : be.sendAddress;
		    	//System.out.println("resend of " + be.sequenceNo);
		    }
		    
		    if (sendSeqNoFilter == null || sendSeqNoFilter.checkSeqNo(be.sequenceNo))
		    {
			    be.buffer.flip();

			    // NOTE: yes, send can send 0 or be.buffer.remaining() 
			    while (be.buffer.hasRemaining())
			    	unicastChannel.send(be.buffer, sendAddress);
		    }
		}
		finally
		{
			be.unlock();
		}
		
		if (!resendState)
			freeQueue.put(be);

		messagesSinceLastHeartbeat++;
		if (messagesSinceLastHeartbeat > HEARTBEAT_PERIOD_MESSAGES)
		{
			// TODO try to piggyback
			sendHeartbeatMessage();
		}
		
		return true;
	}
    
    // NOTE: this differs from RTPS spec (here writerSN changes also for every fragment)
    // do not put multiple Data/DataFrag message into same message (they will report different ids)
	private final AtomicLong writerSequenceNumber = new AtomicLong(0);
	
	// not thread-safe
    public long send(ByteBuffer data) throws InterruptedException
    {
    	int dataSize = data.remaining();
    	if (dataSize == 0)
    		throw new IllegalArgumentException("empty buffer");
    	else if (dataSize < Long.BYTES)	// FREE_MARK Size
    		throw new IllegalArgumentException("buffer too small, must be at least " + Long.BYTES + " bytes long");
    		
    	// no readers, no sending
    	if (!alwaysSend && readerCount.get() == 0)
    	{
    		// mark buffer as consumed
    		data.position(data.limit());
    		return 0;
    	}
    	
	    // unicast vs multicast
	    InetSocketAddress sendAddress = singleReaderSocketAddress.get();
	    if (sendAddress == null)
	    	sendAddress = multicastAddress;

	    BufferEntry be = takeFreeBuffer(sendAddress);
    	ByteBuffer serializationBuffer = be.buffer;

    	Protocol.addMessageHeader(writerGUID.prefix, serializationBuffer);
	    
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
	    			be = takeFreeBuffer(sendAddress);
	    	    	serializationBuffer = be.buffer;

	    			Protocol.addMessageHeader(writerGUID.prefix, serializationBuffer);
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
    
    private BufferEntry takeFreeBuffer(InetSocketAddress sendAddress) throws InterruptedException
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
	    
	    // TODO reschedule in now time
    }
    
    public GUID getGUID() {
    	return writerGUID;
    }

    // suppresses AutoCloseable.close() exception
	@Override
	public void close()
	{
		participant.destroyWriter(writerId);
	}
	
	
	
	
	
	
}