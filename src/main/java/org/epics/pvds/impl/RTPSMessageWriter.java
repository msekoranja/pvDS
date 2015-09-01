package org.epics.pvds.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.SerializableControl;
import org.epics.pvds.Protocol.SequenceNumberSet;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.util.CityHash64;
import org.epics.pvds.util.LongDynaHeap;

public class RTPSMessageWriter extends RTPSMessageProcessor implements SerializableControl {

		// this instance (writer) EntityId;
		final int writerId;
		
	    private int lastAckNackCount = Integer.MIN_VALUE;
	    
	    private final SequenceNumberSet readerSNState = new SequenceNumberSet();
	    
	    // TODO to be configurable

		// NOTE: Giga means 10^9 (not 1024^3)
	    final double udpTxRateGbitPerSec = Double.valueOf(System.getProperty("RATE", "0.96")); // TODO !!
	    final int MESSAGE_ALIGN = 32;
	    final int MAX_PACKET_SIZE_BYTES_CONF = Integer.valueOf(System.getProperty("SIZE", "8000"));
	    final int MAX_PACKET_SIZE_BYTES = (MAX_PACKET_SIZE_BYTES_CONF / MESSAGE_ALIGN) * MESSAGE_ALIGN;
	    long delay_ns = (long)(MAX_PACKET_SIZE_BYTES * 8 / udpTxRateGbitPerSec);
	
	    // TODO configurable
	    final int SEND_BUFFER_SIZE_KB = 3*10*1024;		// TODO
	    final int MIN_SEND_BUFFER_PACKETS = 2;
	    
	    final AtomicInteger resendRequestsPending = new AtomicInteger(0);
	    
	    class BufferEntry {
	    	final ByteBuffer buffer;

	    	long sequenceNo;
	    	InetSocketAddress sendAddress;
	    	
	    	// 0 - none, 1 - unicast, > 1 - multicast
	    	final AtomicInteger resendRequests = new AtomicInteger(0);
	    	final AtomicReference<InetSocketAddress> resendUnicastAddress = new AtomicReference<InetSocketAddress>();
	    	
	    	public BufferEntry(ByteBuffer buffer) {
	    		this.buffer = buffer;
	    	}
	    	
	    	// thread-safe, must never lock (no locks)
	    	public void resendRequest(InetSocketAddress address)
	    	{
	    		int resendRequestsCount = resendRequests.getAndIncrement();
	    		if (resendRequestsCount == 0)
	    			resendUnicastAddress.set(address);
	    		// duplicate call of same reader, this check is here
	    		// to avoid the same reader increment resendRequests > 1
	    		// and as consequence resend could not be done as unicast
// TODO this causes problems !!!!
//	    		else if (resendUnicastAddress.get() == address)
//	    			resendRequests.compareAndSet(resendRequestsCount + 1, resendRequestsCount);
	    		
	    		// NOTE: sum of all resendRequest might not be same as resendRequestsPending for a moment
	    		// we do not care for multiple request of the same reader
	    		resendRequestsPending.incrementAndGet();
	    	}
	    	
	    	// NOTE: no call to resendRequest() must be made after this method is called, until message is actually sent
	    	public synchronized ByteBuffer prepare(long sequenceId, InetSocketAddress sendAddress)
	    	{
	    		// invalidate all resend requests
	    		resendRequestsPending.addAndGet(-resendRequests.getAndSet(0));
	    		resendUnicastAddress.set(null);

	    		this.sequenceNo = sequenceId;
	    		this.sendAddress = sendAddress;
	    		
	    		buffer.clear();
	    		
	    		return buffer;
	    	}

	    	/// ???
	    	public void beforeSend()
	    	{
	    		resendRequestsPending.addAndGet(-resendRequests.getAndSet(0));
	    	}

	    	/// ???
	    	public void afterSend()
	    	{
	    		resendRequestsPending.addAndGet(-resendRequests.getAndSet(0));
	    	}
	    }
	    

	    final ArrayBlockingQueue<BufferEntry> freeQueue;
	    final ArrayBlockingQueue<BufferEntry> sendQueue;
	    
	    final ConcurrentHashMap<Long, BufferEntry> recoverMap = new ConcurrentHashMap<Long, BufferEntry>();

	    // TODO for test
	    final InetSocketAddress multicastAddress;

		final ByteBuffer serializationBuffer;

	    public RTPSMessageWriter(String multicastNIF, int domainId, int writerId) throws Throwable {
			super(multicastNIF, domainId);
			this.writerId = writerId;

		    // sender setup
		    discoveryUnicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
		    discoveryUnicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, nif);
		    multicastAddress = new InetSocketAddress(discoveryMulticastGroup, discoveryMulticastPort);

		    int bufferPacketsCount = (int)(SEND_BUFFER_SIZE_KB * 1024L / (MAX_PACKET_SIZE_BYTES+SPARE_BUFFER_SIZE));
		    if (bufferPacketsCount < MIN_SEND_BUFFER_PACKETS)
		    	throw new RuntimeException("send buffer to small");
		    
		    freeQueue = new ArrayBlockingQueue<BufferEntry>(bufferPacketsCount);
		    sendQueue = new ArrayBlockingQueue<BufferEntry>(bufferPacketsCount);

		    ByteBuffer buffer = ByteBuffer.allocate(bufferPacketsCount*(MAX_PACKET_SIZE_BYTES+SPARE_BUFFER_SIZE));
		    serializationBuffer = buffer.duplicate();
		
		    //BufferEntry[] bufferPackets = new BufferEntry[bufferPacketsCount];
		    int pos = 0;
		    for (int i = 0; i < bufferPacketsCount; i++)
		    {
		    	buffer.limit(pos + MAX_PACKET_SIZE_BYTES);
		    	buffer.position(pos);
		    	pos += MAX_PACKET_SIZE_BYTES+SPARE_BUFFER_SIZE;

		    	//bufferPackets[i] = new BufferEntry(buffer.slice());
		    	freeQueue.add(new BufferEntry(buffer.slice()));
		    }
		    
		    // TODO use logging
		    System.out.println("Transmitter: buffer size = " + bufferPacketsCount + " packets of " + MAX_PACKET_SIZE_BYTES + 
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
	    
	    public boolean processSubMessage(byte submessageId, ByteOrder endianess, ByteBuffer buffer)
	    {
			switch (submessageId) {
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

			return true;
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

	    
	    private static final int DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN = 24;
	    
	    // no fragmentation
	    public long addDataSubmessage(ByteBuffer buffer, PVField data, int dataSize)
	    {
	    	// big endian, data flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_DATA, (byte)0x04, 0x0000);
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
		    
		    // InlineQoS TODO
		    
		    // Data
		    // must fit this buffer, this is not DATA_FRAG message
		    // TODO use serialize() method that does not bound checks !!!
		    data.serialize(buffer, PVDataSerialization.NOOP_SERIALIZABLE_CONTROL);
		    
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
	    public void addHeartbeatSubmessage(ByteBuffer buffer, long firstSN, long lastSN)
	    {
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_HEARTBEAT, (byte)0x00, 28);
		    
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
	    public long addDataFragSubmessageHeader(ByteBuffer buffer)
	    {
	    	boolean firstFragment = (fragmentStartingNum == 1);
	    	
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_DATA_FRAG, (byte)0x00, 0x0000);

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
		    	fragmentSize = buffer.remaining() - 6;	// - fragmentSize - dataSize (TODO - InlineQoS)
			    fragmentSize = Math.min(fragmentDataLeft, fragmentSize);
		    }
		    buffer.putShort((short)fragmentSize);
		    
		    fragmentDataLeft -= fragmentSize;

		    // sampleSize (unsigned integer)
		    buffer.putInt(fragmentTotalSize);
		    
		    // InlineQoS TODO

		    // data payload comes next
		    
		    // octetsToNextHeader is left to 0 (until end of the message)
		    // this means this DataFrag message must be the last message 
		    
		    return sn;
	    }

	    // fragmentation
	    public long addDataFragSubmessage(ByteBuffer buffer, PVField data, int dataSize)
	    {
	    	fragmentTotalSize = fragmentDataLeft = dataSize;
	    	fragmentSize = 0;		// to be initialized later
	    	fragmentStartingNum = 1;
	    	
	    	long sn = addDataFragSubmessageHeader(buffer);
	    			    
		    data.serialize(buffer, this);
		    
		    return sn;
	    }

	    /*
	    // TODO tmp
	    public void addAnnounceSubmessage(ByteBuffer buffer, int changeCount, Locator unicastEndpoint,
	    		int entitiesCount, BloomFilter<String> filter)
	    {
	    	addSubmessageHeader(buffer, SubmessageHeader.PVDS_ANNOUNCE, (byte)0x00, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;
		    
		    // unicast discovery locator
		    unicastEndpoint.serialize(buffer);
		    
		    // change count 
		    buffer.putInt(changeCount);

		    // service locators // TODO
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
			System.out.println("NACK: " + readerSNState + " / LAST OVERRIDEN SEQNO:" + lastOverridenSeqNo.get());

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
    	    			be.resendRequest(recoveryAddress);
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
	   		if (lastSentSeqNo == 0 || firstSN >= lastSentSeqNo)
	   			return false;
	   		heartbeatBuffer.clear();

	   		addMessageHeader(heartbeatBuffer);
    		addHeartbeatSubmessage(heartbeatBuffer, firstSN, lastSentSeqNo);
    		heartbeatBuffer.flip();
    		// TODO !!!
		    discoveryUnicastChannel.send(heartbeatBuffer, multicastAddress);

		    return true;
	    }
	    
	    // TODO make configurable
	    private static long MIN_HEARTBEAT_TIMEOUT_MS = 1;
	    private static long MAX_HEARTBEAT_TIMEOUT_MS = 64*1024;
	    private static long HEARTBEAT_PERIOD_MESSAGES = 100;		// send every 100 messages (if not sent otherwise)
	    
	    // TODO implement these
	    private AtomicLong oldestACKedSequenceNumber = new AtomicLong(0);
	    private AtomicInteger readerCount = new AtomicInteger(1);

	    // TODO heartbeat, acknack are not excluded (do not sent lastSendTime)
	    private long lastSendTime = 0;
	    
	    public void sendProcess() throws IOException, InterruptedException
	    {
	    	int messagesSinceLastHeartbeat = 0;
	    	long heartbeatTimeout = MAX_HEARTBEAT_TIMEOUT_MS; 
	    	
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
			    		if (readerCount.get() > 0 &&
			    			oldestACKedSequenceNumber.get() < lastSentSeqNo)
			    		{
			    			if (sendHeartbeatMessage())
			    				messagesSinceLastHeartbeat = 0;
			    		}
			    		// TODO consider w/o "<< 1"
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
			    	if (sleep_ns < 1000)		// TODO
			    		break;
			    	else if (sleep_ns > 100000)	// TODO on linux this is ~2000
				    	Thread.sleep(0);
			    	//	Thread.yield();
			    }*/
				long endTime = lastSendTime + delay_ns;
				while (endTime - System.nanoTime() > 0);
				
				lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?

			    //System.out.println(sleep_ns);

			    if (be.sequenceNo != 0)
			    {
			    	recoverMap.put(be.sequenceNo, be);
			    	lastSentSeqNo = be.sequenceNo;
			    	
			    		// TODO remove
			    	//System.out.println("tx: " + be.sequenceNo);
			    }
			    be.buffer.flip();
			    discoveryUnicastChannel.send(be.buffer, be.sendAddress);
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
			    int resendRequestCount = be.resendRequests.get();
			    if (resendRequestCount > 0)
			    {
				    // TODO we need at least 1us precision?
			    	// we do not want to sleep while lock is held, do it here
				    long endTime = lastSendTime + delay_ns;
					while (endTime - System.nanoTime() > 0);

					// guard buffer from being taken by serialization process
					synchronized (be)
		    		{
						// recheck if not taken by prepareBuffer() method
					    resendRequestCount = be.resendRequests.getAndSet(0);
					    resendRequestsPending.getAndAdd(-resendRequestCount);
					    if (resendRequestCount > 0)
					    {
						    lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?

						    be.buffer.flip();
						    // send on unicast address directly if only one reader is interested in it
						    // TODO commented out since reader is only listening on multicast port
						    //discoveryUnicastChannel.send(be.buffer, (resendRequestCount == 1) ? be.resendUnicastAddress.get() : be.sendAddress);
						    discoveryUnicastChannel.send(be.buffer, be.sendAddress);
		
						    messagesSinceLastHeartbeat++;
					    }
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
		
		BufferEntry activeBufferEntry;
		ByteBuffer activeBuffer;
		
		// TODO remove
		public ByteBuffer getBuffer()
		{
			return serializationBuffer;
		}
		
		// not thread-safe
	    public long send(PVField data) throws InterruptedException
	    {
	    	int dataSize = PVDataSerialization.getSerializationSize(data);

	    	takeFreeBuffer();
		    
		    addMessageHeader(serializationBuffer);
		    // TODO put all necessary messages here
		    
		    if ((dataSize + DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN) <= MAX_PACKET_SIZE_BYTES)
		    	addDataSubmessage(serializationBuffer, data, dataSize);
		    else
		    	addDataFragSubmessage(serializationBuffer, data, dataSize);

		    sendBuffer();
		    
		    return writerSequenceNumber.get();
	    }
	    
	    // TODO
	    public void waitUntilSent() {
	    	while (!sendQueue.isEmpty())
	    		Thread.yield();
	    }
	    
	    private long lastSentSeqNo = 0;
	    private final AtomicLong lastOverridenSeqNo = new AtomicLong(0);
	    
	    private void takeFreeBuffer() throws InterruptedException
	    {
	    	// TODO close existing one?

	    	// TODO !!!
		    activeBufferEntry = freeQueue.poll(1, TimeUnit.SECONDS);
		    // TODO remove?
		    if (activeBufferEntry == null)
		    	throw new RuntimeException("no free buffer");
	    	
		    // TODO option with timeout...
		    
		    long seqNo = activeBufferEntry.sequenceNo;
		    if (seqNo != 0)
		    {
		    	recoverMap.remove(seqNo);
		    	lastOverridenSeqNo.set(seqNo);
		    }

		    // TODO
		    InetSocketAddress sendAddress = multicastAddress;
		    
		    activeBuffer = activeBufferEntry.prepare(0, sendAddress);
		    
		    // note: activeBuffer wraps buffer (serializationBuffer)
		    int offset = activeBuffer.arrayOffset();
		    serializationBuffer.position(0);
		    serializationBuffer.limit(offset + activeBuffer.capacity());
		    serializationBuffer.position(offset);
	    }
	    
	    private final long newWriterSequenceNumber()
	    {
	    	long newWriterSN = writerSequenceNumber.incrementAndGet();
	    	//System.out.println("tx newWriterSN: " + newWriterSN);
	    	activeBufferEntry.sequenceNo = newWriterSN;
	    	return newWriterSN;
	    }
	    
	    private void sendBuffer() throws InterruptedException
	    {
	    	// copy back
		    int offset = activeBuffer.arrayOffset();
	    	activeBuffer.position(serializationBuffer.position() - offset);
	    	activeBuffer.limit(serializationBuffer.limit() - offset);
	    	
		    sendQueue.put(activeBufferEntry);

		    activeBufferEntry = null;
	    	activeBuffer = null;
	    }

		final int SPARE_BUFFER_SIZE = 8;
	    final byte[] spareBuffer = new byte[SPARE_BUFFER_SIZE];
	    private int spareOverflow = 0;
	    
	    private boolean spareEnabled = false;
	    private int preSpareLimit = 0;
	    
		@Override
		public void flushSerializeBuffer() {
			
			try {
				if (spareEnabled)
				{
					spareOverflow = serializationBuffer.position() - preSpareLimit;
					if (spareOverflow < 0)
					{
						spareEnabled = false;
						throw new RuntimeException("serialization logic exception");
					}
					
					// save "overflow"
					serializationBuffer.position(preSpareLimit);
					serializationBuffer.get(spareBuffer);
					
					// fix limit
					serializationBuffer.position(preSpareLimit);
					serializationBuffer.limit(preSpareLimit);
				}
				else
				{
					// not full
					if (serializationBuffer.hasRemaining())
					{
						preSpareLimit = serializationBuffer.limit();
						serializationBuffer.limit(preSpareLimit + SPARE_BUFFER_SIZE);
						spareEnabled = true;
						return;
					}
				}
				
				sendBuffer();
				takeFreeBuffer();
				addMessageHeader(serializationBuffer);
				addDataFragSubmessageHeader(serializationBuffer);
				
				if (spareEnabled && spareOverflow > 0)
				{
					// TODO or not.... check if we have spareOverflow of free buffer...
					serializationBuffer.put(spareBuffer, 0, spareOverflow);
					spareOverflow = 0;
					spareEnabled = false;
				}
					
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			
		}

		@Override
		public void ensureBuffer(int size) {
			if (serializationBuffer.remaining() < size)
			{
				flushSerializeBuffer();

				if (serializationBuffer.remaining() < size)
					throw new RuntimeException("requested buffer size exceeds maximum payload size");
			}
		}

		@Override
		public void alignBuffer(int alignment) {
			// TODO 
		}

		@Override
		public void cachedSerialize(Field field, ByteBuffer buffer) {
			// no-cache
			field.serialize(buffer, this);
		}
	    
	}