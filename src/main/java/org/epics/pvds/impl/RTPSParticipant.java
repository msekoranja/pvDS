package org.epics.pvds.impl;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.Protocol.ProtocolVersion;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.Protocol.VendorId;

/**
 * RTPS message processor implementation.
 * The class itself is not thread-safe.
 * @author msekoranja
 */
public class RTPSParticipant extends RTPSEndPoint implements AutoCloseable
{
	protected final MessageReceiver receiver = new MessageReceiver();
    protected final MessageReceiverStatistics stats = new MessageReceiverStatistics();

    private static final int INITIAL_READER_CAPACITY = 16;
    protected final Map<GUIDHolder, RTPSReader> readers = new HashMap<GUIDHolder, RTPSReader>(INITIAL_READER_CAPACITY);

    private static final int INITIAL_WRITER_CAPACITY = 16;
    protected final Map<GUIDHolder, RTPSWriter> writers = new HashMap<GUIDHolder, RTPSWriter>(INITIAL_WRITER_CAPACITY);

    
    private static final int INITIAL_W2R_CAPACITY = 16;
    protected final Map<GUIDHolder, RTPSReader> writer2readerMapping = new HashMap<GUIDHolder, RTPSReader>(INITIAL_W2R_CAPACITY);
   
    public RTPSParticipant(String multicastNIF, int domainId, boolean writersOnly) {
		this(new GUIDPrefix(), multicastNIF, domainId, writersOnly);
	}
    
    public RTPSParticipant(GUIDPrefix guidPrefix, String multicastNIF, int domainId, boolean writersOnly) {
		super(guidPrefix, multicastNIF, domainId, !writersOnly);
	}

    public RTPSReader createReader(int readerId, GUID writerGUID,
    		int maxMessageSize, int messageQueueSize)
    {
    	return createReader(readerId, writerGUID, maxMessageSize, messageQueueSize, QoS.DEFAULT_READER_QOS, null);
    }
    
    public RTPSReader createReader(int readerId, GUID writerGUID,
    		int maxMessageSize, int messageQueueSize,
    		QoS.ReaderQOS[] qos, RTPSReaderListener listener)
    {
    	// writersOnly participant
    	if (multicastChannel == null)
    		throw new IllegalStateException("cannot create reader on writersOnly participant");
    
    	EntityId.verifyEntityKey(readerId);
    	
    	GUIDHolder guid = new GUIDHolder(guidPrefix.value, readerId);

    	if (readers.containsKey(guid))
    		throw new RuntimeException("Reader with such readerId already exists.");
    		
    	RTPSReader reader = new RTPSReader(this, readerId, writerGUID,
    			maxMessageSize, messageQueueSize, qos, listener);
    	readers.put(guid, reader);

    	// mapping
    	writer2readerMapping.put(new GUIDHolder(writerGUID), reader);
    	
    	return reader;
    }
    
    void destroyReader(int readerId, GUID writerGUID)
    {
    	writer2readerMapping.remove(new GUIDHolder(writerGUID));

    	GUIDHolder guid = new GUIDHolder(guidPrefix.value, readerId);
    	readers.remove(guid);
    }
	    
    public RTPSWriter createWriter(int writerId, int maxMessageSize, int messageQueueSize)
    {
    	return createWriter(writerId, maxMessageSize, messageQueueSize, QoS.DEFAULT_WRITER_QOS, null);
    }

    public RTPSWriter createWriter(int writerId,
    		int maxMessageSize, int messageQueueSize,
    		QoS.WriterQOS[] qos, RTPSWriterListener listener)
    {
    	EntityId.verifyEntityKey(writerId);

    	GUIDHolder guid = new GUIDHolder(guidPrefix.value, writerId);

    	if (writers.containsKey(guid))
    		throw new RuntimeException("Writer with such writerId already exists.");
    		
    	RTPSWriter writer = new RTPSWriter(this, writerId, maxMessageSize, messageQueueSize, qos, listener);
    	writers.put(guid, writer);
    	return writer;
    }

    void destroyWriter(int writerId)
    {
    	GUIDHolder guid = new GUIDHolder(guidPrefix.value, writerId);
    	writers.remove(guid);
    }

    private final GUIDHolder localWriterGUID = new GUIDHolder();

    // not thread-safe
    public final boolean processMessage(SocketAddress receivedFrom, ByteBuffer buffer)
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

			switch (receiver.submessageId)
			{
				case SubmessageHeader.RTPS_DATA:
				case SubmessageHeader.RTPS_DATA_FRAG:
				{
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
					
					receiver.sourceGuidHolder.set(receiver.sourceGuidPrefix, receiver.writerId);
					RTPSReader reader = writer2readerMapping.get(receiver.sourceGuidHolder);
					if (reader != null)
						reader.processDataSubMessage(submessageDataStartPosition, octetsToInlineQos, buffer);

					break;
				}
				
				case SubmessageHeader.RTPS_HEARTBEAT:
				{
					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);

					receiver.sourceGuidHolder.set(receiver.sourceGuidPrefix, receiver.writerId);
					RTPSReader reader = writer2readerMapping.get(receiver.sourceGuidHolder);
					if (reader != null)
						reader.processHeartbeatSubMessage(buffer);
					
					break;
				}

				case SubmessageHeader.RTPS_ACKNACK:
				{
					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);
	
					receiver.sourceGuidHolder.set(receiver.sourceGuidPrefix, receiver.readerId);

					// ACKNACK is an unicast response, use local GUID + receiver.writerId
					localWriterGUID.set(guidPrefix.value, receiver.writerId);
					RTPSWriter writer = writers.get(localWriterGUID);
					if (writer != null)
						writer.processAckNackSubMessage(buffer);

					break;
				}
				
				default:
					stats.unknownSubmessage++;
					break;
			}
			
	        // jump to next submessage position
	        buffer.position(submessageDataStartPosition + receiver.submessageSize);	// check for out of bounds exception?
		}
		
		stats.validMessage++;
		
		return true;
	}
    
    public final MessageReceiver getReceiver()
    {
    	return receiver;
    }

    public final MessageReceiverStatistics getStatistics()
    {
    	return stats;
    }
   
    
    private static final int INITIAL_TIMER_CAPACITY = 16;
    private final ConcurrentHashMap<Object, PeriodicTimerCallback> periodicTimerSubscribers
    	= new ConcurrentHashMap<Object, PeriodicTimerCallback>(INITIAL_TIMER_CAPACITY);
    
    interface PeriodicTimerCallback {
    	void onPeriod(long now);
    }
    
    void addPeriodicTimeSubscriber(Object key, PeriodicTimerCallback callback)
    {
    	periodicTimerSubscribers.put(key, callback);
    }
    
    void removePeriodicTimeSubscriber(Object key)
    {
    	periodicTimerSubscribers.remove(key);
    }

    public final GUIDPrefix getGUIDPrefix()
    {
    	return guidPrefix;
    }
    
    public void periodicTimer(long now)
    {
    	for (PeriodicTimerCallback cb : periodicTimerSubscribers.values())
    	{
    		try {
    			cb.onPeriod(now);
    		} catch (Throwable th) {
    			// TODO log
    			th.printStackTrace();
    		}
    	}
    }
    
    public final long PERIODIC_TIMER_MS = 1000;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    public void start()
    {
		if (started.getAndSet(true))
			return;
		
	    new Thread(() -> {
    		
    		try
    		{
	    		Selector selector = Selector.open();

	    		// readers
	    		if (multicastChannel != null)
	    			multicastChannel.register(selector, SelectionKey.OP_READ, multicastChannel);
	    		
	    		// readers (user data over unicast) and writers (receiving ACKNACK)
	    		unicastChannel.register(selector, SelectionKey.OP_READ, unicastChannel);
	    		
	    		
	    	    final ByteBuffer buffer = ByteBuffer.allocate(65536);
    			
	    	    long lastPeriodicTimer = System.currentTimeMillis();
    			while (!stopped.get())
    			{
    				int keys = selector.select(PERIODIC_TIMER_MS);
    				if (keys > 0)
    				{
	    				for (SelectionKey key : selector.selectedKeys())
	    				{
	    					try {
			    				buffer.clear();
					    	    SocketAddress receivedFrom = ((DatagramChannel)key.attachment()).receive(buffer);
					    	    buffer.flip();
					    	    processMessage(receivedFrom, buffer);
	    					} catch (Throwable th) {
	    						th.printStackTrace();
	    					}
	    				}
	    				selector.selectedKeys().clear();
    				}
    				
    				long now = System.currentTimeMillis();
    				if (now - lastPeriodicTimer >= PERIODIC_TIMER_MS)
    				{
    					periodicTimer(now);
    					lastPeriodicTimer = now;
    				}
    			}
    		}
    		catch (Throwable th) 
    		{
    			th.printStackTrace();
    		}
	    }, "processor-rx-thread").start();
    	
    }

    public void stop()
    {
		stopped.set(true);
    }
    
    // suppresses AutoCloseable.close() exception
	@Override
	public void close()
	{
		ArrayList<RTPSWriter> writersArray = new ArrayList<RTPSWriter>(writers.values());
		writersArray.forEach((writer) -> writer.close());
		
		ArrayList<RTPSReader> readersArray = new ArrayList<RTPSReader>(readers.values());
		readersArray.forEach((reader) -> reader.close());
		
		stop();
	}
}