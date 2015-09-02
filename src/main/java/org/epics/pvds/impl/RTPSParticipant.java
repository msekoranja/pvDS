package org.epics.pvds.impl;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.Protocol.ProtocolVersion;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.Protocol.VendorId;

/**
 * RTPS message processor implementation.
 * The class itself is not thread-safe.
 * @author msekoranja
 */
public class RTPSParticipant extends RTPSEndPoint
{
	protected final MessageReceiver receiver = new MessageReceiver();
    protected final MessageReceiverStatistics stats = new MessageReceiverStatistics();

    private static final int INITIAL_READER_CAPACITY = 16;
    protected final Map<GUIDHolder, RTPSReader> readerMap = new HashMap<GUIDHolder, RTPSReader>(INITIAL_READER_CAPACITY);

    private static final int INITIAL_WRITER_CAPACITY = 16;
    protected final Map<GUIDHolder, RTPSWriter> writerMap = new HashMap<GUIDHolder, RTPSWriter>(INITIAL_WRITER_CAPACITY);

    public RTPSParticipant(String multicastNIF, int domainId) throws Throwable {
		super(multicastNIF, domainId);
	}
    
    public RTPSReader createReader(int readerId, int maxMessageSize, int messageQueueSize)
    {
    	GUIDHolder guid = new GUIDHolder(GUIDPrefix.GUIDPREFIX.value, readerId);

    	if (readerMap.containsKey(guid))
    		throw new RuntimeException("Reader with such readerId already exists.");
    		
    	RTPSReader reader = new RTPSReader(this, readerId, maxMessageSize, messageQueueSize);
    	readerMap.put(guid, reader);
    	return reader;
    }
	    
    public RTPSWriter createWriter(int writerId, int maxMessageSize, int messageQueueSize)
    {
    	GUIDHolder guid = new GUIDHolder(GUIDPrefix.GUIDPREFIX.value, writerId);

    	if (writerMap.containsKey(guid))
    		throw new RuntimeException("Writer with such writerId already exists.");
    		
    	RTPSWriter writer = new RTPSWriter(this, writerId, maxMessageSize, messageQueueSize);
    	writerMap.put(guid, writer);
    	return writer;
    }

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
					RTPSReader reader = readerMap.get(receiver.readerId);
					if (reader != null)
						reader.processDataSubMessage(octetsToInlineQos, buffer);
						
					break;
				}
				
				case SubmessageHeader.RTPS_HEARTBEAT:
				{
					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);

					receiver.sourceGuidHolder.set(receiver.sourceGuidPrefix, receiver.readerId);
					RTPSReader reader = readerMap.get(receiver.sourceGuidHolder);
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

					receiver.sourceGuidHolder.set(receiver.sourceGuidPrefix, receiver.writerId);
					RTPSWriter writer = writerMap.get(receiver.sourceGuidHolder);
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
    
   private final AtomicBoolean started = new AtomicBoolean();
   public final void start()
    {
	    new Thread(new Runnable() { 
	    	public void run()
	    	{
	    		if (started.getAndSet(true))
	    			return;
	    		
	    		try
	    		{
	    			discoveryMulticastChannel.configureBlocking(false);
	
		    		Selector selector = Selector.open();
		    		discoveryMulticastChannel.register(selector, SelectionKey.OP_READ);
		    		
		    	    ByteBuffer buffer = ByteBuffer.allocate(65536);
	    			
		    	    // TODO stop
	    			while (true)
	    			{
	    				// TODO let decide on timeout, e.g. rtpsReceiver.waitTime();
	    				int keys = selector.select();
	    				if (keys > 0)
	    				{
	    					// ACK all
	    					selector.selectedKeys().clear();
	    					
		    				buffer.clear();
				    	    SocketAddress receivedFrom = discoveryMulticastChannel.receive(buffer);
				    	    buffer.flip();
				    	    processMessage(receivedFrom, buffer);
	    				}
	    			}
	    		}
	    		catch (Throwable th) 
	    		{
	    			th.printStackTrace();
	    		}
	    	}
	    }, "processor-rx-thread").start();
    	
    }
    
}