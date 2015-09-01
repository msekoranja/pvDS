package org.epics.pvds.impl;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.ProtocolVersion;
import org.epics.pvds.Protocol.SubmessageHeader;
import org.epics.pvds.Protocol.VendorId;

/**
 * RTPS message processor (receiver) implementation.
 * The class itself is not thread-safe, i.e. processMessage() method should be called from only one thread. 
 * @author msekoranja
 */
public abstract class RTPSMessageProcessor extends RTPSMessageEndPoint
{
	protected final MessageReceiver receiver = new MessageReceiver();
    protected final MessageReceiverStatistics stats = new MessageReceiverStatistics();

    public RTPSMessageProcessor(String multicastNIF, int domainId) throws Throwable {
		super(multicastNIF, domainId);
	}
	    
    abstract boolean processSubMessage(byte submessageId, ByteOrder endianess, ByteBuffer buffer);

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

	        if (!processSubMessage(receiver.submessageId, endianess, buffer))
				stats.unknownSubmessage++;

	        // jump to next submessage position
	        buffer.position(submessageDataStartPosition + receiver.submessageSize);	// check for out of bounds exception?
		}
		
		stats.validMessage++;
		
		return true;
	}
    
    public final MessageReceiverStatistics getStatistics()
    {
    	return stats;
    }
    
}