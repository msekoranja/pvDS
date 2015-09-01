package org.epics.pvds.impl;

import java.net.SocketAddress;

public final class MessageReceiver 
{
	SocketAddress receivedFrom;
	
	short sourceVersion;
	short sourceVendorId;
	byte[] sourceGuidPrefix = new byte[12];
	byte submessageId;
	byte submessageFlags;
	int submessageSize;
	//byte[] destGuidPrefix = new byte[12];
	//unicastReplyLocatorList;
	//multicastReplyLocatorList;
	//boolean haveTimestamp;
	//Time_t timestamp;
	
	// TODO EntityId
	int readerId;
	int writerId;
	
	GUIDHolder sourceGuidHolder = new GUIDHolder();
	
	public void reset() 
	{
		// it does not makes sence to reset
		// receivedFrom
		// sourceVersion, sourceVendorId, sourceGuidPrefix
		// since they are reset by every message header 
	}
}