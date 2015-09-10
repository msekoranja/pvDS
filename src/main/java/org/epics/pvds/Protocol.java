/**
 * 
 */
package org.epics.pvds;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import org.epics.pvds.util.BitSet;
import org.epics.pvds.util.HexDump;

/**
 * @author msekoranja
 *
 */
public final class Protocol {

	
	/**
	 * Protocol ID.
	 * @author msekoranja
	 */
	public static class ProtocolId {
		// octet[4]
		//public static final byte[] VALUE = { 0x70, 0x76, 0x44, 0x53 }; 	// pvDS
		//public static final byte[] RTPS_VALUE = { 0x52, 0x54, 0x50, 0x53 }; 	// RTPS

		public static final int PVDS_VALUE = 0x70764453; 	// pvDS
		//public static final int RTPS_VALUE = 0x52545053; 	// RTPS
	}
	
	/**
	 * Protocol version.
	 * @author msekoranja
	 */
	public static class ProtocolVersion {
		// octet major, octet minor
		//public static final byte[] PROTOCOLVERSION_2_1 = { 2, 1 };
		
		public static final short PROTOCOLVERSION_2_1 = 0x0201;
	}

	public static class VendorId {
		// octet[2]
		//private final byte[] value = new byte[2];
		//public static final byte[] VENDORID_UNKNOWN = { 0, 0 };

		public static final short VENDORID_UNKNOWN = 0x0000;
		
		public static final short PVDS_VENDORID = 0x01CA;		// fictional; not confirmed by OMG
	}

	/**
	 * 12-byte GUID prefix.
	 * GUID consists of 12-byte prefix and 4-byte EntityId (process-wide unique).
	 * @author msekoranja
	 */
	public static class GUIDPrefix {
		// octet[12]
		public final byte[] value;
		
		public static final byte[] GUIDPREFIX_UNKNOWN_VALUE = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		
		private GUIDPrefix() {
			value = new byte[12];
			
			// put first 12-bytes of UUID into the prefix byte-array
			UUID uuid4 = UUID.randomUUID();
			ByteBuffer bb = ByteBuffer.wrap(value);
			bb.putLong(uuid4.getMostSignificantBits());
			bb.putInt((int)(uuid4.getLeastSignificantBits() >>> 32));
		}
		
		public GUIDPrefix(byte[] value) {
			this.value = value;
		}
		
		public static final GUIDPrefix GUIDPREFIX_UNKNOWN = new GUIDPrefix(GUIDPREFIX_UNKNOWN_VALUE);

		public static final GUIDPrefix GUIDPREFIX = new GUIDPrefix();
	}

	public static class EntityId {
		// octet[3] entityKey, octet entityKind
		//private final byte[] value = new byte[4];
		//public static final byte[] ENTITYID_UNKNOWN = { 0, 0, 0, 0 };
		
		public final int value;
		
		
		// first two MSB
		public static byte ENTITYKIND_USERDEFINED_MASK = 0x00;
		public static byte ENTITYKIND_BUILDIN_MASK = (byte)0xc0;
		public static byte ENTITYKIND_VENDORSPECIFIC_MASK = 0x40;
		
		// last 6 LSB
		public static byte ENTITYKIND_UNKNOWN_MASK = 0x00;
		public static byte ENTITYKIND_PARTICIPANT_MASK = 0x01;	// always build-in
		public static byte ENTITYKIND_WRITER_MASK = 0x02;
		public static byte ENTITYKIND_WRITERKEY_MASK = 0x03;
		public static byte ENTITYKIND_READER_MASK = 0x04;
		public static byte ENTITYKIND_READERKEY_MASK = 0x07;

		private EntityId(int entityKey, byte entityKind)
		{
			if (entityKey < 0 || entityKey > 0x00FFFFFF)
				throw new IllegalArgumentException("entityKey < 0 || entityKey > 0x00FFFFFF");
			
			value = entityKey << 8 | entityKind;
		}
		
		public EntityId(int entityId)
		{
			value = entityId;
		}

		public static final EntityId ENTITYID_UNKNOWN = new EntityId(0, (byte)0);
		
		public static EntityId generateParticipantEntityId(int participantId)
		{
			if (participantId > MAX_PARTICIPANT_ID)
				throw new IllegalArgumentException("participantId > MAX_PARTICIPANT_ID");
			
			return new EntityId(participantId,
					(byte)(ENTITYKIND_BUILDIN_MASK | ENTITYKIND_PARTICIPANT_MASK));
		}
		
	}
	
	public static class GUID
	{
		public final GUIDPrefix prefix;
		public final EntityId entityId;

		/**
		 * @param prefix
		 * @param entityId
		 */
		public GUID(GUIDPrefix prefix, EntityId entityId) {
			this.prefix = prefix;
			this.entityId = entityId;
		}

		@Override
		public String toString() {
			StringBuffer b = new StringBuffer(32);
			for (byte v : prefix.value)
				b.append(HexDump.toHex(v));
			int entityValue = entityId.value;
			byte b1 = (byte)(entityValue & 0xFF); entityValue >>>= 8;
			byte b2 = (byte)(entityValue & 0xFF); entityValue >>>= 8;
			byte b3 = (byte)(entityValue & 0xFF); entityValue >>>= 8;
			byte b4 = (byte)(entityValue & 0xFF); 
			b.append(HexDump.toHex(b4));
			b.append(HexDump.toHex(b3));
			b.append(HexDump.toHex(b2));
			b.append(HexDump.toHex(b1));
			return b.toString();
		}

		public static GUID parse(String str)
		{
			if (str.length() != 32)
				throw new IllegalArgumentException("string not a valid GUID");
			
			byte[] prefix = new byte[12];
			for (int i = 0; i < 12; i++)
				prefix[i] = (byte)Integer.parseInt(str.substring(i*2,i*2+2), 16);
			
			byte b1 = (byte)Integer.parseInt(str.substring(24, 26), 16);
			byte b2 = (byte)Integer.parseInt(str.substring(26, 28), 16);
			byte b3 = (byte)Integer.parseInt(str.substring(28, 30), 16);
			byte b4 = (byte)Integer.parseInt(str.substring(30, 32), 16);
			int entityValue = b1 << 24 | b2 << 16 | b3 << 8 | b4;
			
			return new GUID(new GUIDPrefix(prefix), new EntityId(entityValue));
		}
		
	}
	
	// max UDP payload 65507 bytes for IPv4 and 65487 bytes for IPv6
	// some OS limit to 8k?!
	// MTU UDP 1440 (w/ IPSEC)
	
	public static final long HEADER_NO_GUID =
		(long)ProtocolId.PVDS_VALUE << 32 |
		ProtocolVersion.PROTOCOLVERSION_2_1 << 16 |
		VendorId.PVDS_VENDORID;
/*	
	public static class Message {
		ProtocolId pi;
		ProtocolVersion pv;
		VendorId vi;
		GUIDPrefix gp;
	}

	public static class SubmessageHeader {
		byte submessageId;	// Submessages with ID's 0x80 to 0xff (inclusive) are vendor-specific;
		byte flags;	// LSB = endian 	// E = SubmessageHeader.flags & 0x01 (0 = big, 1 = little)
		ushort octetsToNextHeader;
		
	}
	*/
	/*
The representation of this field is a CDR unsigned short (ushort).
In case octetsToNextHeader > 0, it is the number of octets from the first octet of the contents of the Submessage until the first octet of the header of the next Submessage (in case the Submessage is not the last Submessage in the Message) OR it is the number of octets remaining in the Message (in case the Submessage is the last Submessage in the Message). An interpreter of the Message can distinguish these two cases as it knows the total length of the Message.
In case octetsToNextHeader==0 and the kind of Submessage is NOT PAD or INFO_TS, the Submessage is the last Submessage in the Message and extends up to the end of the Message. This makes it possible to send Submessages larger than 64k (the size that can be stored in the octetsToNextHeader field), provided they are the last Submessage in the Message.
In case the octetsToNextHeader==0 and the kind of Submessage is PAD or INFO_TS, the next Submessage header starts immediately after the current Submessage header OR the PAD or INFO_TS is the last Submessage in the Message.
*/
	public static final int RTPS_HEADER_SIZE = 20;
	public static final int RTPS_SUBMESSAGE_ALIGNMENT = 4;
	public static final int RTPS_SUBMESSAGE_HEADER_SIZE = 4;
	public static final int RTPS_SUBMESSAGE_SIZE_MIN = 8;
	
	public static final int MAX_DOMAIN_ID = 232;
	public static final int MAX_PARTICIPANT_ID = 119;

	// TODO this collides with RTPS
	public static final int PB = 7400;
	public static final int DG = 250;
	public static final int PG = 2;
	public static final int d0 = 0;
	public static final int d1 = 10;
	public static final int d2 = 1;
	public static final int d3 = 11;
	
	public static class SubmessageHeader {

	    public static final byte RTPS_PAD            = 0x01;
	    public static final byte RTPS_ACKNACK        = 0x06;
	    public static final byte RTPS_HEARTBEAT      = 0x07;
	    public static final byte RTPS_GAP            = 0x08;
	    public static final byte RTPS_INFO_TS        = 0x09;
	    public static final byte RTPS_INFO_SRC       = 0x0c;
	    public static final byte RTPS_INFO_REPLY_IP4 = 0x0d;
	    public static final byte RTPS_INFO_DST       = 0x0e;
	    public static final byte RTPS_INFO_REPLY     = 0x0f;
	    public static final byte RTPS_NACK_FRAG      = 0x12;
	    public static final byte RTPS_HEARTBEAT_FRAG = 0x13;
	    public static final byte RTPS_DATA 	  	     = 0x15;
	    public static final byte RTPS_DATA_FRAG      = 0x16;
	    
	    // Submessages with ID's 0x80 to 0xff (inclusive) are vendor-specific
	    public static final byte PVDS_ANNOUNCE    	 = (byte)0x80;
	    public static final byte PVDS_SHUTDOWN    	 = (byte)0x81;
		
	}
	
	public static class Locator
	{
		static final public int LOCATOR_ADDRESS_SIZE = 16;
		
		int kind;
		int port;	// unsigned
		byte[] address;

		static final byte[] LOCATOR_ADDRESS_INVALID = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
		
		static final int LOCATOR_PORT_INVALID = 0;
		
		static final int LOCATOR_KIND_INVALID = -1;
		static final int LOCATOR_KIND_RESERVED = 0;
		static final int LOCATOR_KIND_UDPv4 = 1;
		static final int LOCATOR_KIND_UDPv6 = 2;
		
		private Locator(int kind, int port, byte[] address)
		{
			this.kind = kind;
			this.port = port;
			if (address.length != LOCATOR_ADDRESS_SIZE)
				throw new IllegalArgumentException("address.length != " + LOCATOR_ADDRESS_SIZE);
			this.address = address;
		}
		
		private static final byte[] IPv6Prefix =  { 0,0,0,0,0,0,0,0,0,0,(byte)0xff,(byte)0xff };
		public static Locator generateUDPLocator(InetSocketAddress socketAddress)
		{
			int kind;
			InetAddress address = socketAddress.getAddress();
			if  (address instanceof Inet4Address)
				kind = LOCATOR_KIND_UDPv4;
			else if (address instanceof Inet6Address)
				kind = LOCATOR_KIND_UDPv6;
			else
				throw new IllegalArgumentException("unsupported InetAddress type: " + address.getClass());
			
			byte[] byteAddress = new byte[LOCATOR_ADDRESS_SIZE];
			System.arraycopy(IPv6Prefix, 0, byteAddress, 0, IPv6Prefix.length);
			System.arraycopy(address.getAddress(), 0, byteAddress, LOCATOR_ADDRESS_SIZE - 4, 4);
			
			return new Locator(kind, socketAddress.getPort(), byteAddress);
		}

		public static final Locator LOCATOR_INVALID =
			new Locator(LOCATOR_KIND_INVALID, LOCATOR_PORT_INVALID, LOCATOR_ADDRESS_INVALID);
		
		public void serialize(ByteBuffer buffer)
		{
			buffer.putInt(kind);
			buffer.putInt(port);	// unsigned, there is no ports above 65535 limit
			buffer.put(address);	// fixed to 16-bytes
		}
	}
	
	/**
     * Communicates the state of the reader to the writer.
     * All sequence numbers up to the one prior to readerSNState.base
     * are confirmed as received by the reader.
     * The sequence numbers that appear in the set indicate missing sequence numbers on the reader side.
     * The ones that do not appear in the set are undetermined (could be received or not).
     * 
     * Limitation: 63-bit range of sequence numbers is enough for couple of decades,
     * therefore there is no need to handle wrap.
     */
    public static class SequenceNumberSet
    {
    	public static final long MAX_RANGE = 255;
    	
    	public long bitmapBase;
    	public final BitSet bitmap = new BitSet(256);
    	
    	public void reset(long bitmapBase)
    	{
    		bitmap.clear();
    		this.bitmapBase = bitmapBase;
    	}
    	
    	public void set(long seqNo)
    	{
    		long diff = seqNo - bitmapBase;
    		if (diff < 0 || diff > 255)
    			throw new IllegalArgumentException("!(0 <= (seqNo - bitmapBase) < 256)");
    		bitmap.set((int)diff);
    	}

    	// TODO error handling
    	public void serialize(ByteBuffer buffer)
		{
    		buffer.putLong(bitmapBase);
    		
    		int bitsInUse = bitmap.length();
		    // unsigned int, but limited to max 256
    		buffer.putInt(bitsInUse);

    		final int M = (bitsInUse + 31) / 32;
    		long[] bitArray = bitmap.getBitArray();
    		
		    ByteOrder endianess = buffer.order();
		    buffer.order(ByteOrder.BIG_ENDIAN);
    		
    		int l = 0;
    		for (; l < M/2; l++)
    			buffer.putLong(bitArray[l]);
    		
    		if ((M & 1) == 1)
    			buffer.putInt((int)(bitArray[l] & 0xFFFFFFFF));

    		buffer.order(endianess);
		}
    	
    	// TODO error handling
    	public boolean deserialize(ByteBuffer buffer)
    	{
		    bitmap.clear();
		    // set last bit to set wordsInUse to max
		    bitmap.set(255);
		    
		    bitmapBase = buffer.getLong();
		    
		    if (bitmapBase <= 0)
		    	return false;
		    
		    // unsigned int, but limited to max 256
		    int bitCount = buffer.getInt();
		    if (bitCount < 0 || bitCount > 256)
		    	return false;
		    
		    // int count
		    final int M = (bitCount + 31) / 32;
    		long[] bitArray = bitmap.getBitArray();

		    ByteOrder endianess = buffer.order();
		    buffer.order(ByteOrder.BIG_ENDIAN);

    		int l = 0;
    		for (; l < M/2; l++)
    			bitArray[l] = buffer.getLong();
    		
    		if ((M & 1) == 1)
    			bitArray[l] = buffer.getInt();
    		
    		buffer.order(endianess);
    		
    		// clear bit 255 yet not overriden
    		if (M < 7)
    			bitmap.clear(255);
    		
    		return true;
    	}

		@Override
		public String toString() {
			return "SequenceNumberSet [bitmapBase=" + bitmapBase + ", bitmap="
					+ bitmap + "]";
		}
    	
    }

    public static final void addMessageHeader(ByteBuffer buffer)
    {
	    // MessageHeader
    	buffer.putLong(HEADER_NO_GUID);
	    buffer.put(GUIDPrefix.GUIDPREFIX.value);
    }

    public static final void addSubmessageHeader(ByteBuffer buffer, 
    		byte submessageId, byte submessageFlags, int octetsToNextHeaderPos)
    {
    	buffer.put(submessageId);
	    // E = SubmessageHeader.flags & 0x01 (0 = big, 1 = little)
    	buffer.put(submessageFlags);
    	buffer.putShort((short)octetsToNextHeaderPos);
    }
    
}