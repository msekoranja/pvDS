package org.epics.pvds.test.example.counter;

import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;

public interface Constants {

	// IMPORTANT: there MUST be only one instance with this ID running
	public static final GUIDPrefix WRITER_GUID_PREFIX =
			new GUIDPrefix(new byte[] { (byte)0, (byte)1, (byte)2, (byte)3, (byte)4,
										  (byte)5, (byte)6, (byte)7, (byte)8, (byte)9,
										  (byte)10, (byte)11 });
	public static final int WRITER_ID = 128;
	
	public static final GUID WRITER_GUID = new GUID(WRITER_GUID_PREFIX, new EntityId(WRITER_ID));
	
	public static final String LOCAL_MCAST_NIF = null;
	public static final int DOMAIN_ID = 0;

	public static final long READ_TIMEOUT_MS = 10*1000;

	public static final long ACK_TIMEOUT_MS = 1000;
}
