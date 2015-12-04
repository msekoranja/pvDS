package org.epics.pvds.impl.test;

import junit.framework.TestCase;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.impl.GUIDHolder;

public class GUIDHolderTest extends TestCase {

	private static final GUIDPrefix GUID_PREFIX =
			new GUIDPrefix(new byte[] { (byte)0, (byte)1, (byte)2, (byte)3, (byte)4,
										  (byte)5, (byte)6, (byte)7, (byte)8, (byte)9,
										  (byte)10, (byte)11 });
	private static final int ENTITY_ID = 0x11223380;
	
	public void testGUIDHolder() throws Throwable {
		GUID guid = new GUID(GUID_PREFIX, new Protocol.EntityId(ENTITY_ID));
		GUIDHolder gh = new GUIDHolder();
		GUIDHolder gh1 = new GUIDHolder(guid);
		GUIDHolder gh2 = new GUIDHolder(GUID_PREFIX.value, ENTITY_ID);
		assertTrue(gh1.equals(gh2));
		assertFalse(gh.equals(gh1));

		assertFalse(gh.equals(gh1));
		//assertFalse(gh.hashCode() != gh1.hashCode()); // theoretically true is allowed
		assertFalse(gh.toString().equals(gh1.toString()));
		gh.set(GUID_PREFIX.value, ENTITY_ID);
		assertTrue(gh.equals(gh1));
		
		assertEquals(gh, gh.clone());
		
		assertEquals(gh.hashCode(), gh1.hashCode());
		assertEquals(gh1.hashCode(), gh2.hashCode());
		
		String str = gh1.toString();
		assertNotNull(str);
		assertEquals(32, str.length());
		assertEquals(str, gh2.toString());
	}
	
	// move to separate class
	public void testGUIDStringAndParse()
	{
		GUID guid = new GUID(GUID_PREFIX, new Protocol.EntityId(ENTITY_ID));
		GUID guid2 = GUID.parse(guid.toString());
		assertEquals(guid, guid2);
	}

}
