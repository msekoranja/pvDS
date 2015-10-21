package org.epics.pvds.impl.test;

import junit.framework.TestCase;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.impl.RTPSParticipant;

public class RTPSParticipantTest extends TestCase {

	public void testConstruction()
	{
		try
		{
			new RTPSParticipant(null, -1, true);
			fail("negative domainId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}
		
		try
		{
			new RTPSParticipant(null, Protocol.MAX_DOMAIN_ID + 1, true);
			fail("out-of-range domainId accepted");
		} 
		catch (IllegalArgumentException iae) {
			// OK
		}

		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, true);
				RTPSParticipant p2 = new RTPSParticipant(null, 0, true)) {
			assertNotNull(p1.getGUIDPrefix());
			assertNotNull(p2.getGUIDPrefix());
			assertSame(p1.getGUIDPrefix(), p1.getGUIDPrefix());
			assertSame(p2.getGUIDPrefix(), p2.getGUIDPrefix());
			assertFalse(p1.getGUIDPrefix().equals(p2.getGUIDPrefix()));
		}

	}

	public void testClose() {
		try (RTPSParticipant p1 = new RTPSParticipant(null, 0, false)) {
			p1.createWriter(0, 16, 1);
			p1.createWriter(1, 16, 1);
			p1.createWriter(2, 16, 1);
			GUID guid = new GUID(new GUIDPrefix(), new EntityId(0));
			p1.createReader(0, guid, 16, 1);
			p1.createReader(1, guid, 16, 1);
			p1.createReader(2, new GUID(new GUIDPrefix(), new EntityId(1)), 16, 1);
		}
	}
}
