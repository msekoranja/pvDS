package org.epics.pvds.impl;

import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.util.CityHash64;

/**
 * CPU/memory optimized GUID holder, that allows
 * fast hash lookups and comparisons.
 * @author msekoranja
 */
public class GUIDHolder
{
	
	// optimized GUID (16-byte byte[] converted to 2 longs)
	long p1;
	long p2;
	
	public GUIDHolder()
	{
	}
	
	public GUIDHolder(GUID guid)
	{
		set(guid.prefix.value, guid.entityId.value);
	}

	public GUIDHolder(byte[] guidPrefix, int entityId)
	{
		set(guidPrefix, entityId);
	}

	public void set(byte[] guidPrefix, int entityId)
	{
		// gets p1 and p2 as little endian
		p1 = CityHash64.getLong(guidPrefix, 0);
		int ip2 = CityHash64.getInt(guidPrefix, 8);
		p2 = ((long)ip2 << 32) | entityId;
	}

	@Override
	public int hashCode() {
		return (int) (p1 ^ p2);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof GUIDHolder)
		{
			GUIDHolder o = (GUIDHolder)obj;
			return p1 == o.p1 && p2 == o.p2;
		}
		else
			return false;
	}

	@Override
	public Object clone() throws CloneNotSupportedException
	{
		GUIDHolder o = new GUIDHolder();
		o.p1 = p1;
		o.p2 = p2;
		return o;
	}

	@Override
	public String toString() {
		// make print compatible with GUID.toString()

		long rp1 = Long.reverseBytes(p1);
		long rp2 = Long.reverseBytes(p2);
		
		long gp1 = rp1; 
		long gp2 = rp2 << 32 | p2 & 0xFFFFFFFF;
		
		return String.format("%016X", gp1) + String.format("%016X", gp2);
	}
	
}