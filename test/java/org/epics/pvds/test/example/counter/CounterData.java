package org.epics.pvds.test.example.counter;

import java.nio.ByteBuffer;
import java.util.Date;

public class CounterData {

	public long count = 0;
	public long timestampMillis = System.currentTimeMillis();
	
	public static int MESSAGE_SIZE = Long.BYTES + Long.BYTES;
	
	public CounterData() {
	}

	public void incrementAndUpdateTimestamp()
	{
		count++;
		timestampMillis = System.currentTimeMillis();
	}
	
	public void serialize(ByteBuffer buffer)
	{
		buffer.putLong(count);
		buffer.putLong(timestampMillis);
	}

	public void deserialize(ByteBuffer buffer)
	{
		count = buffer.getLong();
		timestampMillis = buffer.getLong();
	}
	
	@Override
	public String toString()
	{
		return new Date(timestampMillis) + " " + count;
	}
}
