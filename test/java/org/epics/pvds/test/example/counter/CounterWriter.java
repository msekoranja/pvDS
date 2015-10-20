package org.epics.pvds.test.example.counter;

import java.nio.ByteBuffer;

import org.epics.pvds.impl.RTPSParticipant;
import org.epics.pvds.impl.RTPSWriter;

public class CounterWriter {

	public static void main(String[] args) throws Throwable {
		
		RTPSParticipant participant = new RTPSParticipant(Constants.LOCAL_MCAST_NIF, Constants.DOMAIN_ID, true);
		participant.start();
		
		RTPSWriter writer = participant.createWriter(0, CounterData.MESSAGE_SIZE, 1);
	    System.out.println("Writer GUID: " + writer.getGUID());
		writer.start();
		
		ByteBuffer buffer = ByteBuffer.allocate(CounterData.MESSAGE_SIZE);
		CounterData counterData = new CounterData();

		while (true)
		{
			buffer.clear();
			counterData.serialize(buffer);
			buffer.flip();

			writer.send(buffer);

			// 10Hz
			Thread.sleep(100);
			counterData.incrementAndUpdateTimestamp();
		}
		
		//writer.stop();
		//participant.stop();
		
	}

}
