package org.epics.pvds.test.example.counter;

import java.nio.ByteBuffer;

import org.epics.pvds.impl.RTPSParticipant;
import org.epics.pvds.impl.RTPSWriter;

public class CounterWriter {

	public static void main(String[] args) throws Throwable {
		
		RTPSParticipant participant = new RTPSParticipant(Constants.WRITER_GUID_PREFIX, Constants.LOCAL_MCAST_NIF, Constants.DOMAIN_ID, true);
		participant.start();
		
		RTPSWriter writer = participant.createWriter(Constants.WRITER_ID, CounterData.MESSAGE_SIZE, 1);
	    System.out.println("Writer GUID: " + writer.getGUID());
		writer.start();
		
		ByteBuffer buffer = ByteBuffer.allocate(CounterData.MESSAGE_SIZE);
		CounterData counterData = new CounterData();

		while (true)
		{
			buffer.clear();
			counterData.serialize(buffer);
			buffer.flip();

			// UNRELIABLE writer
			//writer.send(buffer);
			
			// RELIABLE writer
			if (!writer.send(buffer, Constants.ACK_TIMEOUT_MS))
				System.err.println(counterData.count + " not ACKed by all reliable readers");
			
			// 10Hz
			Thread.sleep(100);
			counterData.incrementAndUpdateTimestamp();
		}
		
		//writer.stop();
		//participant.stop();
		
	}

}
