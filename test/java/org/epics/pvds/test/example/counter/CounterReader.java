package org.epics.pvds.test.example.counter;

import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.impl.QoS;
import org.epics.pvds.impl.RTPSParticipant;
import org.epics.pvds.impl.RTPSReader;
import org.epics.pvds.impl.RTPSReader.SharedBuffer;

public class CounterReader {

	public static void main(String[] args) throws Throwable {
		
		RTPSParticipant participant = new RTPSParticipant(Constants.LOCAL_MCAST_NIF, Constants.DOMAIN_ID, false);
		participant.start();
		
		GUID writerGUID = Constants.WRITER_GUID; //GUID.parse(args[0]);
		RTPSReader reader = participant.createReader(
				0, writerGUID,
				CounterData.MESSAGE_SIZE, 1,
				QoS.UNRELIABLE_UNORDERED_QOS, null
				);
		
		CounterData counterData = new CounterData();
		while (true)
		{
			try (SharedBuffer sharedBuffer = reader.waitForNewData(Constants.READ_TIMEOUT_MS))
			{
				if (sharedBuffer == null)
					System.err.println("timeout");
				else
				{
					counterData.deserialize(sharedBuffer.getBuffer());
					System.out.println(counterData);
				}
			}
			
		}
		
		//participant.stop();
		
	}

}
