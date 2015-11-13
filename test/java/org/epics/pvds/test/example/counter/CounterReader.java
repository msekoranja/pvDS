package org.epics.pvds.test.example.counter;

import java.util.concurrent.TimeUnit;

import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.impl.QoS;
import org.epics.pvds.impl.RTPSParticipant;
import org.epics.pvds.impl.RTPSReader;
import org.epics.pvds.impl.RTPSReader.SharedBuffer;

public class CounterReader {

	public static void main(String[] args) throws Throwable {
		
		RTPSParticipant participant = new RTPSParticipant(Constants.LOCAL_MCAST_NIF, Constants.DOMAIN_ID, false);
		
		GUID writerGUID = Constants.WRITER_GUID; //GUID.parse(args[0]);
		RTPSReader reader = participant.createReader(
				0, writerGUID,
				CounterData.MESSAGE_SIZE, 2,		// TODO RELIABLE/ORDERED requires queueSize > 1
//				QoS.RELIABLE_ORDERED_QOS, null
				QoS.UNRELIABLE_UNORDERED_QOS, null
				);
		
		CounterData counterData = new CounterData();
		while (true)
		{
			try (SharedBuffer sharedBuffer = reader.read(Constants.READ_TIMEOUT_MS, TimeUnit.MILLISECONDS))
			{
				if (sharedBuffer == null)
					System.err.println("no data");
				else
				{
					counterData.deserialize(sharedBuffer.getBuffer());
					System.out.println(counterData);
				}
			}
			
		}
		
		//participant.close();
		
	}

}
