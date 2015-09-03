package org.epics.pvds.test;

import java.nio.ByteBuffer;

import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.impl.MessageReceiverStatistics;
import org.epics.pvds.impl.RTPSParticipant;
import org.epics.pvds.impl.RTPSReader;
import org.epics.pvds.impl.RTPSReader.SharedBuffer;
import org.epics.pvds.impl.RTPSWriter;

public class TestPVDS {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable {
		
		final int INT_COUNT = 1024;
		ByteBuffer data = ByteBuffer.allocate(1024*Integer.BYTES);
		for (int i = 0; i < INT_COUNT; i++)
			data.putInt(i);
		
		final int maxMessageSize = data.position();
		final int messageQueueSize = 3;
		
	    boolean isRx = false, isTx = false;
	    if (args.length < 2)
	    	isRx = isTx = true;
	    else if (args[1].equals("rx"))
	    	isRx = true;
	    else if (args[1].equals("tx"))
	    	isTx = true;
	    else
	    	throw new IllegalArgumentException("invalid mode");

		final int domainId = 0;
	    final String multicastNIF = (args.length > 0) ? args[0] : null;
	    
	    final RTPSParticipant processor = new RTPSParticipant(multicastNIF, domainId);
	    processor.start();
	    
	    final long TIMEOUT_MS = 3000;
	    
	    GUID writerGUID = null;
	    if (isTx)
	    {
		    final RTPSWriter writer = processor.createWriter(0, maxMessageSize, messageQueueSize);
		    writerGUID = writer.getGUID();
		    writer.start();
		    
		    new Thread(() -> {
		    	try {
				    int packageCounter = -1;
				    while (true)
				    {
				    	data.putInt(0, ++packageCounter);
				    	data.flip();
				    	
				    	long seqNo = writer.send(data); 
						System.out.println(packageCounter + " / sent as " + seqNo);
				    	if (!writer.waitUntilReceived(seqNo, TIMEOUT_MS))
				    		System.out.println(packageCounter + " / no ACK received for " + seqNo);
				    	else
				    		System.out.println(packageCounter + " / OK for " + seqNo);
				    }
		    	} catch (Throwable th) {
		    		th.printStackTrace();
		    	}
		    }, "tx").start();
		}

	    
	    if (isRx)
	    {
	    	if (writerGUID == null)
	    	{
	    		// parse args[2]
	    		throw new RuntimeException("no writer GUID parsing implemented");
	    	}
		    final RTPSReader reader = processor.createReader(0, writerGUID, maxMessageSize, messageQueueSize);
		    while (true)
		    {
	    		long t1 = System.currentTimeMillis();
	
		    	try (SharedBuffer sb = reader.waitForNewData(TIMEOUT_MS))
		    	{
		    		if (sb == null)
		    			System.out.println("no data");
		    		else
		    		{
		        		long t2 = System.currentTimeMillis();
						double bw = 8*(sb.getBuffer().remaining()*1000)/(double)(t2-t1)/1000/1000/1000;
		    			System.out.printf("packet %d data received: bw = .3f Gbit/s\n", sb.getBuffer().getInt(0), bw);
		    		}
	
		    		MessageReceiverStatistics stats = processor.getStatistics();
		    		if (stats.missedSN > 0 ||
		    			stats.lostSN > 0 ||
						stats.ignoredSN > 0)
		    			System.out.println(stats);
		    		stats.reset();
		    	}
		    }
	    }

	    
	}
}



// TODO recovery resendProcess and thready safety!!!!!
// TODO send ackNack more often (before)

// TODO periodic send of ackNack (or heartbeat with final)
