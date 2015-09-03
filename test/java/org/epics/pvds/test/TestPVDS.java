package org.epics.pvds.test;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

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
		
		final int INT_COUNT = 2048*1024;
		ByteBuffer data = ByteBuffer.allocate(INT_COUNT*Integer.BYTES);
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
		    final RTPSWriter writer = processor.createWriter(0x12345678, maxMessageSize, messageQueueSize);
		    writerGUID = writer.getGUID();
		    System.out.println("Writer GUID: " + writerGUID);
		    writer.start();
		    
		    new Thread(() -> {
		    	try {
				    int packageCounter = -1;
				    while (true)
				    {
				    	data.putInt(0, ++packageCounter);
				    	data.flip();
				    	
				    	long seqNo = writer.send(data); 
						//System.out.println(packageCounter + " / sent as " + seqNo);
				    	if (!writer.waitUntilAcked(seqNo, Long.MAX_VALUE /*TIMEOUT_MS*/))
				    		System.out.println(packageCounter + " / no ACK received for " + seqNo);
				    	//else
				    		//System.out.println(packageCounter + " / OK for " + seqNo);
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
	    		if (args.length < 3)
	    			throw new RuntimeException("provide writer GUID as third argument");
	    		
	    		// parse args[2]
	    		writerGUID = GUID.parse(args[2]);
	    	}
	    	
		    System.out.println("Subscribing to writer GUID: " + writerGUID);

	    	int lastReceivedPacketCount = -1; int totalMissed = 0;
		    final RTPSReader reader = processor.createReader(0, writerGUID, maxMessageSize, messageQueueSize);
		    while (true)
		    {
	    		long t1 = System.currentTimeMillis();
	
		    	try (SharedBuffer sb = reader.waitForNewData(TIMEOUT_MS))
		    	{
		    		if (sb == null)
		    		{
		    			System.err.println("no data");
		    		}
		    		else
		    		{
		        		long t2 = System.currentTimeMillis();
						double bw = 8*((long)sb.getBuffer().remaining()*1000)/(double)(t2-t1)/1000/1000/1000;
						
		    			IntBuffer intBuffer = sb.getBuffer().asIntBuffer();
		    			int packetCount = intBuffer.get();
		    			int expectedValue = 1; boolean valid = true;
		    			while (valid && intBuffer.hasRemaining())
		    				valid = (intBuffer.get() == expectedValue++);
						
		    			if (lastReceivedPacketCount != -1 && (packetCount - lastReceivedPacketCount) > 1)
		    			{
		    				int missed = packetCount - lastReceivedPacketCount;
		    				totalMissed += missed;
		    				System.err.println("missed:" + missed);
		    			}
		    			lastReceivedPacketCount = packetCount;
		    				
		    			System.out.printf("packet %d data received: bw = %.3f Gbit/s, valid data: %b / totalMissed: %d\n", packetCount, bw, valid, totalMissed);

		    			if (!valid)
		    				System.exit(1);
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
