package org.epics.pvds.test;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.logging.Logger;

import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.ByteArrayData;
import org.epics.pvdata.pv.PVByteArray;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Structure;
import org.epics.pvds.impl.PVDataSerialization;
import org.epics.pvds.impl.RTPSMessageReceiver;
import org.epics.pvds.impl.RTPSMessageTransmitter;

public class TestPVDS {

	
	
	static Logger log = Logger.getGlobal();

	@SuppressWarnings("unused")
	private final static Structure simpleStructure =
		FieldFactory.getFieldCreate().createFieldBuilder().
			add("value", ScalarType.pvInt).
			createStructure();

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable {
		
        //ExecutorService actorsThreadPool = Executors.newCachedThreadPool();
        //Actors actors = new Actors(actorsThreadPool);

        //final PollerImpl reactor = new PollerImpl();
        //reactor.start();
        //actorsThreadPool.execute(reactor);

        //ActorThread actorThread = actors.startActorThread();

		/*
	    PVStructure data = PVDataFactory.getPVDataCreate().createPVStructure(simpleStructure);
	    data.getSubField(PVInt.class, "value").put(0x12345678);
*/
	    
	    PVByteArray data = (PVByteArray)PVDataFactory.getPVDataCreate().createPVScalarArray(ScalarType.pvByte);
	    data.setLength(1000*1000*10);
		
	    boolean isRx = false, isTx = false;
	    if (args.length < 2)
	    	isRx = isTx = true;
	    else if (args[1].equals("rx"))
	    	isRx = true;
	    else if (args[1].equals("tx"))
	    	isTx = true;
	    else
	    	throw new IllegalArgumentException("invalid mode");
	    
		final RTPSMessageReceiver rtpsReceiver = isRx ? 
				new RTPSMessageReceiver(args.length > 0 ? args[0] : null, 0, 0x87654321) : null;
		if (isRx)
		{
			final DatagramChannel discoveryMulticastChannel = rtpsReceiver.getDiscoveryMulticastChannel();
			
			
		    new Thread(new Runnable() {
		    	public void run() {

		    		try
		    		{
			    		discoveryMulticastChannel.configureBlocking(false);

			    		Selector selector = Selector.open();
			    		discoveryMulticastChannel.register(selector, SelectionKey.OP_READ);
			    		
			    	    ByteBuffer rxBuffer = ByteBuffer.allocate(64000);
			    		try
			    		{
			    			while (true)
			    			{
			    				// TODO let decide on timeout, e.g. rtpsReceiver.waitTime();
			    				int keys = selector.select(10);
			    				if (keys == 0)
			    				{
			    					rtpsReceiver.noData();
			    				}
			    				else
			    				{
			    					// ACK all
			    					selector.selectedKeys().clear();
			    					
				    				rxBuffer.clear();
						    	    SocketAddress receivedFrom = discoveryMulticastChannel.receive(rxBuffer);
						    	    rxBuffer.flip();
						    	    rtpsReceiver.processMessage(receivedFrom, rxBuffer);
			    				}
			    			}
			    		}
			    		catch (Throwable th) 
			    		{
			    			th.printStackTrace();
			    		}
		    		} catch (Throwable th) {
		    			th.printStackTrace();
		    		}
		    	}
		    }, "rx-multicast-thread").start();
		
		    // TODO
		    /* commented since this will case 2 threads calling rtpsReceiver.processMessage (not thread safe!)
			final DatagramChannel discoveryUnicastChannel = rtpsReceiver.getDiscoveryUnicastChannel();
		    new Thread(new Runnable() {
		    	public void run() {
		    	    ByteBuffer rxBuffer = ByteBuffer.allocate(64000);
		    		try
		    		{
		    			while (true)
		    			{
		    				rxBuffer.clear();
				    	    SocketAddress receivedFrom = discoveryUnicastChannel.receive(rxBuffer);
				    	    rxBuffer.flip();
				    	    rtpsReceiver.processMessage(receivedFrom, rxBuffer);
		    			}
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "rx-unicast-thread").start();
			*/
		    Thread.sleep(1000);
		}
		
		
	    final RTPSMessageTransmitter tx = isTx ? 
	    		new RTPSMessageTransmitter(args.length > 0 ? args[0] : null, 0, 0x12345678) : null;

	    
	    if (isTx)
	    {
	
			final DatagramChannel txDiscoveryUnicastChannel = tx.getDiscoveryUnicastChannel();
		    new Thread(new Runnable() {
		    	public void run() {
		    	    ByteBuffer rxBuffer = ByteBuffer.allocate(64000);
		    		try
		    		{
		    			while (true)
		    			{
		    				rxBuffer.clear();
				    	    SocketAddress receivedFrom = txDiscoveryUnicastChannel.receive(rxBuffer);
				    	    rxBuffer.flip();
				    	    tx.processMessage(receivedFrom, rxBuffer);
		    			}
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "tx-rx-thread").start();
	
		    
		    
		    
		    
		    new Thread(new Runnable() {
		    	public void run() {
		    		try
		    		{
			    		tx.sendProcess();
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "tx-thread").start();
	
		    Thread.sleep(1000);
	    }
	    
	    /*
	    // test
	    tx.takeFreeBuffer();
	    tx.addMessageHeader(tx.getBuffer());
	    tx.addHeartbeatSubmessage(tx.getBuffer(), 1, 2);
	    
	    SequenceNumberSet sns = new SequenceNumberSet();
	    sns.reset(1234);
	    sns.set(1236);
	    sns.set(1237);
	    //sns.set(1234+254);
	    //sns.set(1234+255);
	    tx.addAckNackSubmessage(tx.getBuffer(), sns);
	    
	    tx.sendBuffer();
	    */
	    final long TIMEOUT = 3000;
	    
	    if (isRx && isTx)
	    {
	    	byte lastv = -1;
		    long ss = PVDataSerialization.getSerializationSize(data);
	    	byte[] countArray = new byte[1]; byte count = 0;
		    ByteArrayData bad = new ByteArrayData();
		    while(true)
			{
					long t1 = System.currentTimeMillis();
			    	countArray[0] = count++;
			    	data.put(0, 1, countArray, 0);
				    tx.send(data);
				    PVField rdata = rtpsReceiver.waitForNewData(data, TIMEOUT);
					long t2 = System.currentTimeMillis();
					double bw = 8*(ss*1000)/(double)(t2-t1)/1000/1000/1000;
					//System.out.println(bw + " Gbit/s");
		    		byte v = 0;
		    		if (rdata != null)
		    		{
		    			((PVByteArray)rdata).get(0, 1, bad);
		    			v = bad.data[0];
		    			
		    			if ((byte)(lastv + 1) != v)
		    			{
		    				System.out.println(lastv + " +1 (" + (lastv+1) + ") != " + v);
		    				System.exit(1);
		    			}
		    			lastv = v;
		    				
		    		}
					System.out.printf("[%d] %.3f %d\n", v, rdata == null ? -1 : bw, rtpsReceiver.getStatistics().missedSN);
					if (rtpsReceiver.getStatistics().missedSN > 0 ||
							rtpsReceiver.getStatistics().lostSN > 0 ||
							rtpsReceiver.getStatistics().ignoredSN > 0 )
						System.out.println(rtpsReceiver.getStatistics());

					
					if (rdata == null)
					{
						System.err.println("message lost");
		    			System.exit(1);
					}
					
					rtpsReceiver.getStatistics().reset();
			}
	    }
	    else if (isRx)
	    {
		    long ss = PVDataSerialization.getSerializationSize(data);
		    ByteArrayData bad = new ByteArrayData();
		    byte prevv = -1; int missedMessage = 0;
		    while(true)
		    {
		    	try {
		    		long t1 = System.currentTimeMillis();

		    		// tx send
		    		
		    	    PVField rdata = rtpsReceiver.waitForNewData(data, TIMEOUT);
		    		long t2 = System.currentTimeMillis();
		   //System.out.println(rtpsReceiver.getStatistics());
		    		double bw = 8*(ss*1000)/(double)(t2-t1)/1000/1000/1000;
		    		//System.out.println(bw + " Gbit/s");
		    		byte v = 0;
		    		if (rdata != null)
		    		{
		    			((PVByteArray)rdata).get(0, 1, bad);
		    			v = bad.data[0];
		    			if ((byte)(prevv + 1) != v) {
		    				System.out.println(prevv + " +1 (" + (prevv+1) + ") != " + v);
		    				missedMessage++;
		    			}
		    			prevv = v;

		    		}
		    		if (rdata != null)
		    			System.out.printf("[%d] %.3f %d, missed messages: %d\n", v, rdata == null ? 0 : bw, rtpsReceiver.getStatistics().missedSN, missedMessage);
					if (rdata == null || rtpsReceiver.getStatistics().missedSN > 0 ||
							rtpsReceiver.getStatistics().lostSN > 0 ||
							rtpsReceiver.getStatistics().ignoredSN > 0 )
						System.out.println(rtpsReceiver.getStatistics());

					
					if (rdata == null)
					{
						System.err.println("message lost");
		    			System.exit(1);
					}
					
		    		rtpsReceiver.getStatistics().reset();
		    	} catch (Throwable th) {
		    		th.printStackTrace();
		    		System.out.println(rtpsReceiver.getStatistics());
		    		System.exit(1);
		    	}
		    }
	    	
	    }
	    else if (isTx)
	    {
	    	//long sleepTime = Long.valueOf(System.getProperty("DELAY", "0"));
	    	int clients = Integer.valueOf(System.getProperty("CLIENTS", "1"));
	    	byte[] countArray = new byte[1]; byte count = 0;
		    while(true)
		    {
		    	countArray[0] = count++;
		    	data.put(0, 1, countArray, 0);

		    	tx.send(data);
		    	if (tx.waitUntilReceived(clients, TIMEOUT) != clients);		// TODO what if ACK is lost (it happens!!!)
		    		System.out.println(System.currentTimeMillis() + " / no ACK received");
		    	//// 0.1GB/sec, 10MB...1/10s
		    	//if (sleepTime > 0)
		    	//	Thread.sleep(sleepTime);
		    }
	    }
	    
	    
	    System.out.println("done.");
	    
	    /*
	    
	    long startTime = System.nanoTime();
	    
	    long timeSpent = System.nanoTime() - startTime;
	    System.out.println(" done in " + timeSpent/1000/1000 + "ms");
	    */
	    
	}

}



// TODO recovery resendProcess and thready safety!!!!!
// TODO send ackNack more often (before)

// TODO periodic send of ackNack (or heartbeat with final)
