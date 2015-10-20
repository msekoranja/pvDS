package org.epics.pvds.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import org.epics.pvds.Protocol;
import org.epics.pvds.util.InetAddressUtil;

public class TestBandwidth {

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

		NetworkInterface nif = NetworkInterface.getByName(args[0]);

		// fallback to loopback
		if (nif == null)
			nif = InetAddressUtil.getLoopbackNIF();
		
		if (nif == null)
			throw new IOException("no network interface available");
		
		System.out.println("NIF: " + nif.getDisplayName());

		int domainId = 0;
		if (domainId > Protocol.MAX_DOMAIN_ID)
			throw new IllegalArgumentException("domainId >= " + String.valueOf(Protocol.MAX_DOMAIN_ID));
		
        InetAddress discoveryMulticastGroup =
        	InetAddress.getByName("239.255." + String.valueOf(domainId) + ".1");
        int discoveryMulticastPort = Protocol.PB + domainId * Protocol.DG + Protocol.d0;
        
        final DatagramChannel discoveryMulticastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
        	.setOption(StandardSocketOptions.SO_REUSEADDR, true)
//        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
        	.bind(new InetSocketAddress(discoveryMulticastPort));
        
        discoveryMulticastChannel.join(discoveryMulticastGroup, nif);
//        discoveryMulticastChannel.configureBlocking(false);

        
        final DatagramChannel discoveryUnicastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
//        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
    		.setOption(StandardSocketOptions.SO_REUSEADDR, false);
        
        // TODO do we really need to specify unicast discovery port?
        int participantId;
        int unicastDiscoveryPort = 0;
        for (participantId = 0; participantId < Protocol.MAX_PARTICIPANT_ID; participantId++)
        {
        	unicastDiscoveryPort = Protocol.PB + domainId * Protocol.DG + participantId * Protocol.PG + Protocol.d1;
        	try {
        		discoveryUnicastChannel.bind(new InetSocketAddress(unicastDiscoveryPort));
        		break;
        	} catch (Throwable th) {
        		// noop
        	}
        }
        
        if (participantId > Protocol.MAX_PARTICIPANT_ID)
        	throw new RuntimeException("maximum number of participants on this host reached");
        	
    
	    //discoveryUnicastChannel.configureBlocking(false);
	    
	    System.out.println("pvDS started: domainId = " + domainId + ", participantId = " + participantId);
	    System.out.println("pvDS discovery multicast group: " + discoveryMulticastGroup + ":" + discoveryMulticastPort);
	    System.out.println("pvDS unicast port: " + unicastDiscoveryPort);
	    //System.out.println("pvDS GUID prefix: " + Arrays.toString(guidPrefix.value));


	    // NOTE: Giga means 10^9 (not 1024^3)
	    double udpTxRateGbitPerSec = 0.96;
	    final int MAX_PACKET_SIZE_BYTES = 8000;
	    long delay_ns = (long)(MAX_PACKET_SIZE_BYTES * 8 / udpTxRateGbitPerSec);
	    
	    discoveryMulticastChannel.configureBlocking(true);
	    /*
	    new Thread(new Runnable() {
	    	public void run() {
	    	    ByteBuffer t = ByteBuffer.allocate(64000);
	    		try
	    		{
	    			long lastPacketId = 0; long missed = 0;
	    			long rx = 0;
	    			long startTime = System.nanoTime();
	    			while (true)
	    			{
			    	    t.clear();
			    	    discoveryMulticastChannel.receive(t);
			    	    rx += t.position();
			    	    t.flip();
			    	    long packetId = t.getLong();
			    	    if ((lastPacketId + 1) != packetId)
			    	    	missed++;
			    	    lastPacketId = packetId;

			    	    if (lastPacketId == 1)
			    	    {
			    	    	// not perfect, but OK
			    			startTime = System.nanoTime();
			    			missed = 0; rx = 0;
			    	    	System.out.println("started.");
			    	    }
			    	    else if (lastPacketId % 5000 == 0)
			    	    {
			    	    	long et = System.nanoTime();
			    	    	long th = (rx * 1000 * 8) / (et - startTime);
			    	    	System.out.println("missed: " + missed + ", " + th + "Mbit/s" + ", " + ((100*missed)/lastPacketId) + "%");
			    	    }
	    			}
	    		}
	    		catch (Throwable th) 
	    		{
	    			th.printStackTrace();
	    		}
	    	}
	    }, "rx").start();
	    */
	    
	    ByteBuffer b = ByteBuffer.allocate(MAX_PACKET_SIZE_BYTES);

	    // sender setup
	    discoveryUnicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
	    discoveryUnicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, nif);
	    InetSocketAddress multicastAddress = new InetSocketAddress(discoveryMulticastGroup, discoveryMulticastPort);

	    System.out.println("rate-limited delay [ns]: " + delay_ns); 
	    
	    // TODO we need at least 1us precision

	    // 1Gig bytes
	    long packets = 10*1000000000L / MAX_PACKET_SIZE_BYTES; long c = 0;
	    long startTime = System.nanoTime();

	    long lastSendTime = 0;
	    while (true)
		{
			// wait for more data here... 
			if (c++ == packets)
				break;
			b.clear();
			b.putLong(c);
			b.position(b.capacity());
			b.flip();
			
			long endTime = lastSendTime + delay_ns;
			while (endTime - System.nanoTime() > 0);

		    lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?

		    while (b.hasRemaining())
		    	discoveryUnicastChannel.send(b, multicastAddress);
		    
	    }
	    
	    long timeSpent = System.nanoTime() - startTime;
	    System.out.println(packets + " done in " + timeSpent/1000/1000 + "ms");
	    
	}

}
