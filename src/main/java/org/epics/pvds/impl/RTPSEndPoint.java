package org.epics.pvds.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;

import org.epics.pvds.Protocol;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.util.InetAddressUtil;

/**
 * RTPS end-point instance (e.g. receiver, transmitter) base class.
 * The class itself is not thread-safe, i.e. processMessage() method should be called from only one thread. 
 * @author msekoranja
 */
public class RTPSEndPoint
{
	protected final DatagramChannel discoveryMulticastChannel;
	protected final DatagramChannel discoveryUnicastChannel;

	protected final NetworkInterface nif;
    protected InetAddress discoveryMulticastGroup;
    protected int discoveryMulticastPort;
    
    // TODO remove
    public DatagramChannel getDiscoveryMulticastChannel()
    {
    	return discoveryMulticastChannel;
    }
    
    // TODO remove
    public DatagramChannel getDiscoveryUnicastChannel()
    {
    	return discoveryUnicastChannel;
    }

    public NetworkInterface getMulticastNIF() {
		return nif;
	}

	public InetAddress getDiscoveryMulticastGroup() {
		return discoveryMulticastGroup;
	}

	public int getDiscoveryMulticastPort() {
		return discoveryMulticastPort;
	}

	public RTPSEndPoint(String multicastNIF, int domainId) throws Throwable
	{
		if (domainId > Protocol.MAX_DOMAIN_ID)
			throw new IllegalArgumentException("domainId >= " + String.valueOf(Protocol.MAX_DOMAIN_ID));

		if (multicastNIF == null)
			nif = InetAddressUtil.getLoopbackNIF();
		else
			nif = NetworkInterface.getByName(multicastNIF);

		if (nif == null)
			throw new IOException("no network interface available");
		
		System.out.println("NIF: " + nif.getDisplayName());
		
		
		// TODO configure IPv4 multicast prefix
        discoveryMulticastGroup =
        	InetAddress.getByName("239.255." + String.valueOf(domainId) + ".1");
        discoveryMulticastPort = Protocol.PB + domainId * Protocol.DG + Protocol.d0;
        
        discoveryMulticastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
        	.setOption(StandardSocketOptions.SO_REUSEADDR, true)
//	        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
        	.bind(new InetSocketAddress(discoveryMulticastPort));
        
        discoveryMulticastChannel.join(discoveryMulticastGroup, nif);
//	        discoveryMulticastChannel.configureBlocking(false);
	    discoveryMulticastChannel.configureBlocking(true);

        
        discoveryUnicastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
//	        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
    		.setOption(StandardSocketOptions.SO_REUSEADDR, false);
        
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
	    discoveryUnicastChannel.configureBlocking(true);
	    
	    // TODO use logging
	    System.out.println("pvDS started: domainId = " + domainId + ", participantId = " + participantId);
	    System.out.println("pvDS discovery multicast group: " + discoveryMulticastGroup + ":" + discoveryMulticastPort);
	    System.out.println("pvDS unicast port: " + unicastDiscoveryPort);
	    System.out.println("pvDS GUID prefix: " + Arrays.toString(GUIDPrefix.GUIDPREFIX.value));
	}
	
}