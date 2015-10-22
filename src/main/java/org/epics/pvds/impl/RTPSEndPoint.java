package org.epics.pvds.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.logging.Logger;

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
	private static final Logger logger = Logger.getLogger(RTPSEndPoint.class.getName());

	/*
	protected final DatagramChannel discoveryMulticastChannel;
	protected final DatagramChannel discoveryUnicastChannel;
    protected InetAddress discoveryMulticastGroup;
    protected int discoveryMulticastPort;
	*/

	protected final DatagramChannel multicastChannel;
	protected final DatagramChannel unicastChannel;
    protected InetAddress multicastGroup;
    protected int multicastPort;
	
    protected final GUIDPrefix guidPrefix;
	protected final NetworkInterface nif;
	protected final int participantId;
    
	public RTPSEndPoint(GUIDPrefix guidPrefix, String multicastNIF, int domainId, boolean enableMulticastChannel)
	{
		try
		{
			if (guidPrefix == null)
				throw new IllegalArgumentException("guidPrefix == null");
			this.guidPrefix = guidPrefix;
			
			if (domainId < 0)
				throw new IllegalArgumentException("domainId < 0");

			if (domainId > Protocol.MAX_DOMAIN_ID)
				throw new IllegalArgumentException("domainId >= " + String.valueOf(Protocol.MAX_DOMAIN_ID));
	
			if (multicastNIF == null)
				nif = InetAddressUtil.getLoopbackNIF();
			else
				nif = NetworkInterface.getByName(multicastNIF);
	
			if (nif == null)
				throw new IOException("no network interface available");
			
			logger.config(() -> "pvDS multicast NIF: " + nif.getDisplayName());
			
			// TODO configurable IPv4 multicast prefix
			
			/*
	        discoveryMulticastGroup =
	        	InetAddress.getByName("239.255." + String.valueOf(domainId) + ".1");
	        discoveryMulticastPort = Protocol.PB + domainId * Protocol.DG + Protocol.d0;
	        
	        discoveryMulticastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
	        	.setOption(StandardSocketOptions.SO_REUSEADDR, true)
	        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
	        	.bind(new InetSocketAddress(discoveryMulticastPort));
	        
	        discoveryMulticastChannel.join(discoveryMulticastGroup, nif);
	        discoveryMulticastChannel.configureBlocking(false);
	
	        
	        discoveryUnicastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
	        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
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
	        	
		    discoveryUnicastChannel.configureBlocking(false);
		    */
		
			multicastGroup =
					InetAddress.getByName("239.255." + String.valueOf(domainId) + ".2");
			multicastPort = Protocol.PB + domainId * Protocol.DG + Protocol.d2;
	
			if (enableMulticastChannel)
			{
				multicastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
						.setOption(StandardSocketOptions.SO_REUSEADDR, true)
						.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
						.bind(new InetSocketAddress(multicastPort));
		
				multicastChannel.join(multicastGroup, nif);
				multicastChannel.configureBlocking(false);
			}
			else
			{
				multicastChannel = null;
			}
			
			unicastChannel = DatagramChannel
					.open(StandardProtocolFamily.INET)
					.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
					.setOption(StandardSocketOptions.SO_REUSEADDR, false);
	
			int unicastPort = 0;
			int partId;
			for (partId = 0; partId < Protocol.MAX_PARTICIPANT_ID; partId++)
			{
				unicastPort = Protocol.PB + domainId * Protocol.DG 
								+ partId * Protocol.PG + Protocol.d3;
				try {
					unicastChannel.bind(new InetSocketAddress(unicastPort));
					break;
				} catch (Throwable th) {
					// noop
				}
			}
	
			if (partId > Protocol.MAX_PARTICIPANT_ID)
				throw new RuntimeException("maximum number of participants on this host reached");
			this.participantId = partId;
	
			unicastChannel.configureBlocking(false);
			
			logger.config(() -> "pvDS multicast group: " + multicastGroup + ":" + multicastPort);
			final int funicastPort = unicastPort;
			logger.config(() -> "pvDS unicast port: " + funicastPort);
		    //logger.config(() -> "pvDS discovery multicast group: " + discoveryMulticastGroup + ":" + discoveryMulticastPort);
		    //logger.config(() -> "pvDS unicast port: " + unicastDiscoveryPort);
			logger.config(() -> "pvDS GUID prefix: " + Arrays.toString(guidPrefix.value));
			logger.config(() -> "pvDS started: domainId = " + domainId + ", participantId = " + participantId);
		} catch (IOException ioex) {
			throw new RuntimeException("Failed to instantiate participant: " + ioex.getMessage(), ioex);
		}
	}

	public NetworkInterface getMulticastNIF() {
		return nif;
	}

	public DatagramChannel getMulticastChannel() {
		return multicastChannel;
	}

	public DatagramChannel getUnicastChannel() {
		return unicastChannel;
	}

	public InetAddress getMulticastGroup() {
		return multicastGroup;
	}

	public int getMulticastPort() {
		return multicastPort;
	}

	public int getParticipantId() {
		return participantId;
	}
	
}