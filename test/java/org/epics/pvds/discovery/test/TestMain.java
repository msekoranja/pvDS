/**
 * 
 */
package org.epics.pvds.discovery.test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.epics.pvds.Protocol.EntityId;
import org.epics.pvds.Protocol.GUID;
import org.epics.pvds.Protocol.GUIDPrefix;
import org.epics.pvds.discovery.DiscoveryDataSet;
import org.epics.pvds.discovery.DiscoveryServiceImpl;
import org.epics.pvds.util.StringToByteArraySerializator;

/**
 * @author msekoranja
 *
 */
public class TestMain {
	
	// one per process
	public static final AtomicInteger participandId = new AtomicInteger();
	
	public static void main(String[] args)
	{
		final HashSet<String> entities = new HashSet<String>();
		for (int i = 0; i < 1000; i++)
			entities.add("test" + String.valueOf(i));
		
		DiscoveryDataSet<String> dataSet = new DiscoveryDataSet<String>()
		{
			@Override
			public Set<String> getEntities() {
				return entities;
			}

			@Override
			public boolean hasEntity(String entity) {
				return entities.contains(entities);
			}
		};
		
		//DiscoveryServiceImpl<String> ds = 
			new DiscoveryServiceImpl<String>(
					30*1000,
					1*1000,
					new GUID(new GUIDPrefix(), EntityId.generateParticipantEntityId(participandId.incrementAndGet())),
					dataSet,
					StringToByteArraySerializator.INSTANCE
				);
			
/*
	    // starts from 1
	    int changeCount = 1;
	    int entitiesCount = 1000;
	    BloomFilter<String> filter = new BloomFilter<String>(StringToByteArraySerializator.INSTANCE, 8, 1024);
	    for (int i = 0; i < entitiesCount; i++)
	    	filter.add(String.valueOf(i));
	    transmitter.addAnnounceSubmessage(changeCount, unicastEndpoint, entitiesCount, filter);
	    
	    ByteBuffer buffer = transmitter.getBuffer();
	    
	    HexDump.hexDump("announce", buffer.array(), 0, buffer.position());
	    
	    RTPSMessageReceiver rtpsReceiver = new RTPSMessageReceiver();
	    
	    buffer.flip();
	    boolean successfulyProcessed = rtpsReceiver.processMessage(buffer);
	    System.out.println("successfulyProcessed: " + successfulyProcessed);
 */
	}
}