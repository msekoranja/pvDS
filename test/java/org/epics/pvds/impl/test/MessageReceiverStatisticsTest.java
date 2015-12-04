package org.epics.pvds.impl.test;

import junit.framework.TestCase;

import org.epics.pvds.impl.MessageReceiverStatistics;

public class MessageReceiverStatisticsTest extends TestCase {

	public void testMessageReceiverStatistics() {
		MessageReceiverStatistics stats = new MessageReceiverStatistics();
		
		stats.validMessage = 12;
		
		assertNotNull(stats.toString());
		
		stats.reset();
		
		assertEquals(0,  stats.validMessage);
		// TODO add more
	}

}
