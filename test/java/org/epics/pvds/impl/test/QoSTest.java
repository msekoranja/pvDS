package org.epics.pvds.impl.test;

import org.epics.pvds.impl.QoS;

import junit.framework.TestCase;

public class QoSTest extends TestCase {

	public void test_QOS_LIMIT_READERS() {
		new QoS.QOS_LIMIT_READERS(1);
		
		try
		{
			new QoS.QOS_LIMIT_READERS(-1);
			fail("negative limit accepted");
		}
		catch (IllegalArgumentException iae) {
			// OK
		}
	}
	
	public void test_QOS_SEND_SEQNO_FILTER() {
		try
		{
			new QoS.QOS_SEND_SEQNO_FILTER(null);
			fail("null filter accepted");
		}
		catch (IllegalArgumentException iae) {
			// OK
		}
	}
}
