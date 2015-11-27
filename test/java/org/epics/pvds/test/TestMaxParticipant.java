package org.epics.pvds.test;

import java.util.ArrayList;

import org.epics.pvds.Protocol;
import org.epics.pvds.impl.RTPSParticipant;

public class TestMaxParticipant {

	public static void main(String[] args) throws Throwable {
		
		final int maxParticipants = Protocol.MAX_PARTICIPANT_ID + 1;
		ArrayList<RTPSParticipant> list = new ArrayList<RTPSParticipant>(maxParticipants);
		int i = 0;
		Throwable lastException = null;
		try {
			while (i < maxParticipants)
			{
				// create participants with different group 
				list.add(new RTPSParticipant(null, 0, i, false));
				i++;
			}
		} catch (Throwable th) {
			th.fillInStackTrace();
			lastException = th;
		}
		System.out.println("Sucessfully created " + i + " (out of " + maxParticipants + ") participants/groups.");
		if (lastException != null)
		{
			System.out.println("Last try failed with exception:\n");
			lastException.printStackTrace(System.out);
		}
		
		list.forEach((p) -> p.close());
	}

}
