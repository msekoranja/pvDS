package org.epics.pvds.impl;

import java.util.Arrays;

public final class MessageReceiverStatistics {
	public long[] submessageType = new long[255];
	public long messageToSmall;
	public long nonRTPSMessage;
	public long versionMismatch;
	public long vendorMismatch;

	public long invalidSubmessageSize;
	public long submesssageAlignmentMismatch;
    
	public long unknownSubmessage;
	public long validMessage;
    
	public long invalidMessage;
    
	public long lastSeqNo;
	public long missedSN;
	public long receivedSN;
	public long lostSN;
	public long recoveredSN;
	public long ignoredSN;
    
	public long noBuffers;
	public long fragmentTooLarge;
    
    public void reset()
    {
    	Arrays.fill(submessageType, 0);
		messageToSmall = 0;
		nonRTPSMessage = 0;
	    versionMismatch = 0;
	    vendorMismatch = 0;

	    invalidSubmessageSize = 0;
	    submesssageAlignmentMismatch = 0;
	    
	    unknownSubmessage = 0;
	    validMessage = 0;
	    
	    invalidMessage = 0;
	    
	    lastSeqNo = 0;
	    missedSN = 0;
	    receivedSN = 0;
	    lostSN = 0;
	    recoveredSN = 0;
	    ignoredSN = 0;
	    
	    noBuffers = 0;
	    fragmentTooLarge = 0;
    }

	@Override
	public String toString() {
		return "MessageReceiverStatistics [messageToSmall="
				+ messageToSmall + ", nonRTPSMessage=" + nonRTPSMessage
				+ ", versionMismatch=" + versionMismatch + ", vendorMismatch="
				+ vendorMismatch + ", invalidSubmessageSize="
				+ invalidSubmessageSize + ", submesssageAlignmentMismatch="
				+ submesssageAlignmentMismatch + ", unknownSubmessage="
				+ unknownSubmessage + ", validMessage=" + validMessage
				+ ", invalidMessage=" + invalidMessage + ", lastSeqNo="
				+ lastSeqNo + ", missedSN=" + missedSN + ", receivedSN="
				+ receivedSN + ", lostSN=" + lostSN + ", recoveredSN="
				+ recoveredSN + ", ignoredSN=" + ignoredSN + ", noBuffers="
				+ noBuffers + ", fragmentTooLarge=" + fragmentTooLarge + "]";
	}
    
}