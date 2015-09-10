package org.epics.pvds.impl;

public final class QoS {

	public interface QOS {};
	public interface ReaderQOS extends QOS {};
	public interface WriterQOS extends QOS {};
	
	// implies QOS_ORDERED
	public static ReaderQOS QOS_RELIABLE = new ReaderQOS() {};

	public static ReaderQOS QOS_ORDERED = new ReaderQOS() {};

	public static class QOS_LIMIT_RELIABLE_READERS implements WriterQOS {
		public int limit;
		public QOS_LIMIT_RELIABLE_READERS(int limit)
		{
			if (limit <= 0)
				throw new IllegalArgumentException("limit <= 0");
			this.limit = limit;
		}
	};
}
