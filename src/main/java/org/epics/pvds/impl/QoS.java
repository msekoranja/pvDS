package org.epics.pvds.impl;


public final class QoS {

	public interface QOS {};
	public interface ReaderQOS extends QOS {};
	public interface WriterQOS extends QOS {};
	
	// implies QOS_ORDERED
	public static final ReaderQOS QOS_RELIABLE = new ReaderQOS() {};

	public static final ReaderQOS QOS_ORDERED = new ReaderQOS() {};

	public static final ReaderQOS QOS_HB_DONT_IGNORE_BUFFERED = new ReaderQOS() {};

	public static class QOS_LIMIT_READERS implements WriterQOS {
		public final int limit;
		public QOS_LIMIT_READERS(int limit)
		{
			if (limit <= 0)
				throw new IllegalArgumentException("limit <= 0");
			this.limit = limit;
		}
	};

	// always send messages, even if there is no readers
	public static final WriterQOS QOS_ALWAYS_SEND = new WriterQOS() {};

	// used for tests to control packet loss
	public static class QOS_SEND_SEQNO_FILTER implements WriterQOS {
		
		public static interface SeqNoFilter {
			boolean checkSeqNo(long seqNo);
		}
		
		public final SeqNoFilter filter;
		
		public QOS_SEND_SEQNO_FILTER(SeqNoFilter filter)
		{
			if (filter == null)
				throw new IllegalArgumentException("filter == null");
			this.filter = filter;
		}
	};

	public static final WriterQOS[] DEFAULT_WRITER_QOS = null;
	public static final ReaderQOS[] DEFAULT_READER_QOS = null;

	
	public static final ReaderQOS[] RELIABLE_ORDERED_QOS = new ReaderQOS[] {
		QOS_RELIABLE, QOS_ORDERED
	};

	public static final ReaderQOS[] UNRELIABLE_UNORDERED_QOS = new ReaderQOS[] {
	};

	public static final ReaderQOS[] UNRELIABLE_ORDERED_QOS = new ReaderQOS[] {
		QOS_ORDERED
	};
}
