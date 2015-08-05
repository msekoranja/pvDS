package org.epics.pvds.util;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * <code>InetAddress</code> utility methods.
 */
public class InetAddressUtil {

	/**
	 * Get a loopback NIF.
	 * @return a loopback NIF, <code>null</code> if not found.
	 */
	// TODO support case with multiple loopback NIFs
	public static NetworkInterface getLoopbackNIF() {

		Enumeration<NetworkInterface> nets;
		try {
			nets = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException se) {
			return null;
		}

		while (nets.hasMoreElements())
		{
			NetworkInterface net = nets.nextElement();
			try
			{
				if (net.isUp() && net.isLoopback())
					return net;
			} catch (Throwable th) {
				// some methods throw exceptions, some return null (and they shouldn't)
				// noop, skip that interface
			}
		}
		
		return null;
	}

}
