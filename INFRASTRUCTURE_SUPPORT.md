Multicast group capacity (Jan 2010)
------------------------
Extreme's Summit x650 was the clear leader in multicast group capacity, successfully forwarding traffic to 6,000 groups. The Arista and HP switches were next, each forwarding traffic to 2,047 groups. Cisco's Nexus was a little behind them with 2,000 groups, while the Blade and Dell switches each supported 1,024 groups.

Maximum number of multicast groups a socket/node can subscribe to
------------------------
Many operating systems, including Linux, limit the number of multicast group memberships that a machine can belong to simultaneously. (A "multicast group membership" indicates that a machine is listening to messages for a specific multicast IP address. In other words, there is a limit on how many multicast IP addresses you can listen to.)

On Linux, in particular, the default limit is relatively small (only 20 on many standard kernels). However, this limit can be configured dynamically.

In addition to the operating system, there might be a physical or practical limit of the NIC that is being used. For example, for the Intel's Pro 10/100 cards the practical limit is 14 (according to docs http://www.intel.com/support/network/sb/cs-009951.htm).

You can check the multicast groups your machine currently belongs to by running "netstat -gn". You can ignore the IPv6 entries (unless you are using IPv6 multicast, in which case you can ignore IPv4). The entry for 224.0.0.1 will be set up internally by the network stack. You can verify that the other addresses match the multicast addresses you believe should be in use.
You can check the current maximum number of multicast group memberships that your Linux machine can belong to simultaneously by running "cat /proc/sys/net/ipv4/igmp_max_memberships". The default value is usually 20.
If you need to increase the limit, you can do so dynamically by using the /proc file system. For example, running "echo 40 > /proc/sys/net/ipv4/igmp_max_memberships" (as root) would set the limit to 40. To make the change permanent, modify net.ipv4.igmp_max_memberships in /etc/sysctl.conf.
You can reload the configuration file by using sysctl -p or by rebooting your system.

For Linux:

igmp_max_memberships - INTEGER
	Change the maximum number of multicast groups we can subscribe to.
	Default: 20

	Theoretical maximum value is bounded by having to send a membership
	report in a single datagram (i.e. the report can't span multiple
	datagrams, or risk confusing the switch and leaving groups you don't
	intend to).

	The number of supported groups 'M' is bounded by the number of group
	report entries you can fit into a single datagram of 65535 bytes.

	M = 65536-sizeof (ip header)/(sizeof(Group record))

	Group records are variable length, with a minimum of 12 bytes.
	So net.ipv4.igmp_max_memberships should not be set higher than:

	(65536-24) / 12 = 5459

	The value 5459 assumes no IP header options, so in practice
	this number may be lower.

	conf/interface/*  changes special settings per interface (where
	"interface" is the name of your network interface)

	conf/all/*	  is special, changes the settings for all interfaces

NOTE: a network interface (an entity with mac address) card joins the group (address, no port!), local OS stack does dispaching (+ port filtering)
