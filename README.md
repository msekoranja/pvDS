[![Build Status](https://travis-ci.org/msekoranja/pvDS.svg?branch=master)](https://travis-ci.org/msekoranja/pvDS)
[![Code Coverage](https://img.shields.io/codecov/c/github/msekoranja/pvDS/coverity_scan.svg)](https://codecov.io/github/msekoranja/pvDS?branch=master)

# pvDS
Process Variable Distribution System - high-throughput reliable multicast protocol

# Development
The project can be build via *gradle* by executing the provided wrapper scripts as follows:
 * Linux: `./gradlew build`
 * Windows: `gradlew.bat build`

There is no need to have *gradle* installed on your machine, the only prerequisite for building is a Java >= 8 installed.

__Note:__ The first time you execute this command the required jars for the build system will be automatically downloaded and the build will start afterwards. The next time you execute the command the build should be faster.

# Overview

pvDS protocol is based on OMG RTPS v2.1 specification. It includes minor changes to work better with large data-sets.
In addition it does not implement RTPS discovery protocol, which is a limiting factor in systems with large number of topics.

pvDS is a UDP based protocol. UDP itself is not a reliable protocol, pvDS adds [NACK](https://en.wikipedia.org/wiki/NAK_(protocol_message)) (negative acknowledgment) based algorithm that achieves reliability (can be enabled via QoS options). When message needs to be delivered to multiple instances multicast is used instead of unicast. This reduces network load.

pvDS participant is called a `RTPSPaticipant`. Each participant belongs to a domain. Participants from different domains are insulated from each other (cannot talk to each other). Within domain participant can belong to different groups. A group defines a multicast address within domain multicast address space. All members of one group will see all the multicast traffic sent by any participant belonging to the group. 

Each `RTPSPaticipant` communicate with more that one topic. A topic is identified with unique ID. If a participant wants to sent (push, publish) data to the topic it must create an `RTPSWriter` instance. And in order to receive (pull, subscribe) messages from the topic it needs to create an  `RTPSReader` instance. `RTPSWriter` instance determines whether the topic supports reliable QoS or not (i.e. whether the writer waits for all the readers to acknowldege the reception of the messages). A message is available for resend (in case one reader missed an UDP message) until it gets overriden by the newer message by the writer. A size of the (re)send buffer on the writer side is configurable.
