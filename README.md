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
Current implementation of pvDS does not implement a discovery algorithm.

pvDS is a UDP based protocol. UDP itself is not a reliable protocol, pvDS adds [NACK](https://en.wikipedia.org/wiki/NAK_(protocol_message)) (negative acknowledgment) based algorithm that achieves reliability (can be enabled via QoS options). When message needs to be delivered to multiple instances multicast is used instead of unicast. This reduces network load.

pvDS participant is called a `RTPSPaticipant`. Each participant belongs to a domain. Participants from different domains are insulated from each other (cannot talk to each other). Within domain participant can belong to different groups. A group defines a multicast address within domain multicast address space. All members of one group will see all the multicast traffic sent by any participant belonging to the group. 

Each `RTPSPaticipant` communicate with more that one topic. A topic is identified with globally unique ID (GUID). The GUID is composed of the `RTPSPaticipant` GUID (as prefix) and topic ID (as postfix). If a participant wants to sent (push, publish) data to the topic it must create an `RTPSWriter` instance. And in order to receive (pull, subscribe) messages from the topic it needs to create an  `RTPSReader` instance. `RTPSWriter` instance determines whether the topic supports reliable QoS or not (i.e. whether the writer waits for all the readers to acknowldege the reception of the messages). A message is available for resend (in case one reader missed an UDP message) until it gets overriden by the newer message by the writer. A size of the (re)send buffer on the writer side is configurable. `RTPSReader` needs to know the GUID of the topic to subscribe to.

pvDS sends only array of bytes as data. This means that does not specifies the data serialization protocol. It is up to the programmer to take care of that or to develop a framework on top of the pvDS.

# Code Example

An example is located [here](../master/test/java/org/epics/pvds/test/example/counter). It consists of one writer and one (or as many as you like) readers. A writer periodically with 10Hz sends a message containing a free running 64-bit counter and a timestamp.

In order to run example, just open the project in your favourite IDE and run `CounterWriter` and `CounterReader` Java console applications. You can run multiple instance of `CounterReader` at once, but do NOT run multiple instances of `CounterWriter` - GUID is hardcoded (for simplicity) and running multiple instances of participants with the same GUID violates the pvDS GUID uniqueness requirement.

Run `gradlew eclipse` command to generate Eclipse project files.

#### [Constants.java](..//master/test/java/org/epics/pvds/test/example/counter/Constants.java)

Defines constants used by this example. 

#### [CounterData.java](../master/test/java/org/epics/pvds/test/example/counter/CounterData.java)

A POJO class that defined a message. It includes serialization and deserialization messages.

#### [CounterWriter.java](../master/test/java/org/epics/pvds/test/example/counter/CounterWriter.java)

An example of simple pvDS writer. Default implementation acs as reliable writer. Unreliable version of code is commented out.

#### [CounterReader.java](../master/test/java/org/epics/pvds/test/example/counter/CounterReader.java)

An example of simple pvDS reader. Default implementation acs as reliable and ordered reader. Unreliable version of code is commented out.
