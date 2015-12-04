[![Build Status](https://travis-ci.org/msekoranja/pvDS.svg?branch=master)](https://travis-ci.org/msekoranja/pvDS)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/7195/badge.svg)](https://scan.coverity.com/projects/msekoranja-pvds)

# pvDS
Process Variable Distribution System - high-throughput reliable multicast protocol

# Development
The project can be build via *gradle* by executing the provided wrapper scripts as follows:
 * Linux: `./gradlew build`
 * Windows: `gradlew.bat build`

There is no need to have *gradle* installed on your machine, the only prerequisite for building is a Java >= 8 installed.

__Note:__ The first time you execute this command the required jars for the build system will be automatically downloaded and the build will start afterwards. The next time you execute the command the build should be faster.

