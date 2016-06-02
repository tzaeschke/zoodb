
ZooDB
=====

<a href="http://www.zoodb.org.net">
<img src="https://github.com/tzaeschke/zoodb/blob/master/doc/images/logo_510412_web.png" alt="ZooDB logo" align="right" />
</a>

[![Build Status](https://travis-ci.org/tzaeschke/zoodb.svg?branch=master)](https://travis-ci.org/tzaeschke/zoodb)


ZooDB is an object oriented database based on the JDO 3.0 standard.
It is written by Tilmann Zaeschke.
It is licensed under GPLv3 (GNU Public License), see file COPYING.

ZooDB is also available via maven:

```
<dependency>
    <groupId>org.zoodb</groupId>
    <artifactId>zoodb</artifactId>
    <version>0.4.7</version>
</dependency>
```

Bug Bounty
==========
Starting June 11 2016, until June 30 2016, there is a bug bounty for severe bugs concerning indexing and database consistency.

Bounty:
- The bounty is 0.1BTC (or 50CHF cash when collected from my office). 
- The total maximum number of rewarded bugs is 10.
- Bug hunt starts at June 11 2016, with the exception of the three people who first mentioned this to me (you know who you are), they can submit bugs right away.
- At my discretion I can choose to accept bugs as valid/applicable even if they do not strictly adhere to all rules.
- Acceptance of bugs to qualify for the bounty is at my discretion.

Requirements:
- The bug must be reproducible with a small program which needs to be submitted to me (per email or as GitHub issue on the ZooDB project) as part of the bug report.
- The bug must be reproducible by me (I will try hard and contact you if I can't reproduce it) with the latest version of the master branch (at time of submission).
- JVM crashes do not count (they are JVM bugs)
- There is no open bug description that documents the same problem (i.e. you should only submit your bug if there  is no open bug report for the same/similar problem)
- Bugs must be of category 1 or 2: 
  - Category 1 bugs: The bugs must reproduce a serious problem with indexing (index corruption or unreasonable behaviour) during index creation, usage or deletion.
  - Category 2 bugs: The bugs must cause database corruption to a point where ZooDB cannot recover by simply restarting ZooDB. Corruption of the user-domain model due to incorrect domain code (or code that relies on unsupported functionality in ZooDB) does _not_ count. 



Current Status
==============
Under development, but already in use by some university projects.


Current Features
================
- Works as normal database or as in-memory database.
- Fast (4x faster than db4o using db4o's PolePosition benchmark suite).
- Reasonably scalable, has been used successfully with 60,000,000+ objects in a 30+ GB database.
- Maximum object count: 2^63.
- Maximum database size depends on (configurable) cluster size: 2^31 * CLUSTER_SIZE. With default cluster size: 2^31 * 4KB = 8TB.
- Crash-recovery/immunity (dual flush, no log-file required).
- Standard stuff: commit/rollback, query, indexing, lazy-loading, transitive persistence & updates (persistence by reachability), automatic schema definition, embedded object support (second class objects).
- Queries support standard operators, indexing, path queries, parameters, aggregation (avg, max, min), projection, uniqueness, ORDER BY, setting result classes (partial), methods (partial).
- Multi-user/-session capability (optimistic TX), but currently not terribly efficient.
- Thread-safe.
- XML export/import (currently only binary attributes).
- Some examples are available in the 'examples' folder.
- Open source (GPLv3).

Note that some features may be available in the latest snapshot only.

Current Limitations
===================
- Schema evolution is ~90% complete (updating OIDs is not properly supported, no low level queries).
  --> Queries don't work with lazy evolution.
- No backup (except copying the DB file).
- Single process usage only (No stand-alone server).
- JDO only partially supported:
  - Some query features not supported: group by, range, variables, imports, setting result classes (partial).
  - No XML config or Annotations; configuration only via Java API.
  - Manual enhancement of classes required (insert activateRead()/activateWrite() & extend provided super-class).
- Little documentation (some example code), but follows JDO 3.0 spec.


Dependencies
============
* JDO 3.0 (Java Data Objects): 
  - URL: https://db.apache.org/jdo/
  - JAR: jdo2-api-3.0.jar
* JTA (Java Transaction API):
  - URL: http://java.sun.com/products/jta/
  - JAR: jta.jar
* JUnit (currently use 4.8.1, but should work with newer and older versions as well):
  - URL: http://www.junit.org/
  - JAR: junit-4.8.1.jar
* Java 7


Contact
=======
zoodb(AT)gmx(DOT)de

![ZooDB](https://github.com/tzaeschke/zoodb/raw/master/doc/images/logo_510412_web.png)

