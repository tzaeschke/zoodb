
ZooDB
=====

<a href="http://www.zoodb.org">
<img src="https://github.com/tzaeschke/zoodb/blob/master/doc/images/logo_510412_web.png" alt="ZooDB logo" align="right" />
</a>

[![Build Status](https://travis-ci.org/tzaeschke/zoodb.svg?branch=master)](https://travis-ci.org/tzaeschke/zoodb)
[![codecov](https://codecov.io/gh/tzaeschke/zoodb/branch/master/graph/badge.svg)](https://codecov.io/gh/tzaeschke/zoodb)


ZooDB is an object oriented database based on the JDO 3.0 standard.
It is written by Tilmann Zäschke since 2008, since 2011 with friendly support by the GlobIS Group at ETH Zurich.
ZooDB is currently licensed under GPLv3 (GNU Public License), see file COPYING.

ZooDB is also available via maven:

```
<dependency>
    <groupId>org.zoodb</groupId>
    <artifactId>zoodb</artifactId>
    <version>0.5.0</version>
</dependency>
```

News
====

2017-07-18 - Release of ZooDB 0.5.0. New features:
 * Java 8
 * JDO 3.1
 * Migrated logging to slf4j
 * Various bugs fixed 


Bug Bounty #2
=============
Starting March 3rd 2017, until May 31st 2017, there is a bug bounty for severe bugs concerning indexing and database consistency. Five bugs were accepted for the bug bounty.


Bug Bounty
==========
The first bug hunt started June 11 2016 and ended June 30 2016. The hunt was for severe bugs concerning indexing and database consistency. No bugs were reported.


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

