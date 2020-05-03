
ZooDB
=====

<a href="http://www.zoodb.org">
<img src="https://github.com/tzaeschke/zoodb/blob/master/doc/images/logo_510412_web.png" alt="ZooDB logo" align="right" />
</a>

[![Build Status](https://travis-ci.org/tzaeschke/zoodb.svg?branch=master)](https://travis-ci.org/tzaeschke/zoodb)
[![codecov](https://codecov.io/gh/tzaeschke/zoodb/branch/master/graph/badge.svg)](https://codecov.io/gh/tzaeschke/zoodb)


ZooDB is an object oriented database based on the JDO 3.0 standard.
It is written by Tilmann Zäschke since 2008, since 2011 with friendly support by the GlobIS Group at ETH Zurich.
ZooDB is currently licensed under APL 2.0 (Apache License, Version 2.0), see file COPYING.

ZooDB is also available via maven:

```
<dependency>
    <groupId>org.zoodb</groupId>
    <artifactId>zoodb</artifactId>
    <version>0.6.0</version>
</dependency>
```

There are two ZooDB plug-in projects:
 * [Server B-Tree with prefix sharing: A faster server index](https://github.com/tzaeschke/zoodb-server-btree)
 * [ZooDB profiler: A profiling tool for database usage](https://github.com/tzaeschke/zoodb-profiler)


News
====

2020-05-03 - Release of ZooDB 0.6.0:
 * **License changed to Apache License, Version 2.0**
 * New query compiler V4 with support for 
   - Declared Variables
   - Math operations in queries, such as `+`, `-`, `*`, `/`, `%`, ...
   - Right/left hand sides of `<`, `<=`, `>`, `>=`, `==` are fully interchangeable
   - Queries constrained by `contains()`
   - ZooDB will use the old V3 parser where possible because execution is a bit faster.
     In rare cases, (left/right sides changed), ZooDB may wrongly detect a V3 query
     where the V4 parser would be required, please fix the queries to work with V3 in this case.
   - V4 is still work in progress.
 * Fixed Issue #103: Reading from ByteBuffers in parallel requires synchronization.
 * Fixed Issue #114: ZooDB may choose wrong root page when opening database.
 * Fixed Issue #115: Extend ZooCheckDb tool to show file format versions, even when not compatible.
 * Fixed missing cleanup for empty transactions. Tighter control over open iterators in FSM.
 * Major code cleanup

2019-01-03 - Release of ZooDB 0.5.2. Bug fixes:
 * Fixed Issues #109 and #110 : Inconsistency after removing attribute index..
 * Fixed issue #54 : schema auto-creation fails to roll back properly when commit fails.

2018-09-17 - Release of ZooDB 0.5.1. Bug fixes:
 * Issue #111: Support for serialization of persistent objects outside ZooDB
 * Issue #112: Proper handling of reattaching detached objects

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
- Multi-user/-session capability (optimistic TX), THIS IS NOT WELL TESTED!
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
* [JDO 3.1](https://db.apache.org/jdo/) (Java Data Objects):
* [JTA](http://java.sun.com/products/jta/) (Java Transaction API):
* [JUnit](http://www.junit.org/) (currently use 4.12, but should work with newer and older versions as well):
* [Java 8](https://java.com/de/download/)
* [SLF4J](https://www.slf4j.org/) (Logging API)

Contributions
=============
Please [sign-off](https://github.com/tzaeschke/zoodb/blob/master/SubmittingPatches.md) all patches before creating a pull request.

Contact
=======
zoodb(AT)gmx(DOT)de

![ZooDB](https://github.com/tzaeschke/zoodb/raw/master/doc/images/logo_510412_web.png)

