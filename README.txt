zoodb
=====
ZooDB is an object oriented database based on the JDO standard.


Current status
==============
Under development, but already in use by some minor university projects.

Current features
================
- Fast (4x faster than db4o using db4o's PolePosition suite)
- Reasonably scalabe, has been used with 80.000.000 objects an a +30GB database
- Open source (GPL)

Current limitations
===================
- Schema evolution is ~80% complete
- No backup (except copying the DB file)
- Single-user/single session only
- Not threadsafe
- JDO only partially supported:
  - only basic queries
  - No XML config; configuration only via Java API
  - Manual enhancement of classes required (insert activateRead()/activateWrite() & extend provided super-class).
- Little documentation, but follows JDO 3.0 spec.
