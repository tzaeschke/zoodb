ZooDB
=====
ZooDB is an object oriented database based on the JDO 3.0 standard.


Current Status
==============
Under development, but already in use by some university projects.


Current Features
================
- Fast (4x faster than db4o using db4o's PolePosition benchmark suite)
- Reasonably scalabe, has been used successfully with +60.000.000 objects in a +30GB database
- Crash-recovery/immunity (dual flush, no logfile required)
- Open source (GPL)
- Standard stuff: commit/rollback, query, indexing, lazy-loading, transparent persistence, embedded object support (second class objects) 


Current Limitations
===================
- Schema evolution is ~80% complete
- No backup (except copying the DB file)
- Single-user/single session only
- Not threadsafe
- No stand-alone server
- JDO only partially supported:
  - only basic queries
  - No XML config or Annotations; configuration only via Java API
  - Manual enhancement of classes required (insert activateRead()/activateWrite() & extend provided super-class).
- Little documentation (some example code), but follows JDO 3.0 spec.
