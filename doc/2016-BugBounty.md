
Bug Bounty
==========
Starting June 11 2016, until June 30 2016, there is a bug bounty for severe bugs concerning indexing and database consistency.

Bounty:
- The bounty 100CHF. 
- The total maximum number of rewarded bugs is 10.
- Bug hunt starts at June 11 2016, with the exception of the three people who first mentioned this to me (you know who you are), they can submit bugs right away.
- At my discretion I can choose to accept bugs as valid/applicable even if they do not strictly adhere to all rules.
- Acceptance of bugs to qualify for the bounty is at my discretion.

Requirements:
- The bug must be reproducible with a small program which needs to be submitted to me (per email or as GitHub issue on the ZooDB project) as part of the bug report.
- The bug must be reproducible by me (I will try hard and contact you if I can't reproduce it) with the latest version of the master branch (at time of submission).
- JVM crashes do not count (they are JVM bugs).
- There is no open bug description that documents the same problem (i.e. you should only submit your bug if there  is no open bug report for the same/similar problem)
- Problems caused by usage of the schema API will not be accepted.
- Bugs must be of following categories: 
  - Category 1 bugs: The bugs must reproduce a serious problem with indexing (index corruption or unreasonable behaviour) during index creation, usage or deletion.
  - Category 2 bugs: The bugs must cause database corruption to a point where ZooDB cannot recover by simply restarting ZooDB. Corruption of the user-domain model due to incorrect domain code (or code that relies on unsupported functionality in ZooDB) does _not_ count.
  - Category 3 bugs: Violation of transaction consistency in single user mode. Examples include missing or incorrect updates to the database (not everything is written correctly), being able to access data that should have been overwritten with the last commit, or rollback not working properly (not all persistent instances are rolled back properly).  
  - Category 4 bugs: Queries returning incorrect results.



The bug bounty was presented to the students of the informations systems course at ETH Zurich. The students reported no bugs in the course of this bug hunt.
