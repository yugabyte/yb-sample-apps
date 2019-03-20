[33mcommit 571be40da5269b56c3a61093cf88a81f71ca8498[m
Author: Bogdan Matican <bogdan@yugabyte.com>
Date:   Wed Mar 20 07:06:38 2019 +0000

    Extra pom changes

[33mcommit 1b1aa3ed08933e5665678d4a720b867f19b2dc5d[m
Author: Bogdan Matican <bogdan@yugabyte.com>
Date:   Tue Mar 5 00:21:28 2019 +0000

    Release related pom changes

[33mcommit 8c2ff18f2499adedb530b80171df27ab6ada7037[m
Author: Bogdan Matican <bmatican@users.noreply.github.com>
Date:   Wed Feb 27 15:17:15 2019 -0800

    Add flag for concurrent connections for Cassandra sample apps (#7)
    
    Using the native PoolingOptions to fix min/max number of concurrent, defaulting to 4, based on perf testing and YB side reactor usage.

[33mcommit be68202c96a8754190d539bf87ee0071dc5cd86d[m
Merge: 324f73c 9be6f5a
Author: Hector Cuellar <hectorgcr@gmail.com>
Date:   Mon Feb 25 10:27:49 2019 -0800

    Merge pull request #8 from hectorgcr/master
    
    In SqlInserts.java, check if database already exists before trying to create it

[33mcommit 9be6f5a8307621e2d0e1c665ffd704ba898760ff[m
Author: Hector Cuellar <hectorgcr@users.noreply.github.com>
Date:   Mon Feb 25 10:26:20 2019 -0800

    Use default database for SqlInserts workload

[33mcommit b89994ee3159a72d25645dc5174fc969b73e7515[m
Author: Hector Cuellar <hectorgcr@users.noreply.github.com>
Date:   Fri Feb 22 18:53:39 2019 -0800

    In SqlInserts.java, check if database already exists before trying to create it

[33mcommit 324f73c9552925fc686853ff81113dc3ed094dad[m
Merge: e9d6bfb 82fde35
Author: Bogdan Matican <bmatican@users.noreply.github.com>
Date:   Fri Feb 15 10:34:59 2019 -0800

    Merge pull request #5 from YugaByte/port_locking
    
    ENG-4280 #615 ENG-4283 #618: Take prefix locks for rows with compound…

[33mcommit e9d6bfb8b5d4b50fb0ce508fe502b52a398a9086[m
Merge: b594c02 2ef762e
Author: Bogdan Matican <bmatican@users.noreply.github.com>
Date:   Fri Feb 15 10:34:54 2019 -0800

    Merge pull request #6 from YugaByte/port_load_balancing
    
    #654 Allow for disabling load balancer for cql load tester

[33mcommit 2ef762ed1f10cb8cf3e75eaae06be88a670ffa4a[m
Author: Bogdan Matican <bogdan@yugabyte.com>
Date:   Fri Feb 15 06:00:20 2019 +0000

    #654 Allow for disabling load balancer for cql load tester

[33mcommit 82fde356e725721ec4cbdd48702d1bb94025059c[m
Author: Bogdan Matican <bogdan@yugabyte.com>
Date:   Fri Feb 15 05:52:06 2019 +0000

    ENG-4280 #615 ENG-4283 #618: Take prefix locks for rows with compound primary keys

[33mcommit b594c023acedb35e1a41ab5410c914fb9676c205[m
Merge: 22ac0cf 95476ba
Author: Bogdan Matican <bmatican@users.noreply.github.com>
Date:   Thu Feb 14 21:39:50 2019 -0800

    Merge pull request #4 from YugaByte/port_cql_auth
    
    ENG-4151: Modify sample apps to accept username/password for Cassandra workloads

[33mcommit 95476ba3bcf16bd7528472e2084fd136890668fc[m
Author: Bogdan Matican <bogdan@yugabyte.com>
Date:   Fri Feb 15 05:32:09 2019 +0000

    Porting 7c1d28d7307f7241c3b7eff68cbce2f706db6bce

[33mcommit 22ac0cf7d23c5b9794bd2e9e9bd0211137044728[m
Merge: eda808f b609e5f
Author: Ram Vaidyanathan <ram@yugabyte.com>
Date:   Fri Feb 8 14:13:42 2019 -0800

    Merge pull request #1 from YugaByte/add-dockerfile
    
    Add Dockerfile for sample apps and enable publishing

[33mcommit b609e5feb53a5d5e6950049faff72a6b13abbc64[m
Author: Ram Sri <ram@yugabyte.com>
Date:   Fri Feb 8 14:12:35 2019 -0800

    Update readme and added skipDockerBuild option

[33mcommit 86d435f45f74211f2a10595b1bafab0e14462444[m
Author: Ram Sri <ram@yugabyte.com>
Date:   Fri Feb 8 00:45:01 2019 -0800

    Add Dockerfile for sample apps and enable publishing

[33mcommit eda808f4041801a61764dc9fe348ab7432fdabaf[m
Author: Mihnea Iancu <mihnea@yugabyte.com>
Date:   Mon Oct 15 22:32:58 2018 -0700

    Update default num read threads to 2

[33mcommit dd2da036dc739ae324ca130437d917a1b40cfbcc[m
Author: Mihnea Iancu <mihnea@yugabyte.com>
Date:   Mon Oct 15 22:23:18 2018 -0700

    Fix selects doing full scan instead of point lookup

[33mcommit daa0e4880c42f2435966e3df50cd93ce0dc80115[m
Author: Karthik Ranganathan <karthik@yugabyte.com>
Date:   Mon Oct 15 09:13:13 2018 -0700

    Various bug fixes

[33mcommit c2f2e374ce5d273fa61f1ab7ef7c19590ab1df35[m
Author: Karthik Ranganathan <karthik@yugabyte.com>
Date:   Sun Oct 14 22:06:36 2018 -0700

    Update README.md

[33mcommit 2704e8e4476b592d4d1a30fc90c799c779058189[m
Author: Karthik Ranganathan <karthik@yugabyte.com>
Date:   Sun Oct 14 21:59:48 2018 -0700

    Various improvements to printing help

[33mcommit 0ab6c4e2f18f18fbbc2026b77a930a6db915d9ec[m
Author: Karthik Ranganathan <karthik@yugabyte.com>
Date:   Sat Oct 13 21:42:40 2018 -0700

    Create .gitignore

[33mcommit 488261d1dd11370513cf4e17f3b12a015f4531ba[m
Author: Karthik Ranganathan <karthik@yugabyte.com>
Date:   Sat Oct 13 21:41:25 2018 -0700

    Update README.md
    
    Basic instructions on building and running the sample apps.

[33mcommit ab18f55af856860e90dbeb5451a65c2e2592a229[m
Author: Karthik Ranganathan <karthik@yugabyte.com>
Date:   Sat Oct 13 21:32:14 2018 -0700

    Populating with the initial cut of the sample app code from the main YugaByte DB repo

[33mcommit b613597dee7f4d8974d49fd6259959c611001d40[m
Author: Karthik Ranganathan <karthik.ranga@gmail.com>
Date:   Sat Oct 13 21:30:18 2018 -0700

    Initial commit
