# TODO

Do the 429 avoidance decorator.  learn the best query interval.  use this before @rlretry.  Or maybe use it after rlretry, but just optimize for how long it takes queries to return in general?  yes.  and as an implementation, try setting the query period to the average duration taken for rlretry to return a succesful response (ie. including retries).  But weight this average towards recent results


                   Action.ABRT  Action.RETRY0  Action.RETRY_OVER_2  Action.RETRY_OVER_4  Action.RETRY_OVER_16
ClusteredFailure     -1.000002      -1.000018            21.121019            23.934760             -1.630083
RepeatableFailure    -1.000004      -1.000031            -6.006729            -3.505744             -1.631265
RandomFailure        -1.000003      28.042277            17.699553            23.064918             24.598541
TooBusyFailure       -1.000002      -1.000031            20.788450            17.161290             24.380614
                   Action.ABRT  Action.RETRY0  Action.RETRY_OVER_2  Action.RETRY_OVER_4  Action.RETRY_OVER_16
ClusteredFailure             7              9                   25                  271                     8
RepeatableFailure         3119             67                   62                   61                    67
RandomFailure               34           1632                   35                   36                    27
TooBusyFailure               9             10                  141                    4                   175

