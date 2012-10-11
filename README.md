# Boatyard

Make boats that get shit done.

A little helper for running distributed Javascript work.

Boats have a captain and a job.  Hands asks Mates for work.
Mates ask the captain for work to give to the mate.
Mates and Hands communicate with cluster messages.
Mates and Captains communicate over UDP.
Captains phone home to the yard every once in a while to report their status.

## Running

First, you need to define a partitioner for distributing work from the Captain.
Have a look at examples/partitioner.js or examples/mongo-partitioner.js. Start
a Captain somewhere and point it at a partitioner (chunks of things that need to get done).

    captain --size 100000 --partitioner ./examples/partitioner.js

The Captain will run your partitioner function to set up a list of available
partitions (chunks of work) and start a UDP server on port 9001.  To actually
start working, you'll need to start a mate.

    mate --captain localhost --hands 10 --task ./examples/task.js

The mate with start as many hands (cluster workers) as you specify, defaulting
to the number of CPU's on the box, as well as it's own server on 9002 to catch
messages back from the captain.  Hands will ask their Mate for work (a partition)
as soon as they start up.  Hands manage the state of the partition with
`accquire`, `error`, `progress` and `release`.


## TODO

Would be nice not to have to deploy anything...


    yard build <boat name> <partitioner> <task>
    yard add-mate <host>
    yard add-hands <mate> <howmany>
    yard launch <boat name>
    yard crash <boat name>


