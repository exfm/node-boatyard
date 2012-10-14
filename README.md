# Boatyard

A helper for getting distributed Javascript jobs done with adhoc deploys.

To use boatyard, you just define two javascript functions:

* [partitioner](node-boatyard/blob/master/examples/mongo/partitioner.js) - Divide up a large chunk of work into smaller chunks, ie break a collection of 1 million documents in 100,000 document chunks.

* [task](node-boatyard/blob/master/examples/mongo/task.js) - Do work with a partition, ie read 100,000 documents from a mongo collection and put them on S3.

## Getting Started

    npm install -g boatyard

Now you'll have the `yard` bin that provides lots of helpers.  To get started, build a boat.

    mkdir myboat && cd myboat && touch task.js partitioner.js
    yard build myboat task.js partitioner.js

`build` will generate a `boat.json` config file for now.  Note: you can bail on yard at any time and just edit the generated `boat.json` file (todo in the future this will just be a package.json file).  You can just copy and paste [examples/simple/task.js](node-boatyard/blob/master/examples/simple/task.js) and [examples/simple/partitioner.js](node-boatyard/blob/master/examples/simple/partitioner.js) to `myboat` for now.

When you launch your boat later, a `package.json` file will be pushed to the captain and mates and then npm-install'd.

    yard adddep myboat request 2.11.4
    yard adddep myboat q 0.8.9

Specify the host the captain should run on:

    yard addcaptain myboat <somehost>

And a host to run a mate on:

    yard addmate myboat <someotherhost>

You can specifiy the number of hands (cluster workers) that each mate should manage.  If this isn't specified, will just be set to the number of cpu's on the mate.

    yard addhands myboat <someotherhost> 4

Ok ready to launch our boat and get some shit done.  All we need is node and npm installed on our captain and mates.

    yard launch myboat


## Task flow

When a hand is finished with a partition, it calls `self.release` and `self.getMoreWorkToDo`.  When `release` is called, the mate tells the captain the hand is finished with the partition and it can be marked as complete.  When `getMoreWorkTodo` is called, the mate asks the captain for another partition.  If there is an available partition, the captain sends it to the mate, marks it as inflight and the mate passes it along to the hand.  If there are no more available partitions, the mate kills the hand (cluster.worker.destroy).  If the mate has no remaining hands, it throws it self off the boat (`mate.server.close()`).  When the captain has no more available partitions and no partitions in flight, he kills himself.  Prefer pictures?  [Checkout this diagram](node-boatyard/blob/master/docs/images/hand_and_mate.jpg).

## Stowaways

Stowaways are pretty neat.  They're little config nuggets that get injected into partitions so they're accessible in your partitioner via nconf and task via partition data.  This is still not a really cemented concept and is still really in flux.  See `Captain.set` and `Captain.get`.


## Launch Flow

* Make boat directory on captain
* Upload partitioner source to captain
* Upload partitioner source to mates
* Create package.json and upload to captain
* Create package.json and upload to mates
* Install packages on captain and mates
* Upload captain and mate wrapper executables
* Start captain executable and > captain.log
* Start mate executable and > mate.log


## How it works

@todo Should be in haiku form.

* Boats have a captain and a job.
* Hands asks Mates for work.
* Mates ask the captain for work to give to the mate.
* Mates and Hands communicate with cluster messages.
* Mates and Captains communicate over UDP.
* (todo) Captains phone home to the yard every once in a while to report their status.


## Install

    npm install -g boatyard
