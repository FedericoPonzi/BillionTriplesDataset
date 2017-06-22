# BillionTriplesDataset
Billion Triples Dataset analysis for my BigDataComputing course.

# Distinct task
using distinct with strings, and then wc -l:
3895
using distinct with RDFStatement, and then wc -l:
6756599
also, the second approach is very more slow and memory expensive.
In the first case, we are moving only the subject text.
In the second, we are moving everything instead.

So, in order to understand who's right, let's do some command lines trick.
This should do the magic:
cat btc-2010-chunk-000 | sed -e "s/\ .*//g"| sort | uniq -c | wc -l

Gets the first colmun (the subject), sort, remove duplicates, count number of distincts results.
Just for fun, I've also counted the time:

time cat btc-2010-chunk-000 | sed -e "s/\ .*//g"| sort | uniq -c | wc -l
788703
real	7m34.608s
user	6m54.053s
sys	0m6.416s
Ok, I've missed the right count for about 6 millions statements. So I understoood why this hugh difference.
I'm counting distinct statements, not subjects.
Updating the algorithm in order to count (and pass over) just the subject and nothing else:
isaacisback@mrisaac ~/dev/mapreduce/Project/assets $ cat /tmp/mapreduce-output/part-r-00000 | wc -l
788703
it works :D

In order to get just a single number as output, I've struggled a little with job chaining. In the end was really
as simple as using as input for the next job the output directory of the precedent job.
I've setup a small util class (JobsChainer) in order to do all the chaining stuff:

    JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
    j.waitForCompletion();

In order to get a single number as output, I've been forced to setup 2 phaeses/jobs:
1 phase
    map(object key, text val)
        emit(val.subject, null)
    reduce(text k, null v)
        emit(1, 1)
2 phase
    map(object key, text v)
        emit(v, 1)
    reduce(int k, int v)
        emit(sum(v), null)

-----


# Indegree, outdegree and 10 max outdegree nodes.
To get the indegree, and outdegree, we need to count the number of linking and linked nodes.
In order to do this, we should:
A -> link -> B
A -> link -> D
C -> link -> D
D -> link -> A

Outdegree:
{ A : B, D}
Indegree:
{ A: D}

So let's start from the outdegree count:

1 phase:
    map(object key, text val)
        emit(val.subject, val.object)
    reduce(object key, text values):
        count = 0 #count the number of linked elements for this node.
        for el in values:
            count ++
        emit(count, 1)
here we save on the filesystem a file like this:
A 1
A 1
A 1
We need to read again and then count the values.

2 phase: # This is just a count phase. we have like {d : {1 for every node with outdegree d}}
    map(object key, text val):
        emit(val, 1)
    reduce (int key, text val): #{d: number of nodes with outdegree d}
        count = 0
        for el in values:
            c++
        emit(key, c)

Before moving on, I needed a way to be sure that both the algorithm and its implementation were right. In order to do this,
I used a small subset of a single chunk (27 rows/statments). First I've run the map-reduce jobs on this
and got as output:
2	1
3	6
7	1

Then, I've removed everything after the first space (easy task with the regex) from the file. Basically I've kept the source nodes.
Then with sort | uniq -c:
      7 A
      3 B
      3 C
      3 D
      3 E
      3 F
      3 G
      2 H
As we can see, there is 1 node (H) with outdegree 2, 1 node with outdegree 7 and the other have outdegree 3.

### Indegree
The indegree isn't much interesting (of course, from an algorithmic point of view) because it's essentially identical to the outdegree algorithm.
We just need to swap the key value for the first mapper:

    1 phase:
        map(object key, text val)
            emit(val.object, val.subject)
        reduce(object key, text values):
            count = 0 #count the number of linked elements for this node.
            for el in values:
                count ++
            emit(count, 1)
    2 phase: # This is just a count phase. we have like {d : {1 for every node with outdegree d}}
        map(object key, text val):
            emit(val, 1)
        reduce (int key, text val): #{d: number of nodes with outdegree d}
            count = 0
            for el in values:
                c++
            emit(key, c)

To test this (just to be sure), I've used a simple script in java using the regex and removing everything else.
      1 T
      1 A
      1 C
      1 M
      1 D
      1 F
      1 N
      1 G
      1 O
      1 H
      1 I
      1 P
      1 Q
      1 R
      1 S
      2 L
      3 E
      7 B
As we can see, there is one node with outdegree 7, one node with outdegree 3, one node with indegree 2 and the other have outdegree 1.
Note: In both these algorithm, we excluded the nodes with 0 in/out degrees.

## 10 nodes with maximum outdegree: Which are the 10 nodes with maximum outdegree, and what are their respective degrees?
So let's do this. Computing the top-k of something seems usually use two options:
1. Use 1 job and bash. 1^st job to compute the rankings, and then use bash commands to get the first k elements (sort | head -n k for example).
2. Use 2 jobs. Like before, use the first job to compute the rankings, and the second to extract the first k elemnents.
The problem with 2, is that we don't know in advance if the mappers output can fit on a single reducer. We should not assume that on our own.
To overcome this scalability problem, we can use combiners. On every mapper, instead of emit out every resulted node, we just emit the first k nodes to a single reducer.
The expected input size for the single reducer is the number_of_mappers * k, which should be much much smaller than the input of the mappers (or, at least, in our case).

    1 phase: # Count the number of outdegree per node
        map(object key, text val):
                emit(val.subject, val.object)
        reduce(object key, text values):
            count = 0 #count the number of linked elements for this node.
            for el in values:
                count ++

            emit(count, 1)
    2 phase: # This is just a count phase. we have like
        map(object key, text val):
            emit(0, val)
        combiner(int k, statment):
           order(
        reduce (int key, text val): #{d: number of nodes with outdegree d}
            count = 0
            for el in values:
                c++
            emit(key, c)



# 5 - Compute the percentage of triples with empty context, the percentage of triples whose subject is
      a blank node, and the percentage of triples whose object is a blank node
In order to do this count, first we need to know if a node it's a blank node. In order to do this, I've added a couple
of utility function inside RDFStatment. A blank node, it's a node which looks like this:

    _:genid7xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e

In the implementation of this problem, I've found a very interesting problem. The fact that Mapreduce recycles the elements. More precisely:

    List<NodeOutDegreeTuple> l = new ArrayList<>();
    for(NodeOutDegreeTuple n : values){
        LOG.info(key.toString() +  n.toString());
        l.add(n);
    }
    LOG.info("Size: " + l.size());
    Collections.sort(l);
    LOG.info("Size: " + l.size());
    for(NodeOutDegreeTuple n : l){
        LOG.info(key.toString() +  n.toString());
    }
The seconod output was l.size() times the last inserted element. This is because mapreduce recycles the same element every time an element from the iterator gets consumed.
To overcome this problem, we can think at to solutions:
    1. A computation consuming solution: Keep a priorirty queue of k elements. For every element, try to insert and optionally remove the node with the smallest outdegree value.
    2. A space consuming solution: Save every element in a list, sort it and get the first 10 elements.
Pratically speaking, for a small k value we should consider the first implementation, which requires k*number_of_nodes iterations.
For this implementation, I've choosen the first implementation


The interesting thing is that they starts with "_:", so we can find them using this pattern.

This said, find the number these percentage it's straightforward. We just need to iterate throught all the nodes and find these patterns.

    1 Phase:
        map(object key, text val):
            blankSubject = 0
            blankObject = 0
            noContext = 0

            for l in vals #iterate throu all the lines
                if l.subject.isBlank:
                   blankSubject++
                if l.object.isBlank:
                   blankObject++
                if l.context == null:
                    noContext++
            emit(0, blankSubject) #map empty subjects to key 0
            emit(1, blankObject)  #map empty objects to key 1
            emit(2, noContext) # map no context statments to key 2

        reduce(object key, text values):
            emit(key, sum(values))

In output, we get three keys: 0 is for the number of empty subjects, 1 is for the empty objects, 2 for the number of context-free rdf statments. From .1 we know the number of distinct nodes, so
we just need to divide that number, the number of distinct nodes, with the number of blank subjects/blank objects/context-free statments.


# Each triple can appear with different contexts in the dataset. For each triple, compute the number
 of distinct contexts in which the triple appears (the empty context counts as 1). Report the 10
 triples with the largest number of distinct contexts (break ties arbitrarily)




