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
