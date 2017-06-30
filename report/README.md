
## 1 - Compute the number of distinct nodes and edges in the corresponding RDF graph.
Using distinct with strings, and then wc -l:
3895

Using distinct with RDFStatement, and then wc -l:
6756599
also, the second approach is very more slow and memory expensive.
* In the first case, we are moving only the subject text.
* In the second, we are moving everything instead.

In order to understand who's right, let's do some command lines trick.

This should do the magic:

```
cat btc-2010-chunk-000 | sed -e "s/\ .*//g"| sort | uniq -c | wc -l```

Gets the first colmun (the subject), sort, remove duplicates, count number of distincts results.
Just for fun, let's also count the time:

```
time cat btc-2010-chunk-000 | sed -e "s/\ .*//g"| sort | uniq -c | wc -l
788703
real	7m34.608s
user	6m54.053s
sys	0m6.416s```

Ok, We've missed the right count for about *6 millions statements*. But why?

Because we're counting distinct statements, not distinct subjects!

Updating the algorithm in order to count (and pass over) just the subject and nothing else:
```
isaacisback@mrisaac ~/dev/mapreduce/Project/assets $ cat /tmp/mapreduce-output/part-r-00000 | wc -l
788703
```
Now it's much better!

Moving on the count, in order to get just a single number as output, I've struggled a little with *job chaining*.

In the end was really as simple as using as input for the latter job the output directory of the former job.
I've setup a small util class (`JobsChainer`) in order to do all the chaining stuff:

    JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
    j.waitForCompletion();

In order to get a single number as output, I've been forced to setup 2 phaeses/jobs:

    1 job:
        map(object key, text val)
            emit(val.subject, null)
        reduce(text k, null v)
            emit(1, 1)
    2 job:
        map(object key, text v)
            emit(v, 1)
        reduce(int k, int v)
            emit(sum(v), null)

-----


## 2 - Compute the outdegree/indegree distribution: does it follow a power law? Plot the result in a figure.

To get the indegree, and outdegree, we need to count the number of linking and linked nodes.

For example:

    A -> link -> B
    A -> link -> D
    C -> link -> D
    D -> link -> A

    A's out going links:
    { A : [B, D], B: [],  C : [D] , D: [A]}
    A's incoming links:
    { A : [D], B : [A], C : [], D : [C]}

And then count the number of elements in the various k/v lists.

So let's start with the **outdegree** count:

    1 job:
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
    ...
We need to read again and then count the values.

    2 phase: # This is just a count phase. we have like {d : {1 for every node with outdegree d}}
        map(object key, text val):
            emit(val, 1)
        reduce (int key, text val): #{d: number of nodes with outdegree d}
            count = 0
            for el in values:
                c++
            emit(key, c)

### Testing the implementation
Before moving on, I needed a way to be sure that both the algorithm and its implementation were right.
In order to do this, I've used a small subset of a single chunk (27 rows/statements).
Since it's a small dataset, I'll list it below:

    $ ~ cat sample.txt
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid1xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid2xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid2xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Referred to the Subcommittee on Coast Guard and Maritime Transportation." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid2xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://pervasive.semanticweb.org/ont/2004/06/time#at> "1999-02-24"^^<http://www.w3.org/2001/XMLSchema#date> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid2xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid3xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid3xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Subcommittee Consideration and Mark-up Session Held." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid3xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://pervasive.semanticweb.org/ont/2004/06/time#at> "1999-02-24"^^<http://www.w3.org/2001/XMLSchema#date> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid3xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid4xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid4xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Forwarded by Subcommittee to Full Committee (Amended) by Voice Vote." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid4xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://pervasive.semanticweb.org/ont/2004/06/time#at> "1999-02-24"^^<http://www.w3.org/2001/XMLSchema#date> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid4xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid5xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid5xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Committee Consideration and Mark-up Session Held." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid5xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://pervasive.semanticweb.org/ont/2004/06/time#at> "1999-03-02"^^<http://www.w3.org/2001/XMLSchema#date> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid5xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid6xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid6xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Ordered to be Reported by Voice Vote." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid6xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://pervasive.semanticweb.org/ont/2004/06/time#at> "1999-03-02"^^<http://www.w3.org/2001/XMLSchema#date> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid6xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid7xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid7xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Reported by the Committee on Transportation. H. Rept. 106-42." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid7xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://pervasive.semanticweb.org/ont/2004/06/time#at> "1999-03-04T12:22:00-05:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid7xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid8xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .
    _:genid8xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Placed on the Union Calendar, Calendar No. 24." <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> .


First I've run the map-reduce jobs on this and got as output:

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
As we can see, there is 1 node (H) with outdegree 2, 1 node with outdegree 7 and the other have outdegree 3. So it works!

## 3 - Indegree
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

## 4 - 10 nodes with maximum outdegree: Which are the 10 nodes with maximum outdegree, and what are their respective degrees?
After searching some MR-Patterns, computing the top-k of something seems usually use two options:
1. *Use 1 job and bash*: Use a mr job to compute the rankings, and then use  a couple of command line programs to get the first k elements (sort | head -n k for example).
2. *Use 2 jobs*: like before, use the first job to compute the rankings, but use another job to extract the first k elements.

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

    2 phase: # Get the top k
        map(object key, text val):
            emit(0, val)
        combiner(int k, Statement val):
            r = get top k statements in val
            for i = 1 to k
                emit(i, r[i])
        reduce (int key, statements val): #{d: number of nodes with outdegree d}
            r = get top k statements in val
            for i = 1 to k
                emit(i, r[i])

In the implementation of this algorithm, I've found a very interesting problem: the fact that Mapreduce recycles storage for the elements.

More precisely:

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
The seconod output was l.size() times the last inserted element. This is because mapreduce recycles the same space every time an element from the iterator gets consumed.
To overcome this problem, we can think at two solutions:

    1. *A computation consuming solution*: Keep a priorirty queue of k elements. For every element, try to insert and optionally remove the node with the smallest outdegree value.
    2. *A space consuming solution*: Save every element in a list, sort it and get the first 10 elements.
Pratically speaking, for a small k value we should consider the first implementation, which requires O(k*number_of_nodes iterations).
For this implementation, I've choosen the first implementation

## 5 - Compute the percentage of triples with empty context, the percentage of triples whose subject is
      a blank node, and the percentage of triples whose object is a blank node
In order to do this count, first we need to know if a node it's a blank node. In order to do this, I've added a couple
of utility function inside RDFStatements. A blank node, it's a node which looks like this:

    _:genid7xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e

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
            emit(2, noContext) # map no context statements to key 2

        reduce(object key, text values):
            emit(key, sum(values))

In output, we get three keys: 0 is for the number of empty subjects, 1 is for the empty objects, 2 for the number of context-free rdf statements. From .1 we know the number of distinct nodes, so
we just need to divide that number, the number of distinct nodes, with the number of blank subjects/blank objects/context-free statements.


## 6 - Each triple can appear with different contexts in the dataset. For each triple, compute the number
 of distinct contexts in which the triple appears (the empty context counts as 1). Report the 10
 triples with the largest number of distinct contexts (break ties arbitrarily)

We know in advance that this computation will require a lot of time. The reson is that, we need to use the triple as a key,
this means that we need to move around a *lot* of data. An optimization, would be to move an hash instead of the full triple.

To solve this problem, we will need 2 jobs. The first will compute the number of contexts per triple.
And for the second job, while the first mapper will read from disk the output of the first reducer, the reducer will get the top k triples.
In pseudocode:

    1 job:
        map(Object key, Text val):
            emit(<val.subject, predicate, object>, val.context)

        reduce(StatementsTriple k, List of contexts l):
            count = l.size()
            emit(k, count)

    2 job:
        map(Object key, text val):
            emit(0, <StatementsTriple, count>) //map to one reducer

        reduce(useless k, tuples <StatementsTriple, count>):
            r = get top k statements in t
            for i = 1 to k
                emit(i, r[i])

## 7 - Remove duplicate triples (i.e., produce one or more output files in which triples have no context
and each triple appears only once). How much does the dataset shrink? Consider the three input
files as a unique dataset by summing up their sizes. Similarly for the output files.

While the second part of the statment it's not very clear to me, I'll consider the first one: remove duplicate triples.
A similar approach was used in the first task (count distinct), but this time it's easier because we just need 1 job to output all the distinct elements.

Another difference is that in the first exercise, we didn't have to save & compare the sizes.
When going to save the output as a RDFStatement, I've got a quite good results. The only problem, was a wrong rappresentation of the data on the filesystem.
The main problem is the dot `.` at the end of every statement/row. Since we're gonna later compare the sizes we should remove just the duplicated lines.
To do this, we just need to output the toString of the RDFStatement wrapped in a Text class.

    @Override public String toString()
    {
        return String.format("%s %s %s %s.", subject,predicate,object, context);
    }

### Testing the implementation
To test the implemenetation, I've used the sample.txt file (it has 27 rows) and some command line programs:

    cat sample.txt > temp.txt
    cat temp.txt >> sample.txt

After running these commands, I had sample.txt with 54 rows (every row is duplicate). After running the m-r program:

    $ head -n 27 ~/dev/mapreduce/Project/assets/sample.txt | sort > sorted.txt
    $ sort part-r-00000 > result.txt
    $ diff result.txt sorted.txt

Since diff had no output, it means there are no difference!



# Appendix A: The regex for parsing the RDF graph
In order to parse these hugh text files, I've created a regex that hopefully should be able to do well on the full dataset.

    (?<subject>\<[^\>]+\>|[a-zA-Z0-9\_\:]+) (?<predicate>\<[^\ ]+\>) (?<object>\<[^\>]+\>|\".*\"|[a-zA-Z0-9\_\:]+|\"[^\>]*\>) (?<source>\<[^\>]+\> )?\.

There are 4 capturing groups. One for each element of a statement. The source group which is the context of the statement, it's optional.

A simple Java program which uses this regex to parse a text file looks like this:

    private void parseRDFGraph() throws Exception
    {
        final String REGEX = "(?<subject>\\<[^\\>]+\\>|[a-zA-Z0-9\\_\\:]+) (?<predicate>\\<[^\\ ]+\\>) (?<object>\\<[^\\>]+\\>|\\\".*\\\"|[a-zA-Z0-9\\_\\:]+|\\\".*\\>) (?<source>\\<[^\\>]+\\> )?\\.";
        final Pattern PATTERN = Pattern.compile(REGEX);
        final String path = "/home/fponzi/dev/mapreduce/Project/assets/sample.txt";
        Scanner s = new Scanner(new File(path));
        while(s.hasNext())
        {
            String line = s.nextLine();
            Matcher matcher = PATTERN.matcher(line);
            if (matcher.matches())
            {
                System.out.println(matcher.group(3));
            }
        }
    }
