
## 1 - Compute the number of distinct nodes and edges in the corresponding RDF graph.
Using distinct with strings, and then wc -l:
3895

Using distinct with RDFStatement, and then `wc -l`:

    6756599
also, the second approach is very more slow and memory expensive.
* In the first case, we are moving only the subject text.
* In the second, we are moving everything instead.

In order to understand who's right, let's do some command lines trick.

This should do the magic:

    cat btc-2010-chunk-000 | sed -e "s/\ .*//g"| sort | uniq -c | wc -l```

Gets the first colmun (the subject), sort, remove duplicates, count number of distincts results.
Just for fun, let's also count the time:

    time cat btc-2010-chunk-000 | sed -e "s/\ .*//g"| sort | uniq -c | wc -l
    788703
    real	7m34.608s
    user	6m54.053s
    sys	0m6.416s

Ok, We've missed the right count for about *6 millions statements*. But why?

Because we're counting distinct statements, not distinct subjects!

Updating the algorithm in order to count (and pass over) just the subject and nothing else:

    isaacisback@mrisaac ~/dev/mapreduce/Project/assets $ cat /tmp/mapreduce-output/part-r-00000 | wc -l
    788703

Now it's much better!

Moving on the count, in order to get just a single number as output, I've struggled a little with *job chaining*.

In the end was really as simple as using as input for the latter job the output directory of the former job.
I've setup a small util class (`JobsChainer`) to do all the chaining stuff:

    JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
    j.waitForCompletion();

To get a single number as output, I've been forced to setup 2 phaeses/jobs:

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

## 5 - Compute the percentage of triples with empty context, the percentage of triples whose subject is a blank node, and the percentage of triples whose object is a blank node
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


## 7 - Remove duplicate triples (i.e., produce one or more output files in which triples have no context
and each triple appears only once). How much does the dataset shrink? Consider the three input
files as a unique dataset by summing up their sizes. Similarly for the output files.

While the second part of the statement it's not very clear to me, I'll consider the first one: remove duplicate triples.
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

## 6 - Each triple can appear with different contexts in the dataset. For each triple, compute the number of distinct contexts in which the triple appears (the empty context counts as 1). Report the 10 triples with the largest number of distinct contexts (break ties arbitrarily)
*Please notice: I've put this 6th point as last, because I've made the points in this way. So It's easier to understand my design choices based on the prior ones.*
We know in advance that this computation will require a lot of time. The reason is that we need to use the triple as a key,
This means that we'll need to move around a *lot* of data.
An optimization, would be to move an hash instead of the full triple but we can't because we need to report the top 10 nodes.

To solve this problem, we will need 2 jobs:
  * The first will compute the number of contexts per triple.
  * The second job: while the first mapper will read from disk the output of the first reducer, the reducer will get the top k triples.
In pseudocode:
    old:

    1 job:
        map(Object key, Text val):
            emit(<val.subject, predicate, object>, val.context)

        reduce(StatementsTriple k, List of contexts l):
            count = l.size()
            emit(k, count)

    2 job:
        map(Object key, text val): // Parse the lines
            emit(0, <StatementsTriple, count>) //map to one reducer

        reduce(useless k, tuples <StatementsTriple, count>):
            r = get top k statements in t
            for i = 1 to k
                emit(i, r[i])


### Tesing the implementation
The implementation it's mainly a copy-paste of parts of code from prior exercises. To test this out, I've:

    * Copy and pasted twice the same file, so every line has at list a double. (The file is as I leave it from the previous implementation test)
    * Copy-pasted the last two lines from the sample.txt file, multiple times.
At this point, running the program I'm expecting:

    * Every line with context 2
    * Two statements with context >2
And here it is:

    (Node: <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid3xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e ., Contexts: 2)
    (Node: <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid1xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e ., Contexts: 2)
    (Node: <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/hadAction> _:genid2xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e ., Contexts: 2)
    (Node: _:genid8xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction> ., Contexts: 6)
    (Node: _:genid8xxhttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Fsparqlx3Fqueryx3DDESCRIBEx2Bx253chttpx3Ax2Fx2Fwwwx2Erdfaboutx2Ecomx2Frdfx2Fusgovx2Fcongressx2F106x2Fbillsx2Fh819x253e <http://purl.org/dc/elements/1.1/description> "Placed on the Union Calendar, Calendar No. 24." ., Contexts: 7)
One node with 6 different contexts.
At this point, I've realized that this is not what the statement wanted, because it requested **distinct** contexts.
In order to fix this, we should not count already seen contexts, so an HashSet looked like a good solution...

    HashSet<Text> seen = new HashSet<>();

    for(Text val : values)
    {
        if (seen.contains(val))
            continue;
        seen.add(new Text(val.toString()));
        count++;
    }
... for the moment. The problem is that if a triple has too many context, this could lead to a memory leak.
This dependes much on the graph, and the size of the nodes in the cluster (which we will see later, on amazon!)


---
## AWS: Amazon web services. Let's do this.
Amazon has a load of different cloud services, that let a noob user like me easily create a cluster of nodes and run a big computation like this, on terabytes of data.
First of all, I needed to understand the services I'll need:
 * Amazon Elastic Map Reduce (EMR): to create the cluster and run M/R jobs. It is a custom implementation of Hadoop (but hadoop-compatible).
 * Amazon Simple Storage Service (S3) : A cheap static file hosting. It will host the RDF graph.
 * Amazon Elastic Compute Cloud (EC2): To create a server of various kind.  I will need a micro-tier server, to download the data and put it on S3.
 * Amazon Identity & Access Management (IAM): To handle users of the various services. I need this to let the EC2 instance access the S3 bucket.

I want to use Amazon ec2, because I don't want to use my connection and pc to download the files from the server and upload them to s3.
Also, to keep things fast and cheap, I don't want to store them locally on the ec2 machine, but upload them directly to s3.

So, let's start bottom up!

First, I needed to setup a user for my EC2 instance. I gave him full access to S3. After getting the Access id and Access password, I've set it inside the instace in order to use the command line tool s3put
This is just a wrapper for `boto`, a python library to programmatically access S3.

Apparently, the s3put program can't send data from stdin, so I needed another tool called s3cmd.

    sudo yum install unzip python-pip -y && \
    wget https://github.com/s3tools/s3cmd/archive/master.zip && \
    unzip master.zip && \
    cd s3cmd-master && \
    sudo python setup.py install && \
    cd .. && \
    s3cmd --configure

    #Pipe example
    # mysqldump ... | s3cmd put - s3://bucket/file-name.sql
    #from: https://gist.github.com/viebig/a9109c8c75656e97888970871d386de0

After setting up the s3cmd command, we're ready to perform the wget. Before going further, I made another test.

Since I'm moving a big load of files, I don't want to manage them again. Hadoop natively support gz files, so I tried to keep my code
and use a gz file as argument. The program worked correctly, but I've noticed an output saying:

    2017-10-10 15:38:24,651 INFO  [main] mapreduce.JobSubmitter (JobSubmitter.java:submitJobInternal(200)) - number of splits:1

Running the program with a decompressed file, gave:

    2017-10-10 15:38:24,651 INFO  [main] mapreduce.JobSubmitter (JobSubmitter.java:submitJobInternal(200)) - number of splits:66
So basically it's not possible to split a big gzip file to be processed from multiple mappers. Since there are many files, and every file is 2GB in size (not that much for a normal machine) this should be fine.

To upload the files directly without storing them, I'll need some piping mechanism that downloads the files and uploads them to s3.

After some tries, this seems working fine:


    cat 000-CONTENTS | while read line; do wget --quiet -O- $line | s3cmd put - s3://big-data-computing-exam-finocchi/home/ec2-user/input/gz/${line:41} ; done ;

Then, I've used Amazon Emr, to create a 20 nodes cluster of m3.xlarge nodes,
which have:
m3.xlarge
8 vCPU, 15 GiB memory, 80 SSD GB storage

After uploading my jar file on S3, it managed to run in 29 minutes (with cluster provisioning included).

This let me run the first mapreduce task, the Distinct count problem:
 
    159177414 distinct nodes




Here 

# Appendix A: The regex for parsing the RDF graph
In order to parse these hugh text files, I wrote a regex that hopefully should work well on the full dataset.

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

## Appendix B: Jobs chaining
It was strange to me, to not find some straightforward/predefined way to chain multiple jobs. In order to achive this, I've developed a simple class `ponzi.federico.bdc.utils.JobsChainer` which lets me chain multiple jobs
by using the output of the former, as an input for the latter.

It has a path for the input, a path for the output and a  variable length number of jobs to chain.
Chaining and running chained jobs is a simple as this:

    JobsChainer j = new JobsChainer(inputPath, outputPath, job, job2, job3, job4);
    j.waitForCompletion();
