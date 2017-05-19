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
