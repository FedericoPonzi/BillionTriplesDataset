package ponzi.federico.bdc.problems;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import ponzi.federico.bdc.utils.JobsChainer;
import ponzi.federico.bdc.utils.RDFStatement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.PriorityQueue;


/**
 * Created by Federico Ponzi
 */
public class SameTripleDifferentContexts
{
    private static final Log LOG = LogFactory.getLog(SameTripleDifferentContexts.class);
    private static final int K = 5;
    /** 1 job: **/
    public static class TokenizerMapper
        extends Mapper<Object, Text, RDFStatement, Text>
    {

        private RDFStatement statement;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            statement = new RDFStatement();
            String[] sp = value.toString().split("\n");
            RDFStatement output = new RDFStatement();
            for(String s: sp)
            {
                if(statement.updateFromLine(s))
                {
                    output.copyFrom(statement);
                    output.clearContext();
                    /*
                    Wait, wot? We do this to:
                    - Avoid creating another DS for the triple
                    - Pass less data (we clear the context)
                     */
                    context.write(output, statement.getContext());
                }
            }
        }

    }

    public static class CountNodesReducer
        extends Reducer<RDFStatement, Text, RDFStatement, IntWritable>
    {
        private static final IntWritable count = new IntWritable(1);
        public void reduce(RDFStatement key, Iterable<Text> values,
            Context context
        ) throws IOException, InterruptedException {
            int c = 0;
            for(Text val : values) c++;
            count.set(c);
            LOG.info(key.toString() +  count.toString());
            context.write(key, count);
        }
    }

    public static class TripleNContextsTuple implements WritableComparable<TripleNContextsTuple>{
        private RDFStatement triple = new RDFStatement();
        private IntWritable ncontext = new IntWritable();
        public TripleNContextsTuple()
        {
            super();
            triple = new RDFStatement();
            ncontext = new IntWritable();
        }
        public TripleNContextsTuple(String n, int o)
        {
            triple = new RDFStatement();
            triple.updateFromLine(n);
            ncontext.set(o);
        }
        public TripleNContextsTuple(TripleNContextsTuple o){
            triple.copyFrom(o.triple);
            ncontext.set(o.ncontext.get());
        }

        @Override public int compareTo(TripleNContextsTuple o)
        {
            return ncontext.compareTo(o.ncontext) == 0 ? triple.compareTo(o.triple) : ncontext.compareTo(o.ncontext);
        }

        @Override public void write(DataOutput out) throws IOException
        {
            triple.write(out);
            ncontext.write(out);
        }

        @Override public void readFields(DataInput in) throws IOException
        {
            triple.readFields(in);
            ncontext.readFields(in);
        }

        @Override public int hashCode()
        {
            return triple.hashCode()*ncontext.hashCode();
        }

        @Override public boolean equals(Object obj)
        {
            if(obj instanceof TripleNContextsTuple)
            {
                TripleNContextsTuple o = (TripleNContextsTuple) obj;
                return o.ncontext.equals(ncontext) && o.triple.equals(triple);
            }
            return super.equals(obj);
        }

        @Override public String toString()
        {
            return "(Node: " + triple.toString() +
                ", Outdegree: " + ncontext.toString() + ")";
        }

        public void set(RDFStatement n, int c)
        {
            triple.copyFrom(n);
            ncontext.set(c);
        }
        public void set(String n, int c)
        {
            triple = new RDFStatement();
            triple.updateFromLine(n);
            ncontext.set(c);
        }
    }

    /** 2 job: **/
    public static class CountSameDegreeNodesMapper
        extends Mapper<Object, Text, IntWritable, TripleNContextsTuple>
    {
        private IntWritable zero = new IntWritable(0);
        private TripleNContextsTuple tup = new TripleNContextsTuple();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] sp = value.toString().split("\n");
            for(String s : sp)
            {
                String[] r = s.split("\t");
                tup.set(r[0], Integer.parseInt(r[1]));
                context.write(zero, tup);
            }
        }
    }
    public static class TopKReducer
        extends Reducer<IntWritable, TripleNContextsTuple, IntWritable, TripleNContextsTuple>
    {
        IntWritable pos = new IntWritable(0);
        public void reduce(IntWritable key, Iterable<TripleNContextsTuple> values,
            Context context
        ) throws IOException, InterruptedException {
            // Inizialize the Queue:
            PriorityQueue<TripleNContextsTuple> l = new PriorityQueue<>(K);

            //Iterate on the iterables:
            for(TripleNContextsTuple n : values)
            {
                l.add(new TripleNContextsTuple(n));
                if(l.size() > K) l.remove(); //Keep just k elements.
            }
            for(SameTripleDifferentContexts.TripleNContextsTuple n : l){
                System.out.println(n.toString());
            }
            //Collections.sort(l); this is not working

            int min = Integer.min(l.size(), K);
            for(int i = 0; i < min; i++)
            {
                pos.set(i);
                context.write(pos, l.remove());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        final Log LOG = LogFactory.getLog(SameTripleDifferentContexts.class);
        LOG.info("Starting ncontext counter | arg1 input, arg2 output, arg3 temp dir");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "distinct");
        job.setJarByClass(SameTripleDifferentContexts.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(CountNodesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Job job2 = Job.getInstance(conf, "distinct count");
        job2.setJarByClass(Indegree.class);
        job2.setMapperClass(CountSameDegreeNodesMapper.class);
        job2.setReducerClass(TopKReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(TripleNContextsTuple.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(TripleNContextsTuple.class);

        String inPath = args[0];
        //inPath = "/home/isaacisback/dev/mapreduce/Project/assets/btc-2010-chunk-000";
        JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
        j.waitForCompletion();
    }
}

