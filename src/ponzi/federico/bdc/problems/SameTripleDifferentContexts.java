package ponzi.federico.bdc.problems;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import ponzi.federico.bdc.utils.JobsChainer;
import ponzi.federico.bdc.utils.RDFStatement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.PriorityQueue;


/**
 * Created by Federico Ponzi
 */
public class SameTripleDifferentContexts
{
    private static final Log LOG = LogFactory.getLog(SameTripleDifferentContexts.class);
    private static final int K = 10;

    public static class TripleNContextsTuple
        implements WritableComparable<TripleNContextsTuple>
    {
        private RDFStatement triple = new RDFStatement();
        private IntWritable nContext = new IntWritable();

        public TripleNContextsTuple()
        {
            super();
            triple = new RDFStatement();
            nContext = new IntWritable();
        }

        public TripleNContextsTuple(String tr, String nContext)
        {
            triple = new RDFStatement();
            triple.updateFromLine(tr);
            this.nContext = new IntWritable(Integer.parseInt(nContext));

        }

        public TripleNContextsTuple(TripleNContextsTuple o)
        {
            triple.copyFrom(o.triple);
            nContext.set(o.nContext.get());
        }

        @Override public int compareTo(TripleNContextsTuple o)
        {
            return nContext.compareTo(o.nContext) == 0 ? triple.compareTo(o.triple) : nContext.compareTo(o.nContext);
        }

        @Override public void write(DataOutput out) throws IOException
        {
            triple.write(out);
            nContext.write(out);
        }

        @Override public void readFields(DataInput in) throws IOException
        {
            triple.readFields(in);
            nContext.readFields(in);
        }

        @Override public int hashCode()
        {
            return triple.hashCode() * nContext.hashCode();
        }

        @Override public boolean equals(Object obj)
        {
            if (obj instanceof TripleNContextsTuple)
            {
                TripleNContextsTuple o = (TripleNContextsTuple) obj;
                return o.nContext.equals(nContext) && o.triple.equals(triple);
            }
            return super.equals(obj);
        }

        @Override public String toString()
        {
            return "(Node: " + triple.toString() +
                ", Contexts: " + nContext.toString() + ")";
        }

        public RDFStatement getTriple()
        {
            return triple;
        }

        public IntWritable getContext()
        {
            return nContext;
        }

        public void set(RDFStatement n, int c)
        {
            triple.copyFrom(n);
            nContext.set(c);
        }

        public void set(String n, int c)
        {
            triple = new RDFStatement();
            triple.updateFromLine(n);
            nContext.set(c);
        }
    }

    /**
     * 1 job:
     **/
    public static class TokenizerMapper
        extends Mapper<Object, Text, RDFStatement, Text>
    {

        private RDFStatement statement;

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            statement = new RDFStatement();
            String[] sp = value.toString().split("\n");
            RDFStatement output = new RDFStatement();
            for (String s : sp)
            {
                if (statement.updateFromLine(s))
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

        public void reduce(RDFStatement key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            int c = 0;
            HashSet<Text> seen = new HashSet<>();

            for (Text val : values)
            {
                if (!seen.contains(val)){
                    seen.add(new Text(val));
                    c++;
                }
            }
            count.set(c);
            context.write(key, count);
        }
    }


    /**
     * 2 job:
     **/
    public static class CountSameDegreeNodesMapper
        extends Mapper<Object, Text, IntWritable, TripleNContextsTuple>
    {
        private IntWritable zero = new IntWritable(0);
        private PriorityQueue<TripleNContextsTuple> topK = new PriorityQueue<>();

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String[] sp = value.toString().split("\n");
            for (String s : sp)
            {
                String[] r = s.split("\t");

                topK.add(new TripleNContextsTuple(r[0], r[1]));

                if (topK.size() > K)
                {
                    topK.remove();
                }
            }
        }

        @Override protected void cleanup(Context context)
            throws IOException, InterruptedException
        {
            int min = Integer.min(topK.size(), K); //This is mainly for debugging purpose
            for(int i = 0; i < min; i++)
            {
                context.write(zero, topK.remove());
            }

            super.cleanup(context);
        }
    }

    public static class TopKReducer extends
        Reducer<IntWritable, TripleNContextsTuple, IntWritable, TripleNContextsTuple>
    {
        IntWritable pos = new IntWritable(0);

        public void reduce(IntWritable key, Iterable<TripleNContextsTuple> values, Context context)
            throws IOException, InterruptedException
        {
            PriorityQueue<TripleNContextsTuple> topK = new PriorityQueue<>();

            //Iterate on the iterables:
            for (TripleNContextsTuple n : values)
            {
                topK.add(new TripleNContextsTuple(n));
                if (topK.size() > K)
                    topK.remove();
            }

            int min = Integer.min(topK.size(), K); //This is mainly for debugging purpose
            for(int i = 0; i < min; i++)
            {
                pos.set(i);
                context.write(pos, topK.remove());
            }

        }
    }

    public static void main(String[] args) throws Exception
    {
        final Log LOG = LogFactory.getLog(SameTripleDifferentContexts.class);
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        conf.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());

        Job job = Job.getInstance(conf, "Same triple different contexts");
        job.setJarByClass(SameTripleDifferentContexts.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(CountNodesReducer.class);
        job.setMapOutputKeyClass(RDFStatement.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(RDFStatement.class);
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

