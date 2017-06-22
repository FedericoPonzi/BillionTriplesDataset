package ponzi.federico.bdc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;


/**
 * Created by Federico Ponzi
 */
public class TopKOutdegree
{
    private static final Log LOG = LogFactory.getLog(Distinct.class);
    private static final int K = 5;
    /** 1 job: **/
    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, Text>
    {

        private RDFStatement statement;
        private Text subject;
        private Text object;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            statement = new RDFStatement();
            subject = new Text();
            object = new Text();
            String[] sp = value.toString().split("\n");
            for(String s: sp)
            {
                if(statement.updateFromLine(s))
                {
                    subject.set(statement.getSubject());
                    object.set(statement.getObject());
                    context.write(subject, object);
                }else {
                    LOG.error("Error parsing: " + s);
                }
            }
        }

    }

    public static class CountNodesReducer
        extends Reducer<Text, Text, Text, IntWritable>
    {

        private static final IntWritable count = new IntWritable(1);
        public void reduce(Text key, Iterable<Text> values,
            Context context
        ) throws IOException, InterruptedException {
            int c = 0;
            for(Text val : values) c++;
            count.set(c);
            LOG.info(key.toString() +  count.toString());
            context.write(key, count);
        }
    }
    public static class OutdegreeComparator implements Comparator<NodeOutDegreeTuple>
    {
        @Override public int compare(NodeOutDegreeTuple o1, NodeOutDegreeTuple o2)
        {
            if(o1.outdegree.get() < o2.outdegree.get())
            {
                return -1;
            }
            if (o1.outdegree.get() == o2.outdegree.get())
            {
                return 0;
            }
            //if (o1.outdegree.get() > o2.outdegree.get())
            return 1;
        }

    }
    public static class NodeOutDegreeTuple implements WritableComparable<NodeOutDegreeTuple>{
        private Text node = new Text();
        private IntWritable outdegree = new IntWritable();
        public NodeOutDegreeTuple()
        {
            super();
            node = new Text();
            outdegree = new IntWritable();
        }
        public NodeOutDegreeTuple(String n, int o)
        {
            node.set(n);
            outdegree.set(o);
        }
        public NodeOutDegreeTuple(NodeOutDegreeTuple o){
            node.set(o.node.toString());
            outdegree.set(o.outdegree.get());
        }

        @Override public int compareTo(NodeOutDegreeTuple o)
        {
            return outdegree.compareTo(o.outdegree) == 0 ? node.compareTo(o.node) : outdegree.compareTo(o.outdegree);
        }

        @Override public void write(DataOutput out) throws IOException
        {
            node.write(out);
            outdegree.write(out);
        }

        @Override public void readFields(DataInput in) throws IOException
        {
            node.readFields(in);
            outdegree.readFields(in);
        }

        @Override public int hashCode()
        {
            return node.hashCode()*outdegree.hashCode();
        }

        @Override public boolean equals(Object obj)
        {
            if(obj instanceof NodeOutDegreeTuple)
            {
                NodeOutDegreeTuple o = (NodeOutDegreeTuple) obj;
                return o.outdegree.equals(outdegree) && o.node.equals(node);
            }
            return super.equals(obj);
        }

        @Override public String toString()
        {
            return "(Node: " + node.toString() +
                ", Outdegree: " + outdegree.toString() + ")";
        }
        public void set(String n, int c)
        {
            node.set(n);
            outdegree.set(c);
        }
    }

    /** 2 job: **/
    public static class CountSameDegreeNodesMapper
        extends Mapper<Object, Text, IntWritable, NodeOutDegreeTuple>
    {
        private IntWritable zero = new IntWritable(0);
        private NodeOutDegreeTuple tup = new NodeOutDegreeTuple();

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
    public static class TopKReducer // Inputs {0, top k values from every mapper }
        extends Reducer<IntWritable, NodeOutDegreeTuple, IntWritable, NodeOutDegreeTuple>
    {
        IntWritable pos = new IntWritable(0);
        public void reduce(IntWritable key, Iterable<NodeOutDegreeTuple> values,
            Context context
        ) throws IOException, InterruptedException {
            // Inizialize the Queue:
            PriorityQueue<NodeOutDegreeTuple> l = new PriorityQueue<>(K);

            //Iterate on the iterables:
            for(NodeOutDegreeTuple n : values)
            {
                l.add(new NodeOutDegreeTuple(n));
                if(l.size() > K) l.remove(); //Keep just k elements.
            }
            for(TopKOutdegree.NodeOutDegreeTuple n : l){
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
        final Log LOG = LogFactory.getLog(TopKOutdegree.class);
        LOG.info("Starting outdegree counter | arg1 input, arg2 output, arg3 temp dir");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "distinct");
        job.setJarByClass(Indegree.class);
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
        job2.setMapOutputValueClass(NodeOutDegreeTuple.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(NodeOutDegreeTuple.class);

        String inPath = args[0];
        //inPath = "/home/isaacisback/dev/mapreduce/Project/assets/btc-2010-chunk-000";
        JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
        j.waitForCompletion();

    }
}

