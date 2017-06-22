package ponzi.federico.bdc.problems;

/**
 * Created by isaacisback on 16/06/17.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import ponzi.federico.bdc.JobsChainer;

import java.io.IOException;


/**
 * Created by Federico Ponzi
 */
public class Indegree
{
    private static final Log LOG = LogFactory.getLog(Distinct.class);
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
                    context.write(object, subject);
                }
            }
        }
    }

    public static class CountNodesReducer
        extends Reducer<Text, Text, IntWritable, IntWritable>
    {

        private static final IntWritable count = new IntWritable(1);
        private static final IntWritable one = new IntWritable(1);
        public void reduce(Text key, Iterable<Text> values,
            Context context
        ) throws IOException, InterruptedException {
            int c = 0;
            for(Text val : values) c++;
            count.set(c);
            context.write(count, one); //pass less data.
        }
    }

    /** 2 job: **/
    public static class CountSameDegreeNodesMapper
        extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        private IntWritable val = new IntWritable();
        private IntWritable one  = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] sp = value.toString().split("\n");
            for(String s : sp)
            {
                String[] r = s.split("\t");

                val.set(Integer.parseInt(r[0]));

                context.write(val, one);
            }
        }
    }
    public static class PrintReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        IntWritable r = new IntWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            int res = 0;
            for(IntWritable v : values)
                res += 1;
            r.set(res);
            context.write(key, r);
        }
    }
    public static void main(String[] args) throws Exception {
        final Log LOG = LogFactory.getLog(Distinct.class);
        LOG.info("Starting outdegree counter | arg1 input, arg2 output, arg3 temp dir");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "distinct");
        job.setJarByClass(Indegree.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(CountNodesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        Job job2 = Job.getInstance(conf, "distinct count");
        job2.setJarByClass(Indegree.class);
        job2.setMapperClass(CountSameDegreeNodesMapper.class);
        job2.setReducerClass(PrintReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        String inPath = args[0];
        //inPath = "/home/isaacisback/dev/mapreduce/Project/assets/btc-2010-chunk-000";
        JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
        j.waitForCompletion();

    }
}

