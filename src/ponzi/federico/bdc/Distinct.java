package ponzi.federico.bdc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Federico Ponzi
 */
public class Distinct
{
    private static final Log LOG = LogFactory.getLog(Distinct.class);

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, NullWritable>
    {

        private RDFStatement statement;
        private Text subject;
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            statement = new RDFStatement();
            subject = new Text();
            String[] sp = value.toString().split("\n");
            for(String s: sp)
            {
                if(statement.updateFromLine(s))
                {
                    subject.set(statement.getSubject());
                    context.write(subject, NullWritable.get());
                }
            }
        }
    }

    public static class DistinctReducer
        extends Reducer<Text, NullWritable, IntWritable, NullWritable>
    {

        private static final IntWritable one = new IntWritable(1);
        public void reduce(Text key, Iterable<NullWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            context.write(one, NullWritable.get()); //pass less data.
        }
    }


    public static class CountMapper
        extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        private IntWritable one = new IntWritable(1);
        private IntWritable val = new IntWritable();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] sp = value.toString().split("\n");
            for(String s : sp)
            {
                val.set(sp.length);
                context.write(one, val);
            }
        }
    }
    public static class PrintReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable>
    {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            int res = 0;
            for(IntWritable v : values)
                res += v.get();
            context.write(new IntWritable(res), NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception {
        final Log LOG = LogFactory.getLog(Distinct.class);
        LOG.info("Starting count distinct | arg1 input, arg2 output, arg3 temp dir");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "distinct");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Job job2 = Job.getInstance(conf, "distinct count");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(CountMapper.class);
        job2.setReducerClass(PrintReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        String inPath = args[0];
        inPath = "/home/isaacisback/dev/mapreduce/Project/assets/btc-2010-chunk-000";
        JobsChainer j = new JobsChainer(inPath, args[1], job, job2);
        j.waitForCompletion();

    /*    FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]) );
        if(job.waitForCompletion(true))
        {
            if(job2.waitForCompletion(true))
            {
                System.exit(0);
            }
        }
        System.exit(1);*/
    }
}

