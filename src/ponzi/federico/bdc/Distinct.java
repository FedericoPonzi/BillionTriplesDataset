package ponzi.federico.bdc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    public static class IntSumReducer
        extends Reducer<Text, NullWritable, Text, NullWritable>
    {
        public void reduce(Text key, Iterable<NullWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        final Log LOG = LogFactory.getLog(Distinct.class);
        LOG.info("Starting count distinct");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        String inPath = args[0];
        inPath = "/home/isaacisback/dev/mapreduce/Project/assets/btc-2010-chunk-000";
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

