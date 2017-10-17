package ponzi.federico.bdc.problems;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ponzi.federico.bdc.utils.RDFStatement;

import java.io.IOException;

/**
 * Created by isaacisback on 29/06/17.
 */

public class RemoveDuplicateTriples {
    private static final Log LOG = LogFactory.getLog(RemoveDuplicateTriples.class);
    public static class TokenizerMapper
        extends Mapper<Object, Text, RDFStatement, NullWritable>
    {

        private RDFStatement statement;
        private NullWritable nill = NullWritable.get();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            statement = new RDFStatement();

            String[] sp = value.toString().split("\n");

            for(String s: sp)
            {
                if(statement.updateFromLine(s))
                {
                    statement.clearContext();
                    context.write(statement, nill);
                }
            }
        }
    }

    public static class DiffReducer extends Reducer<RDFStatement, NullWritable, Text, NullWritable>
    {
        private NullWritable nill = NullWritable.get();
        private Text out = new Text();
        public void reduce(RDFStatement key, Iterable<NullWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            out.set(key.toString());
            context.write(out, nill);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        conf.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());

        Job job = Job.getInstance(conf, "RemoveDuplicateTriples");

        job.setJarByClass(RemoveDuplicateTriples.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DiffReducer.class);
        job.setOutputKeyClass(RDFStatement.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
