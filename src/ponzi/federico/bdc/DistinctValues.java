package ponzi.federico.bdc;

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
import java.util.StringTokenizer;

/**
 * Created by Federico Ponzi
 * http://informaticalab.com/
 */
public class DistinctValues
{
    public static class DistinctMapper
        extends Mapper<Object, Text, Text, NullWritable>
    {
        private static final NullWritable nill = NullWritable.get();
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
            throws IOException,InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, nill);
            }
        }
    }
    public static class DistinctReducer
        extends Reducer<Text, NullWritable, Text, NullWritable>
    {
        private static final NullWritable nill = NullWritable.get();
        public void reduce(Text key, Iterable<NullWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            context.write(key, nill);
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Distinct values");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        String inputPath = args[0];
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
