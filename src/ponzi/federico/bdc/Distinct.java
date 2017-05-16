package ponzi.federico.bdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Federico Ponzi
 */
public class Distinct
{
    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, NullWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] sp = value.toString().split("\n");
            String regex = "(?<subject>\\<[^\\>]+\\>|[a-zA-Z0-9\\_\\:]+) (?<predicate>\\<[^\\ ]+\\>) (?<object>\\<[^\\>]+\\>|\\\".*\\\"|[a-zA-Z0-9\\_\\:]+|\\\".*\\>) (?<source>\\<[^\\>]+\\> )?\\.";
            Pattern PATTERN = Pattern.compile(regex);
            for(String s: sp)
            {
                Matcher matcher = PATTERN.matcher(s);
                if (matcher.matches())
                {
                    word.set(matcher.group(2));
                    context.write(word, NullWritable.get());
                }else
                {
                    System.out.println("Nope:" + s);
                    throw new RuntimeException();
                }
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text, NullWritable, Text, NullWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<NullWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
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

