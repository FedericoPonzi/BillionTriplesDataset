package ponzi.federico.bdc.problems;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Percentages {

    public static class TokenizerMapper
        extends Mapper<Object, Text, IntWritable, IntWritable>
    {

        private RDFStatement statement;
        private IntWritable blankSubjects;
        private IntWritable blankObjects;
        private IntWritable noContexts;
        private IntWritable z = new IntWritable(0);
        private IntWritable o = new IntWritable(1);
        private IntWritable t = new IntWritable(2);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            statement = new RDFStatement();
            blankObjects = new IntWritable();
            blankSubjects = new IntWritable();
            noContexts = new IntWritable();
            int bO = 0;
            int bS = 0;
            int nC = 0;
            String[] sp = value.toString().split("\n");
            for(String s: sp)
            {
                if(statement.updateFromLine(s))
                {
                    if(statement.hasBlankSubject())
                        bS++;
                    if(statement.hasBlankObject())
                        bO++;
                    if(!statement.hasContext())
                        nC++;
                }
            }

            blankObjects.set(bO);
            blankSubjects.set(bS);
            noContexts.set(nC);

            context.write(z, blankSubjects);
            context.write(o, blankObjects);
            context.write(t, noContexts);
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Percentages.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
