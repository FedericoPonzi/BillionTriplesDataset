package ponzi.federico.bdc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class JobsChainer
{
    /**
     * A simple job chainer class.
     */
    Job[] jobs;
    private Path input;
    private Path output;
    private String temp;

    public JobsChainer(String input, String output, Job... i)
    {
        jobs = i;
        this.input = new Path(input);
        this.output = new Path(output);
        this.temp = output + "-temp";
        try
        {
            setupChain();
        }
        catch (IOException e)
        {
            System.err.print("Error while chaining jobs: ");
            e.printStackTrace();
        }
    }

    private String getOutputPath(Job j)
    {
        /** @return my be null if output not setted. **/
        return j.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
    }

    public void setupChain() throws IOException
    {
        int i = 1;

        FileInputFormat.addInputPath(jobs[0], input);
        FileOutputFormat.setOutputPath(jobs[0], new Path(temp + "-0"));

        for (; i < jobs.length; i++)
        {

            FileInputFormat.addInputPath(jobs[i], new Path(temp + "-" + (i - 1)));
            FileOutputFormat.setOutputPath(jobs[i], new Path(temp + "-" + i));
        }
        FileOutputFormat.setOutputPath(jobs[jobs.length - 1], output);
    }

    public void waitForCompletion()
        throws InterruptedException, IOException, ClassNotFoundException
    {
        int exit = 0;
        for (Job j : jobs)
        {
            if (!j.waitForCompletion(true))
            {
                exit = 1;
                break;
            }
        }
        cleanupTempDirs();
        System.exit(exit);
    }
    private void cleanupTempDirs()
    {
        /**
         * Todo: cleanup after every job.
         */
        for(int i = 0; i < jobs.length-1; i++)
        {
            try
            {
                String path = temp + "-" + i;
                if(!deleteRecursive(new File(path))){
                    System.err.println("Can't delete '"+ path +"'.");
                }
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
            }
        }

    }
    public static boolean deleteRecursive(File path) throws FileNotFoundException
    {
        if (!path.exists()) throw new FileNotFoundException(path.getAbsolutePath());
        boolean ret = true;
        if (path.isDirectory()){
            for (File f : path.listFiles()){
                ret = ret && deleteRecursive(f);
            }
        }
        return ret && path.delete();
    }
}
