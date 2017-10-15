package ponzi.federico.bdc.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Federico Ponzi on 16/05/17.
 */
public class RDFStatement implements WritableComparable<RDFStatement>
{
    private static final Log LOG = LogFactory.getLog(RDFStatement.class);

    public static final String REGEX = "(?<subject>\\<[^\\>]+\\>|[a-zA-Z0-9\\_\\:]+) (?<predicate>\\<[^\\ ]+\\>) (?<object>\\<[^\\>]+\\>|\\\".*\\\"|[a-zA-Z0-9\\_\\:]+|\\\"[^\\>]*\\>) (?<source>\\<[^\\>]+\\> )?\\.";
    private static final Pattern PATTERN = Pattern.compile(REGEX);
    private Text subject;
    private Text predicate;
    private Text object;
    private Text context;
    private IntWritable outdegree;

    public RDFStatement(){
        subject = new Text();
        predicate = new Text();
        object = new Text();
        context = new Text();
        outdegree = new IntWritable();
    }
    public boolean hasBlankSubject(){
        return subject.toString().charAt(0) == '_';
    }
    public boolean hasBlankObject(){
        return subject.toString().charAt(0) == '_';
    }

    public Text getContext()
    {
        return context;
    }

    public void clearContext()
    {
        context = new Text();
    }

    public boolean updateFromLine(String line)
    {
        Matcher matcher = PATTERN.matcher(line);
        if (matcher.matches())
        {
            setAll(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4));
            return true;
        }
        else
        {
            LOG.error("Can't correctly parse this line : '" + line + "'");
        }
        return false;
    }

    public void setAll(String subject, String predicate,String object, String context)
    {
        this.subject = new Text(subject);
        this.predicate = new Text(predicate);
        this.object = new Text(object);
        this.context = context == null ? new Text() : new Text(context);
        this.outdegree = new IntWritable(0);
    }
    public boolean hasContext(){
        return this.context.toString().length() > 0;
    }
    public void copyFrom(RDFStatement other)
    {
        subject.set(other.subject.toString());
        predicate.set(other.predicate);
        object.set(other.object);
        context.set(other.context);
        outdegree.set(other.outdegree.get());
    }
    @Override public boolean equals(Object obj)
    {
        if(obj instanceof RDFStatement)
        {
            RDFStatement o = (RDFStatement) obj;
            return o.subject.equals(subject) && o.predicate.equals(subject) && o.object.equals(object);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return subject.hashCode();
    }

    @Override public int compareTo(RDFStatement o)
    {
        if(o.subject.compareTo(subject) != 0)
        {
            return o.subject.compareTo(subject);
        }
        if(o.predicate.compareTo(predicate) != 0)
        {
            return o.predicate.compareTo(predicate);
        }
        if(o.object.compareTo(object) != 0)
        {
            return o.object.compareTo(object);
        }
        return 0;

    }

    @Override public void write(DataOutput out) throws IOException
    {
        subject.write(out);
        predicate.write(out);
        object.write(out);
        context.write(out);
        outdegree.write(out);
    }

    @Override public void readFields(DataInput in) throws IOException
    {
        subject.readFields(in);
        predicate.readFields(in);
        object.readFields(in);
        context.readFields(in);
        outdegree.readFields(in);
    }
    @Override public String toString()
    {
        return String.format("%s %s %s %s.", subject,predicate,object, context);
    }

    public Text getSubject()
    {
        return subject;
    }
    public Text getObject() { return object;}

}
