package ponzi.federico.bdc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


    @Override public int compareTo(RDFStatement o)
    {
        return o.subject.equals(subject) && o.predicate.equals(subject) && o.object.equals(object) ? 0 : 1;
    }

    @Override public void write(DataOutput out) throws IOException
    {
        subject.write(out);
        predicate.write(out);
        object.write(out);
    }

    @Override public void readFields(DataInput in) throws IOException
    {
        subject.readFields(in);
        predicate.readFields(in);
        object.readFields(in);
    }

    private Text subject;
    private Text predicate;
    private Text object;
    private Text context;

    public static final String regex = "(?<subject>\\<[^\\>]+\\>|[a-zA-Z0-9\\_\\:]+) (?<predicate>\\<[^\\ ]+\\>) (?<object>\\<[^\\>]+\\>|\\\".*\\\"|[a-zA-Z0-9\\_\\:]+|\\\".*\\>) (?<source>\\<[^\\>]+\\> )?\\.";

    public static RDFStatement createFromLine(String line)
    {
        RDFStatement ret = null;
        Pattern PATTERN = Pattern.compile(regex);
        Matcher matcher = PATTERN.matcher(line);

            if (matcher.matches())
            {
                ret = new RDFStatement(matcher.group(1), matcher.group(2), matcher.group(3));
            }
            else
            {
                LOG.error("Can't correctly parse this line : '" + line + "'");
            }
        return ret;
    }
    public RDFStatement(String subject, String predicate, String object)
    {
        this(subject, predicate, object, null);
    }
    public RDFStatement(String subject, String predicate,String object, String context)
    {

        this.subject = new Text(subject);
        this.predicate = new Text(predicate);
        this.object = new Text(object);
        this.context =  context == null? new Text() : new Text(context);
    }

    public void setPredicate(Text predicate)
    {
        this.predicate = predicate;
    }

    public void setObject(Text object)
    {
        this.object = object;
    }

    public void setSubject(Text subject)
    {
        this.subject = subject;
    }

    public Text getSubject()
    {
        return subject;
    }

    public Text getPredicate()
    {
        return predicate;
    }

    public Text getObject()
    {
        return object;
    }
}
