package ponzi.federico.bdc;

/**
 * Created by Federico Ponzi on 16/05/17.
 */
public class RDFModel
{
    private String subject;
    private String predicate;
    private String object;
    String regex = "(?<subject>\\<[^\\>]+\\>|[a-zA-Z0-9\\_\\:]+) (?<predicate>\\<[^\\ ]+\\>) (?<object>\\<[^\\>]+\\>|\\\".*\\\"|[a-zA-Z0-9\\_\\:]+|\\\".*\\>) (?<source>\\<[^\\>]+\\> )?\\.";

    public RDFModel(String subject, String predicate, String object)
    {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public void setPredicate(String predicate)
    {
        this.predicate = predicate;
    }

    public void setObject(String object)
    {
        this.object = object;
    }

    public void setSubject(String subject)
    {
        this.subject = subject;
    }

    public String getSubject()
    {
        return subject;
    }

    public String getPredicate()
    {
        return predicate;
    }

    public String getObject()
    {
        return object;
    }
}
