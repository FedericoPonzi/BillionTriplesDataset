package ponzi.federico.bdc;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by isaacisback on 16/06/17.
 */
public class Test
{
    private void provaRegx() throws Exception
    {
        final String REGEX = "(?<subject>\\<[^\\>]+\\>|[a-zA-Z0-9\\_\\:]+) (?<predicate>\\<[^\\ ]+\\>) (?<object>\\<[^\\>]+\\>|\\\".*\\\"|[a-zA-Z0-9\\_\\:]+|\\\".*\\>) (?<source>\\<[^\\>]+\\> )?\\.";
        final Pattern PATTERN = Pattern.compile(REGEX);
        Scanner s = new Scanner(new File("/home/isaacisback/dev/mapreduce/Project/assets/test.txt"));
        while(s.hasNext())
        {
            String line = s.nextLine();
            Matcher matcher = PATTERN.matcher(line);
            if (matcher.matches())
            {
                System.out.println(matcher.group(3));
            }
        }
    }
    public static void main(String[] args)
    {

        PriorityQueue<TopKOutdegree.NodeOutDegreeTuple> l = new PriorityQueue<>();
        l.add(new TopKOutdegree.NodeOutDegreeTuple("ciao", 1));
        l.add(new TopKOutdegree.NodeOutDegreeTuple("LOL", 10));
        l.add(new TopKOutdegree.NodeOutDegreeTuple("bah", 4));
        l.remove();
        l.add(new TopKOutdegree.NodeOutDegreeTuple("b0h", 8));

        for(TopKOutdegree.NodeOutDegreeTuple n : l){
            System.out.println(n.toString());
        }
        System.out.println(l.size());
        int s = l.size();
        for(int i = 0; i < s; i++){
            System.out.println(l.remove().toString());
        }

    }
}
