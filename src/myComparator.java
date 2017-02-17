import org.apache.hadoop.io.IntWritable;

import java.util.Comparator;
import java.util.Map.Entry;

public class myComparator implements Comparator<Entry<String, Integer>>
{

    @Override
    public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2)
    {
        if (o1.getValue().compareTo(o2.getValue()) == 0) {
            return new IntWritable(Integer.parseInt(o2.getKey())).compareTo(new IntWritable(Integer.parseInt(o1.getKey())));
        } else {
            return o1.getValue().compareTo(o2.getValue());
        }
    }
}