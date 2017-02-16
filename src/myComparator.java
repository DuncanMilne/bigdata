import java.util.Comparator;
import java.util.Map.Entry;

public class myComparator implements Comparator<Entry<String, Integer>>
{

    @Override
    public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2)
    {
        return o1.getValue().compareTo(o2.getValue());
    }

}