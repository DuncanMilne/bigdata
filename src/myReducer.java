import com.kenai.constantine.platform.PRIO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.PriorityQueue;

/**
 * Created by 2087186m on 14/02/17.
 */

//a Reduce() method that performs a summary operation (such as counting the number of students in each queue, yielding name frequencies).

public class myReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    private static int N;
    private IntWritable _value = new IntWritable();
    static myComparator myComparator = new myComparator();
    private static PriorityQueue<SimpleEntry<String, Integer>> prioQ = new PriorityQueue<SimpleEntry<String, Integer>>(10, myComparator);


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        N = Integer.parseInt(context.getConfiguration().get("N"));

        for (IntWritable value:values)
            sum+= value.get();

        prioQ.add(new SimpleEntry<String, Integer>(key.toString(), sum));

        if (prioQ.size() > N) {
            prioQ.poll();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        int size = prioQ.size();

        System.out.println("size is " + size);
        IntWritable values[] = new IntWritable[size];

        Text text[] =  new Text[size];

        for (int i = 0; i < size; i++) {

            SimpleEntry<String, Integer> entry = prioQ.poll();

            text[size-i-1] = new Text(entry.getKey());

            values[size-i-1] = new IntWritable(entry.getValue());
        }

        for (int i = 0; i < size; i++) {
            context.write(text[i], values[i]);
        }

    } //~cleanup

}
