import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * Created by 2087186m on 14/02/17.
 */

//a Reduce() method that performs a summary operation (such as counting the number of students in each queue, yielding name frequencies).

public class myReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value:values) {
            sum+= value.get();
            context.write(key, new IntWritable(sum));
        }
    }

}
