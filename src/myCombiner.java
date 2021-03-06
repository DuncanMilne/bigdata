import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by 2087186m on 17/02/17.
 */
public class myCombiner extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value:values)
            sum+= value.get();

        context.write(key, new IntWritable(sum));
    }   // sums up number of revisions for each article ID
}
