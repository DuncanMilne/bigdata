import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

//a Map() procedure (method) that performs filtering and sorting (such as sorting students by first name into queues, one queue for each name)

/**
 * Created by 2087186m on 14/02/17.
 */
public class myMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    private Text _key = new Text();
    private IntWritable _value = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] record = value.toString().split(" ");

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

        try {
            Date endDate = df.parse(context.getConfiguration().get("enddate"));
            Date startDate = df.parse(context.getConfiguration().get("startdate"));
            if (df.parse(record[4]).compareTo(startDate) > 0 && df.parse(record[4]).compareTo(endDate) < 0) {
                _key.set(record[1]);
                _value.set(1);
                context.write(_key,_value);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
