import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by 2087186m on 14/02/17.
 */
public class myInputFormat extends FileInputFormat<LongWritable,Text> {


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new recordReader();
    }
}