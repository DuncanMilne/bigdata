/**
 * Created by 2087186m on 13/02/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Main(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create(getConf());
        conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
        conf.set("mapred.jar", "out/artifacts/bigdata_jar/bigdata.jar");

        conf.set("startdate", args[0]);
        conf.set("enddate", args[1]);
        conf.set("N", args[2]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);

        job.setInputFormatClass(myInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(myMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(myCombiner.class);
        job.setCombinerClass(myReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path("hdfs://dsenode0.dcs.gla.ac.uk:8020/user/bd4-ae1/enwiki-20080103-sample.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/2087186m/bigdata/31"));

        job.submit();
        return (job.waitForCompletion(true) ? 0 : 1);
    }
}