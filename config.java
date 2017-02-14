import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class config extends Configured implements Tool {
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
		conf.set("mapred.jar", "/local/bd4/bd4-hadoop-ug

	}


}

