import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

public class ORCSample {

    public static class ORCMapper extends
            Mapper<NullWritable, OrcStruct, Text, Text> {
        public void map(NullWritable key, OrcStruct value, Context output)
                throws IOException, InterruptedException {
            output.write((Text) value.getFieldValue(1),
                    (Text) value.getFieldValue(2));
        }
    }

    public static class ORCReducer extends
            Reducer<Text, Text, NullWritable, OrcStruct> {
        private TypeDescription schema = TypeDescription
                .fromString("struct<name:string,mobile:string>");
        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        private final NullWritable nw = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values, Context output)
                throws IOException, InterruptedException {
            for (Text val : values) {
                pair.setFieldValue(1, key);
                pair.setFieldValue(4, val);
                output.write(nw, pair);
            }
        }
    }

    public static void main(String args[]) throws Exception {

        Configuration conf = new Configuration();
        conf.set("orc.mapred.output.schema", "struct<device_uuid:string,behaviour_detail:string>");
        Job job = Job.getInstance(conf, "ORC Test");
        job.setJarByClass(ORCSample.class);
        job.setMapperClass(ORCMapper.class);
        job.setReducerClass(ORCReducer.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setOutputFormatClass(OrcOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}