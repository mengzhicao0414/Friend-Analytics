/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @program: CS585 Project1
 * @Description: cs585 TaskB, report for each country how many Facebook users do they have
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 **/
public class TaskB {
    public static class TaskBMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * @Description: set the <key, value> pair as <nationality, one> and send to combiner and reducer
         * @param: [key, value, context]
         * @return: void
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            word.set(tuple[2]);
            context.write(word, one);

        }
    }

    public static class TaskBReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * @Description: for distinct key count their value and sum together
         * @param: [key, values, context]
         * @return: void
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskB");

        //set input and output file path
        args = new String[2];
        args[0] = "./data/MyPagetest.txt";
        args[1] = "./outputB";

        job.setJarByClass(TaskB.class);
        job.setMapperClass(TaskBMapper.class);
        job.setCombinerClass(TaskBReducer.class);
        job.setReducerClass(TaskBReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
