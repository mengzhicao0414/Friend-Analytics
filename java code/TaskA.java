/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @program: CS585 Project1
 *
 * @Description: cs585 TaskA, report all Facebook users whise Nationality is "XantOEcWqctKBgLh"
 *
 * @author: Tianfang Ni, Mengzhi Cao
 *
 * @create: 2019-02-04
 **/
public class TaskA {

    public static class TaskAMapper
            extends Mapper<Object, Text, Void, Text> {
        private Text word = new Text();

        /***
         * @Description:Mapper only job, find users whose nationality is "XantOEcWqctKBgLh", report their name and hobby
         * @param: [key, value, context]
         * @return: void
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] itr = value.toString().split(",");

            if (itr[2].equals("XantOEcWqctKBgLh")) {
                word.set(itr[1] + " " + itr[4]);
                context.write(null, word);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task A");

        //set input and output file path
        args = new String[2];
        args[0] = "./data/MyPagetest.txt";
        args[1] = "./outputA";

        job.setJarByClass(TaskA.class);
        job.setMapperClass(TaskAMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
