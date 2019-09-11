/**
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 * @program: Friend Analytics
 * @Description: CS585 PROJECT 1 "Friend Analytics"
 **/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


/**
 * @program: CS585 Project1
 * @Description: cs585 TaskF, report the people  who set up a Facebook page but never access again
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 **/
public class TaskF {

    public static class TaskFMapper extends Mapper<Object, Text, Text, Text> {
        /**
         * @Description: Mapper for AccessTime table, set userID as key, access time sa value
         * @param: [key, value, context]
         * @return: void
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            context.write(new Text(tuple[1]), new Text(tuple[4]));
        }
    }


    public static class TaskFReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * @Description: use Arraylist record different accesss time then sort the list and check whether max - minis bigger than the threshold
         * @param: [key, value, context]
         * @return: void
         */
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> list = new ArrayList<>();
            for (Text time : value) {
                list.add(Integer.parseInt(time.toString()));
            }
            Collections.sort(list);
            if (list.size() == 1)
                context.write(null, key);
            else if ( list.size() > 1 && list.get(list.size() - 1) - list.get(0) < 900000) {
                context.write(null, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskF");


        args = new String[2];
        args[0] = "./data/AccessTimetest.txt";
        args[1] = "./outputF";

        job.setJarByClass(TaskF.class);
        job.setMapperClass(TaskFMapper.class);
        job.setReducerClass(TaskFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
