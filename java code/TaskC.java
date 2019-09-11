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
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

/**
 * @program: CS585 Project1
 * @Description: cs585 TaskC, report top 10 Facebook pages which get the most accesses
 * @author: Tianfang Ni, Mengzhi Cao
 * @create: 2019-02-04
 **/
public class TaskC {
    public static class TaskCMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
        * @Description: for each record in AccessTime table, set the page visited as key and one as value
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

    public static class TaskCCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
        * @Description: combine the <key,value> pair from mapper, sum the value for distinct ID
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

    public static class TaskCReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        /*
         * set comparator for priority queue to get the top 10 accessed page
         */
        static Comparator<String[]> taskCComparator = new Comparator<String[]>() {

            public int compare(String[] o1, String[] o2) {
                return Integer.parseInt(o1[1]) - Integer.parseInt(o2[1]);

            }
        };
        /*
         * use priority queue to implement a min heap, which only contains ten biggest value in the priority queue
         */
        PriorityQueue<String[]> minHeap = new PriorityQueue<>(taskCComparator);
        private IntWritable result = new IntWritable();

        /**
        * @Description: sum the value for distinct pageId, and check if the minHeap contains more than ten elements, if so poll the min one
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
            String[] tuple = new String[]{key.toString(), Integer.toString(sum)};
            if (minHeap.size() > 10) {
                String[] k = minHeap.poll();
            }
            minHeap.add(tuple);


        }

        /**
        * @Description: poll the elements in the minHeap which is the top 10 popular pageID
        * @param: [context]
        * @return: void
        */
        public void cleanup(Context context) throws IOException, InterruptedException {
            if(minHeap.size() > 10)
                minHeap.poll();
            while (!minHeap.isEmpty()) {
                String[] tuple = minHeap.poll();
                Text key = new Text(tuple[0]);
                IntWritable value = new IntWritable(Integer.parseInt(tuple[1]));
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskC");

        //set input and output file path
        args = new String[2];
        args[0] = "./data/AccessTimetest.txt";
        args[1] = "./outputC";

        job.setJarByClass(TaskC.class);
        job.setMapperClass(TaskCMapper.class);
        job.setCombinerClass(TaskCCombiner.class);
        job.setReducerClass(TaskCReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
