package hadoop.hadoopmr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce
 */
public class TestMapReduce {
    public static void main(String[] args) throws IOException {
            //测试方法
        //获取配置项
        Configuration conf = new Configuration();
        //获取job
        Job job = Job.getInstance(conf);
        //设置打jar包的类
        job.setJarByClass(TestMapReduce.class);

        // 设置输入文件路径，该方法可以调用多次，用于设置多个输入文件路径
        FileInputFormat.addInputPath(job,new Path(args[0]));

        //指定map和reduce的序列化类型以及人任务类
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定输出hdfs的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
        try {
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
