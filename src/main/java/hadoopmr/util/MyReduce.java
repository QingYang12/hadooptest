package hadoopmr.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text,LongWritable,Text, LongWritable> {
    @Override
    protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
        long s = 0L;
        for (LongWritable longWritable:v2s){
            //迭代求和
            s+=longWritable.get();
        }
        //将结果输出，写到hdfs上
        context.write(k2,new LongWritable(s));
    }
}
