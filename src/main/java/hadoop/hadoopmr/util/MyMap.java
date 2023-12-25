package hadoop.hadoopmr.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMap extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //切分数据，这里处理的是一条数据
        //将数据转换成String
        String string = v1.toString();
        //将一行数据切分成数组。
        String[] words = string.split(",");
        for (String string2 : words) {
            //组装k2和v2
            String kstring = string;
            long v2 = 1L;

            //将数据写出到磁盘
            context.write(new Text(kstring), new LongWritable(v2));
            //根据Key进行排序合并，相同key的数据写出到一起
        }
    }
}
