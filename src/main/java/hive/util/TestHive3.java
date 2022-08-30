package hive.util;



import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *如何在Java中执行Hive命令或HiveQL
 *
 */
public class TestHive3 {
    public static void main(String[] args) throws Exception{
        String sql="use hivetest;show tables;select * from person limit 10";
        List  command = new ArrayList();
        command.add("hive");
        command.add("-e");
        command.add(sql);
        List<String> results = new ArrayList();
        ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);
        Process hiveProcess = hiveProcessBuilder.start();

        BufferedReader br = new BufferedReader(new InputStreamReader(
                hiveProcess.getInputStream()));
        String data = null;
        while ((data = br.readLine()) != null) {
            results.add(data);
        }//其中command可以是其它Hive命令，不一定是HiveQL。
        System.out.println("返回结果为");
        for(String item: results){
            System.out.println(item);
        }
    }

}
