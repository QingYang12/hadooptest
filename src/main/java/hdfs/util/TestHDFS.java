package hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * HDFS
 * 常用：
 *      上传      upload
 *      下载      download
 *      获取元信息 getMetadata
 * 其次：
 *      删除文件或目录   delete
 *      判断是否存在     exists
 *      创建文件或目录   createpath
 *
 * 附加：
 *          文件写入  filewirte
 *          文件读取  fileread
 */
public class TestHDFS {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
            //测试方法
            upload();
            download();
            getMetadata();

    }
//常用：

    /**
     * 获取文件元数据信息   文件目录  文件信息
     * upload
     */
    public static  void upload() {
        Configuration conf =new Configuration() ;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");
            /**
             * 参数一：是否删除源文件
             * 参数二：是否覆盖目标文件
             * 参数三：本地 源文件路径
             * 参数四：HDFS 目标文件路径
             */
            //上传文件
            fs.copyFromLocalFile(false,true,new Path("/usr/local/hadoopTest/test.txt"),new Path("/input2"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }



    }
    /**
     * 获取文件元数据信息   文件目录  文件信息
     * download
     */
    public static void download() {
        Configuration conf =new Configuration() ;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");
            // 2 执行下载操作
            //参数一 boolean delSrc 指是否将原文件删除
            //参数二 Path src 指要下载的文件路径（HDFS文件）
            //参数三 Path dst 指将文件下载到的路径
            //参数四 boolean useRawLocalFileSystem 是否使用RawLocalFileSystem作为本地文件系统  默认是false
            //RawLocalFileSystem不会校验和，所以改为true它不会在本地创建任何crc文件
            fs.copyToLocalFile(false, new Path("/input"), new Path("/usr/local/hadoopTest2/"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }



    }
    /**
     * 获取文件元数据信息   文件目录  文件信息
     * getMetadata
     */

    public static void getMetadata() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");
        Path path = new Path("/");
        /**
         * 列出目录下的所有的文件
         *  参数一  路径
         *  参数二  是否递归遍历
         */
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
        // 遍历所有的文件
        while (iterator.hasNext()) {
            // 获取出当前的文件
            LocatedFileStatus next = iterator.next();
            //BlockLocation[] 数组中存储的是当前文件的所有的物理切块数据信息（一个文件分不同块存储）
            BlockLocation[] blockLocations = next.getBlockLocations();
            //当前文件名
            String name = next.getPath().getName();
            //当前文件路径
            Path path1 = next.getPath();
            //文件的副本个数
            short replication = next.getReplication();
            //数据的物理切块的大小
            long blockSize = next.getBlockSize();
            //遍历该文件的每个块
            for (BlockLocation blockLocation : blockLocations) {
                String[] cachedHosts = blockLocation.getCachedHosts();
                //
                String[] names = blockLocation.getNames();
                //数据块和他副本所在的主机节点(因为一个 block 块可能有多个副本,默认值是 3)
                String[] hosts = blockLocation.getHosts();
                //数据块的长度（实际占用的大小）
                long length = blockLocation.getLength();
                //每个物理切块的起始偏移量(假如一个文件被分为三块，则第一个块的起始偏移量为0，第二个块的起始偏移量为第一个块存储的字节数)
                long offset = blockLocation.getOffset();

                System.out.println(
                        name + blockSize+"===" + path1 + "===" + Arrays.asList(cachedHosts) + "===" + Arrays.asList(names)
                                + "===" + Arrays.asList(hosts) + "==="
                                + length+"==="+offset);

            }
        }
        fs.close();



    }

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
//其次：
    /**
     * 删除文件或目录
     * delete
     */
    public static  void delete() {
        Configuration conf =new Configuration() ;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");

            //删除指定文件/文件夹 删除是会放到一个类似回收站的地方 如果第二个参数设置为true那么就会直接彻底删除
            boolean suc3= fs.delete(new Path("/input3"),true);
            System.out.println(suc3);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }



    }
    /**
     * 判断是否存在
     * exists
     */
    public static  void exists() {
        Configuration conf =new Configuration() ;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");

            //上传文件
            //判断文件或目录存在与否 存在返回true
            boolean suc2= fs.exists(new Path("/input3"));
            System.out.println(suc2);fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }



    }
    /**
     * 创建文件或目录
     * createpath
     */
    public static  void createpath() {
        Configuration conf =new Configuration() ;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");
            //写文件到hdfs系统  创建test.data并写入内容
            FSDataOutputStream out=fs.create(new Path("/input4/test.data"),true) ;
            /*创建完后可执行其他写入操作
                //FileInputStream fileInputStream=new FileInputStream("c:/test.txt");
                //IOUtils.copyBytes(fileInputStream, out, 1024,true);
            */
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }



    }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
//附加：
    //附加测试方法
    /**
     * 文件读取   fileread
     */
    public static void fileread() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");

        FSDataInputStream fin = fs.open(new Path("/input/test.txt"));
        //流定位，可以对任意字节位置进行重新定位，可以实现读完后再读一遍
        fin.seek(1);
        //跳过多少字节
        fin.skip(1) ;

        System.out.println(fin.read());

        BufferedReader br =new BufferedReader(new InputStreamReader(fin));
        String line=null;
        if((line=br.readLine())!=null){
            System.out.println(line);
        }
        br.close();
        fin.close();
        fs.close();
    }
    /**
     * 文件写入   filewirte
     *  在HDFS中的数据一般用于一次存储多次读取的使用场景
     *    *   一般不会对数据进行写操作
     *    *     1) 只能一个用户写
     *    *     2) 覆盖写      X
     *    *     3) 追加写
     */
    public static void filewirte() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://wanghaomac:9000"), conf, "root");

        //读数据
        FSDataInputStream open = fs.open(new Path("/input/test.txt"));
        BufferedReader br =new BufferedReader(new InputStreamReader(open));

        //写数据
        //create方法默认覆盖写，可以设置第二个参数为false改为追加写
        FSDataOutputStream fos = fs.create(new Path("/input2/test2.txt"));
        //追加写
        //FSDataOutputStream fos2 = fs.append(new Path("/a/c.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        String line=null;

        while((line=br.readLine())!=null){
            bw.write(line);
            bw.newLine();
        }
        bw.close();
        br.close();
        fos.close();
        fs.close();
    }

}
