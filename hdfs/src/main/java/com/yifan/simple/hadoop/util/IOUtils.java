package com.yifan.simple.hadoop.util;

import com.yifan.simple.hadoop.distributed.BlockPlacementManager;
import com.yifan.simple.hadoop.distributed.Server;
import com.yifan.simple.hadoop.distributed.ServerLoader;

import java.io.*;
import java.util.Date;
import java.util.List;

/**
 * 工具类
 **/
public class IOUtils {

    /**
     * 拷贝文件的工具类方法
     */
    public static void copyFile(String inputFile, String outputFile) {
        try {
            InputStream fin = new FileInputStream(inputFile);  //输入流
            OutputStream fout = new FileOutputStream(outputFile);   //输出流

            byte[] byteArray = new byte[4096];
            int length = 0; //实际读取个数
            while ((length = fin.read(byteArray)) > 0) {   //read返回 读取的字节数
                fout.write(byteArray, 0, length); //从0到len位置写入到数组bs中
            }
            fout.close();
            fin.close();
        } catch (FileNotFoundException e) {
            System.out.println("指定的输入/输出文件不存在");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("读写文件异常！");
        }
    }

    public static void copyFile(File inputFile, File outputFile) {
        try {
            InputStream fin = new FileInputStream(inputFile);  //输入流
            OutputStream fout = new FileOutputStream(outputFile);   //输出流

            byte[] byteArray = new byte[4096];
            int length = 0; //实际读取个数
            while ((length = fin.read(byteArray)) > 0) {   //read返回 读取的字节数
                fout.write(byteArray, 0, length); //从0到len位置写入到数组bs中
            }
            fout.close();
            fin.close();
        } catch (FileNotFoundException e) {
            System.out.println("指定的输入/输出文件不存在");
        } catch (IOException e) {
            System.out.println("读写文件异常！");
        }
    }

    /**
     * 真正完成数据上传操作的。（把一个大文件拆分成多个小文件，上传到多个不同的服务器，并且复制多个副本）
     *
     * @param inputFile
     * @param outputDir
     */
    public static void copyFileToDFS(String inputFile, String outputDir) {

        // 选择服务器
        BlockPlacementManager blockPlacementManager = new BlockPlacementManager();

        // 文件大小
        File file = new File(inputFile);
        long length = file.length();

        // 总共多少个数据块
        int blocksize = Integer.parseInt(PropertiesUtil.getProperty("blocksize"));
        double blocks = length * 1D / blocksize;
        int trueBlocks = (int) Math.ceil(blocks);   // 计算出来的真正的数据块的个数

        // 公共输入流
        InputStream fin = null;
        try {
            fin = new FileInputStream(inputFile);  //输入流
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // 按数据块传输
        for (int i = 0; i < trueBlocks; i++) {
            // 选N台服务器
            List<Server> servers = ServerLoader.loadServer();
            // chosedServers选取出来的多个副本存放的服务器
            List<Server> chosedServers = blockPlacementManager.choseServer(servers);

            // 拼接出一个数据块在一个服务器节点上的一个路径（处理了第1个副本）
            String blockID = "block_" + new Date().getTime();
            Server server = chosedServers.get(0);
            String serverPath = server.getServerPath();
            String parentPath = (serverPath + "/" + outputDir).replace("//", "/");
            File parentFile = new File(parentPath);
            if (!parentFile.exists()) {
                mkdir(parentPath);
            }
            String perfectServerPath = (serverPath + "/" + outputDir + "/" + blockID).replace("//", "/");
            System.out.println("block路径：" + perfectServerPath);

            /**
             * perfectServerPath 这个数据块的第一个副本在对应的服务器上的存储路径
             * serverPath + outputDir + blockID
             *
             * 创建输出流
             */
            FileOutputStream fout = null;
            try {
                fout = new FileOutputStream(perfectServerPath);//输出流
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            // 开始拷贝
            copyFile(fin, fout, blocksize);

            // 在这儿处理第后续的多个副本的传输问题  启动一个新的服务，
            // 实现从第一个服务器刚才这个数据块复制一个副本到第二个服务器
            copyReplications(chosedServers, perfectServerPath, outputDir, blockID);

            // 关闭输出流
            try {
                fout.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 当所有数据块都传输完毕了，就关闭输入流
        try {
            fin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void copyReplications(List<Server> serverList, String perfectServerPath,
                                         String outputDir, String blockID) {
        if (serverList.size() == 1) {
            return;
        }

        for (int i = 1; i < serverList.size(); i++) {

            String inputPath = perfectServerPath;
            String serverPath = serverList.get(i).getServerPath();

            String outputPerfectPath = serverPath + "/" + outputDir + "/" + blockID;
            outputPerfectPath.replace("\\", "/")
                    .replace("//", "/");

            System.out.println("副本存放的地方：" + outputPerfectPath);

            File currentPath = new File(outputPerfectPath);
            File parentPath = currentPath.getParentFile();
            if (!parentPath.exists()) {
                mkdir(parentPath.getAbsolutePath());
            }

            copyFile(inputPath, outputPerfectPath);
        }
    }

    public static void copyFile(InputStream in, OutputStream out, int blocksize) {
        try {

            // 计算读取次数
            int buffer = 4096;
            int total = blocksize / buffer;
            byte[] byteArray = new byte[buffer];

            // 读取计数
            int length = 0; //实际读取数据大小
            int counter = 0;
            while ((length = in.read(byteArray)) > 0) {   //read返回 读取的字节数
                out.write(byteArray, 0, length); //从0到len位置写入到数组bs中
                counter++;
                if (counter == total) {
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("指定的输入/输出文件不存在");
        } catch (IOException e) {
            System.out.println("读写文件异常！");
        }
    }

    public static void mkdir(String mkdir) {

        // 当前目录
        File currentDir = new File(mkdir);

        // 父目录
        File parentDir = currentDir.getParentFile();

        // 如果不存在，递归创建
        if (!parentDir.exists()) {
            mkdir(currentDir.getParent());
        }
        currentDir.mkdir();
    }
}
