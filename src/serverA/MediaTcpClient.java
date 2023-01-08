package serverA;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/2 20:07
 * @Content: 媒体文件传输-客户端（ServerA）
 * 1. 发送文件名称+文件数据
 * 2. 从ServerB接收文件传输成功信息
 * 3. 传输文件名： String->OutputStream--socket-->inputStream->String
 * 4. 传输文件内容：文件-FileInputStream+BufferedOutputStream->byte[]-OutputStream+inputStream->byte[]-?+?->文件
 */
public class MediaTcpClient extends Thread {
    //域（成员变量）

    ArrayList<String> filePathList = new ArrayList<>();

    Socket socketA;
    BufferedWriter bwA0;
    BufferedWriter bwA;
    BufferedInputStream bisA;
    BufferedOutputStream bosA;

    public MediaTcpClient(ArrayList<String> filePathList) {
        this.filePathList = filePathList;
    }

    private void sendFiles() throws IOException {

        // 客户端思路：先告诉serverA有几个文件，再逐个发送
        // 1.创建Socket对象，连接服务器端（ServerB）
        socketA = new Socket(InetAddress.getLocalHost(), 8888);
        System.out.println("[ServerA.INFO] Connected to ServerB, ready to upload.");
        // 2.告诉serverB有几个文件要发送
        int fileCount = filePathList.size();
        OutputStream osA0 = socketA.getOutputStream();
        bwA0 = new BufferedWriter(new OutputStreamWriter(osA0));
        bwA0.write(String.valueOf(fileCount));
        bwA0.flush();
        socketA.shutdownOutput();
        System.out.println("这里---------------------------------");
        for (String filePath : filePathList) {
            String[] filePathSplits = filePath.split("[/|\\\\]");
//            System.out.println("test:"+ Arrays.toString(filePathSplits));
            String fileName = filePathSplits[filePathSplits.length - 1];
            // 1.创建Socket对象，连接服务器端（ServerB）
            socketA = new Socket(InetAddress.getLocalHost(), 8888);
            // 2.IO输出流，将文件名发送给ServerB
            OutputStream osA1 = socketA.getOutputStream();
            bwA = new BufferedWriter(new OutputStreamWriter(osA1, StandardCharsets.UTF_8));
            bwA.write(fileName);
            bwA.flush();
            socketA.shutdownOutput();//如果不做这一步，serverB会继续读取后面写入的内容并存入文件名
            System.out.println(new Date(System.currentTimeMillis()) + " [ServerA.INFO] File name sent!");

            // 3. IO输出流，将文件数据发送给ServerB
            // 如果不重新赋值SocketA，会报错：Socket output is shutdown，因为shutdownOutput也会关闭socket。
            socketA = new Socket(InetAddress.getLocalHost(), 8888);
            OutputStream osA2 = socketA.getOutputStream();
            bosA = new BufferedOutputStream(osA2);
            bisA = new BufferedInputStream(new FileInputStream(filePath));
            byte[] buf = new byte[1024 * 1024];
            int readLen;
            while ((readLen = bisA.read(buf)) != -1) {
                bosA.write(buf, 0, readLen);
            }
            socketA.shutdownOutput();
            System.out.println(new Date(System.currentTimeMillis()) + " [ServerA.INFO] File content sent!");
            // 3.接收ServerB信息（预留）


            // 4.关闭资源
            bwA0.close();
            bwA.close();
            bisA.close();
            bosA.close();
            socketA.close();

        }
    }

    @Override
    public void run() {
        //运行sendFiles()方法
        try {
            sendFiles();
        } catch (IOException e) {
            System.out.println(new Date(System.currentTimeMillis()) + " [MediaTcpClient.run.ERROR] " + e);
//            e.printStackTrace();
        }
    }
}
