package serverB;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/2 20:11
 * @Content: 媒体文件传输-客户端
 */
public class MediaTcpServer extends Thread {

    private static final String serverBRoot = "e:\\serverB";

    ServerSocket serverSocketB;
    Socket socketB;
    BufferedReader brB0,brB;
    BufferedInputStream bisB;
    BufferedOutputStream bosB;


    /**
     * 创建ServerSocket对象，通过IO流，接收服务器A发送的媒体文件
     *
     * @throws IOException io异常
     */
    public void receiveFiles() throws IOException {
        try {
            try {
                // 媒体文件服务器端步骤：先从ServerA获取文件数目，再循环接收
                // 1. 创建ServerSocket对象，等待连接
                serverSocketB = new ServerSocket(8888);
                System.out.println("\n[ServerB.INFO] ServerSocket opened! Waiting for Client...");
                // 2. 创建IO输入流，从serverA获取待接收文件数量
                socketB = serverSocketB.accept();
                InputStream isB0 = socketB.getInputStream();
                brB0 = new BufferedReader(new InputStreamReader(isB0));
                int fileCount = new Integer(brB0.readLine());
                System.out.println("[ServerB.INFO] 即将接收文件数目: " + fileCount);
                //根据文件数目，循环监听端口，接受文件
                for (int i = 0; i < fileCount; i++) {
                    // 2. IO输入流，从ServerA接收文件名
                    socketB = serverSocketB.accept();
                    InputStream isB = socketB.getInputStream();
                     brB = new BufferedReader(new InputStreamReader(isB, StandardCharsets.UTF_8));
                    String lineContent;
                    StringBuilder fileName = new StringBuilder();
                    while ((lineContent = brB.readLine()) != null) {
                        fileName.append(lineContent);
                    }
                    System.out.println("[ServerB.INFO] 获取到媒体文件名:" + fileName);
                    socketB.close();
                    // 3. IO输入流，从ServerA接收文件数据,保存到名为fileName的文件中
                    socketB = serverSocketB.accept();
                    isB = socketB.getInputStream();
                    bisB = new BufferedInputStream(isB);
                    bosB = new BufferedOutputStream(new FileOutputStream(serverBRoot + File.separator + fileName.toString()));
                    byte[] buf = new byte[1024 * 1024];
                    int readLen = 0;
                    while ((readLen = bisB.read(buf)) != -1) {
                        bosB.write(buf, 0, readLen);
                    }
                    System.out.println(new Date(System.currentTimeMillis()) + " [ServerB.INFO] File content received!");
                    // 3. 发送流，把接收成功信息反馈给客户端

                }
            } finally {
                // 4. 每接收一个文件，关闭 Socket和IO流（一个文件用一次）
                brB.close();
                bosB.close();
                socketB.close();
            }
        } finally {
            // 接收完全部文件后，关闭 ServerSocket
            serverSocketB.close();
        }
    }

    @Override
    public void run() {
        // Q: 线程问题
        // 1. ServerSocket不能关闭，否则ServerA的Socket创建后会报错
        // 2. 是否需要线程？怎么设置线程?
        // 3. ServerB只能接收一批文件，怎样接收多批？
        // A:
        // ServerB循环执行TcpServer线程，当前线程执行完成后，开始执行下一个线程（新建serverSocket）
        // ServerA的TCPClient线程本身会循环执行，所以只需运行一次TCPClient线程
        // B能否按A的方式执行，可以：传输文件完成后，AB线程都会结束。
        // Q: ServerB怎么知道要接收几个文件？
        // A: 开始传输时，A先传输文件个数。
        // Q: 怎样在接收文件线程运行时，执行数据分析任务
        try {
            sleep(1000);
            receiveFiles();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
