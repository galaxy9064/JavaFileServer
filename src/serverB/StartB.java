package serverB;

import java.io.IOException;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/2 20:11
 * @Content: 启动服务器B的功能：1接收文件 2分析数据
 */
public class StartB {
    public static void main(String[] args) throws IOException, InterruptedException {

        //循环执行线程（等待本次执行完成后，再开始下一次）
        while (true) {
            //接收服务器A发送的文件，启动线程
            Thread threadServerB= new MediaTcpServer();
            threadServerB.start();//启动线程，（文件接收服务）
            threadServerB.join();//等待线程执行完毕（循环执行：如果isAlive，则等待）

            // 从数据库C查询数据，启动线程
            Thread ds=new DataStatistics();
            ds.start();
            ds.join();

        }

        //
    }
}
