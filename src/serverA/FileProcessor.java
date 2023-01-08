package serverA;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: galaxy
 * @Date: 2022/12/27 21:17
 * @Content: 文件处理
 * 1. 扫描指定目录e:\ServerA，检查是否有文件，并判断文件是数据文件还是媒体文件
 * 参考：http://www.manongjc.com/detail/25-faigknzhdtbgvst.html
 * 2. 对数据文件启动数据处理线程，发送数据到数据库C；对媒体文件启动媒体文件网络传输线程，发送到服务器B。
 * 3. 线程结束后，删除已处理的文件。
 */
public class FileProcessor extends Thread {

    //成员变量
    public static String serverPath = "e:\\serverA";
    File rootDir;
    String[] fileNameList;
    // 可识别文件列表
//    ArrayList<ArrayList> knownFilesInfoList;
    ArrayList<String> knownFilesNameList;
    ArrayList<String> knownFilesPathList;
    ArrayList<String> knownFilesSuffixList;
    ArrayList<Integer> knownFilesCategoryList;
    // 数据文件列表
    ArrayList<String>  dataFilesNameList;
    ArrayList<String>  dataFilesPathList;
    ArrayList<String>  dataFilesSuffixList;
//    ArrayList<Integer> dataFilesCategoryList;
    // 媒体文件列表
    ArrayList<String>  mediaFilesNameList;
    ArrayList<String>  mediaFilesPathList;
    ArrayList<String>  mediaFilesSuffixList;
//    ArrayList<Integer> mediaFilesCategoryList;

    //创建字典，判断文件分类是数据文件(1)还是媒体文件(2)
    private final static HashMap<String, Integer> FILETYPES = new HashMap<String, Integer>() {
        {
            put("csv", 1);
            put("tsf", 1);
            put("mp4", 2);
            put("mkv", 2);
            put("flv", 2);
            put("mp3", 2);
            put("wav", 2);
        }

    };

    @Test
    @Override
    public void run() {
        //休眠一段时间后再开始检查
        while (true) {
            try {
                sleep(5000);//合适的设置，每600,000ms执行一次
                //调用本类中的方法detectFile()
                this.detectFiles();
                this.classifyFiles();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void detectFiles() {
        //判断目录是否存在
        rootDir = new File(serverPath);
        if (!rootDir.exists() || !rootDir.isDirectory()) {
            //目录不存在，
            System.out.printf("[FileProcessor.detectFiles.WARN]目录 %s 不存在...\n", rootDir);
//            // 创建目录
//            System.out.printf("[WARN]目录 %s 不存在，创建目录中...\n", rootDir);
//            //mkdirs()创建目录，返回布尔值，表示目录是否创建成功
//            if (rootDir.mkdirs()) {
//                System.out.println("[INFO]目录创建成功");
//            }
        } else {
            System.out.printf("[FileProcessor.detectFiles.INFO]目录 %s 存在,检查目录下的文件...\n", rootDir);
        }
        //检查文件列表，并将每个识别出类型文件的完整路径保存到 filePathList 中
        fileNameList = rootDir.list();
        if (fileNameList == null) {
            System.out.println(new Date(System.currentTimeMillis()) +
                    " [FileProcessor.detectFiles..WARN]: 目录下没有检查到文件,等待下次定时任务...");
            return;// 没有检查到文件,返回空列表
        }
        System.out.print(new Date(System.currentTimeMillis()) +
                " [FileProcessor.detectFiles.INFO]: 检查到文件或目录：");
//        System.out.println(Arrays.toString(fileNameList));

//        return; //'return' is unnecessary as the last statement in a 'void' method
    }

    public void classifyFiles() throws InterruptedException {
        // 将识别到的文件保存到列表中
//        knownFilesInfoList = new ArrayList<>();
        knownFilesNameList = new ArrayList<>();
        knownFilesPathList = new ArrayList<>();
        knownFilesSuffixList = new ArrayList<>();
        knownFilesCategoryList = new ArrayList<>();
        dataFilesNameList = new ArrayList<>();
        dataFilesPathList = new ArrayList<>();
        dataFilesSuffixList = new ArrayList<>();
        mediaFilesNameList = new ArrayList<>();
        mediaFilesPathList = new ArrayList<>();
        mediaFilesSuffixList = new ArrayList<>();

        for (String fileName : fileNameList) {
            //识别文件逻辑-判断：1.是否是文件 2.是否有后缀 3.是否为已知文件类型 4、是数据还是媒体
            //创建文件的完整路径
            String filePath = rootDir + File.separator + fileName;
            File file = new File(filePath);
            //1.判断file是否是文件
            if (file.isFile()) {
                System.out.printf("%s [ServerA.INFO]: 文件<%s>分类中: ", new Date(System.currentTimeMillis()), fileName);
                String[] nameSegArray = fileName.split("[.]");
//                // 显示文件名按"."分割后的结果
//                for (String nameSeg : nameSegArray) {
//                    System.out.print(nameSeg + " ");
//                }
                int suffixIndex = nameSegArray.length - 1;
                //2.判断文件是否有后缀
                if (suffixIndex >= 0) {
                    //取文件名字符串数组的最后一项为文件后缀
                    String fileSuffix = nameSegArray[suffixIndex].toLowerCase(Locale.ENGLISH);
                    // 3.判断是否为已知文件类型
                    if (FILETYPES.containsKey(fileSuffix)) {
                        //4. 判断是数据文件还是媒体文件
                        int fileCategory = FILETYPES.get(fileSuffix);
                        System.out.printf("File type = %d.\n", fileCategory);
                        knownFilesNameList.add(fileName);
                        knownFilesPathList.add(filePath);
                        knownFilesSuffixList.add(fileSuffix);
                        knownFilesCategoryList.add(fileCategory);
                    } else{
                        System.out.println(" Unknown file type!\n");
                    }
                }
            }
        }
        System.out.println("[ServerA.INFO] Classified file amount:" + knownFilesCategoryList.size());
        for (int index=0;index<knownFilesCategoryList.size();index++) {
            switch (knownFilesCategoryList.get(index)) {
                case 1:
                    //识别到数据文件，更新数据文件列表
                    System.out.println("Data file, add to data files List!");
                    this.dataFilesNameList.add(knownFilesNameList.get(index));
                    this.dataFilesPathList.add(knownFilesPathList.get(index));
                    this.dataFilesSuffixList.add(knownFilesSuffixList.get(index));

                    break;
                case 2:
                    //识别到媒体文件，更新媒体文件列表
                    System.out.println("Media file, add to media file list!");
                    String fileName=knownFilesNameList.get(index);
                    String filePath=knownFilesPathList.get(index);
                    this.mediaFilesNameList.add(fileName);
                    this.mediaFilesPathList.add(filePath);
                    this.mediaFilesSuffixList.add(knownFilesSuffixList.get(index));

                    break;
            }
        }
        //启动向服务器B发送文件的线程
        Thread mediaTcpClient = new MediaTcpClient(mediaFilesPathList);
        mediaTcpClient.start();
        mediaTcpClient.join();
        //启动向数据库C录入数据的线程
        Thread dataSaver = new DataSaver(dataFilesPathList);
        dataSaver.start();
        //等待两个进程结束
        dataSaver.join();
    }

}
