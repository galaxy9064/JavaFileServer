package serverA;

import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/4 22:43
 * @Content: 判断数据文件类别，读取数据信息
 */
public class DataFileParser {
    ArrayList<String> fileNameList;
    ArrayList<String> filePathList;
    ArrayList<String> noiseDataNameList;
    ArrayList<String> noiseDataPathList;
    ArrayList<String> weatherDataNameList;
    ArrayList<String> weatherDataPathList;
    ArrayList<String> gravityDataNameList;
    ArrayList<String> gravityDataPathList;

    private static final String DBURL = "jdbc:mysql://localhost:3306";

    private static final String DBUSER = "root";

    private static final String DBPASSWORD = "88465328";

    public static void main(String[] args) throws IOException, SQLException {
        ArrayList<String> filePathList = new ArrayList<>();
        filePathList.add("E:\\serverA\\noise_20221221.csv");
        filePathList.add("E:\\serverA\\weather_20221129000000.csv");
        filePathList.add("E:\\serverA\\武汉九峰(42002)3(2121)20221129_20221129.TSF");
        filePathList.add("E:\\serverA\\武汉九峰(42002)3(2121)20221201_20221201.TSF");
        filePathList.add("E:\\serverA\\武汉九峰(42002)3(2121)20221203_20221203.TSF");
        DataFileParser dataFileParser = new DataFileParser(filePathList);

    }

    public DataFileParser(String filePath) throws IOException, SQLException {
        this(new ArrayList<String>() {{
            add(filePath);
        }});
    }

    public DataFileParser(ArrayList<String> filePathList) throws IOException, SQLException {
        this.filePathList = filePathList;
        //分析文件名，判断文件是什么数据
        for (String filePath : filePathList) {
            //不能用split(File.separator)，原因如下：
            // https://blog.csdn.net/weixin_42110038/article/details/115997863
            String[] filePathArray = filePath.split("[/|\\\\]");//linux和Windows下都可以
            String fileName = filePathArray[filePathArray.length - 1];
            String[] fileNameArray = fileName.split("[_.]");
            String fileSuffix = fileNameArray[fileNameArray.length - 1];
            if (fileSuffix.equalsIgnoreCase("csv")) {//文字匹配时忽略大小写差异
                String dataCategory = fileNameArray[0];
                switch (dataCategory) {
                    case "noise"://读取noise数据
//                        readNoiseFileData(filePath);
//                        insertIntoNoise(filePath);
                        break;
                    case "weather"://读取weather数据
//                        readWeatherFileData(filePath);
                }
            } else if (fileSuffix.equalsIgnoreCase("tsf")) {//读取gravity数据
                readGravityFileData(filePath);//读取gravity数据
            }
        }
    }

    /**
     * 读取噪音文件数据
     *
     * @param filePath 文件路径
     */
    private void readNoiseFileData(String filePath) throws IOException, SQLException {
        String charset = "gbk";
        String separator = ",";
        BufferedReader brNoise = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;
        System.out.println("[DataFileParser.INFO] Read noise file.");
        int lineCount = 0;
        while ((lineContent = brNoise.readLine()) != null) {//第一行是表头，跳过
            if (lineCount > 0) {
                String[] oneLineData = lineContent.split(separator);
                System.out.println(Arrays.toString(oneLineData));
//                new DataSaver().sendNoiseData(oneLineData);
                //每次INSERT都要启动关闭Connection和PreparedStatement，速度慢，应该一次创建，全部发送
            }
            lineCount++;
        }
        brNoise.close();
    }

    private void insertIntoNoise(String filePath) throws IOException, SQLException {
        String charset = "gbk";
        String separator = ",";
        BufferedReader brNoise = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;
        System.out.println("[DataFileParser.INFO] Read noise file.");
        int lineCount = 0;
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        Connection c = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
        PreparedStatement ps = c.prepareStatement("USE class2022data;");
        ps.execute();
        while ((lineContent = brNoise.readLine()) != null) {//第一行是表头，跳过
            if (lineCount > 0) {
                String[] singleLineData = lineContent.split(separator);
                System.out.println(Arrays.toString(singleLineData));
//                new DataSaver().sendNoiseData(singleLineData);
                //每次INSERT都要启动关闭Connection和PreparedStatement，速度慢，应该一次创建，全部发送
                String staCode = singleLineData[1];
                String strDate = singleLineData[2];
                String noiseValue = singleLineData[0];
                ps = c.prepareStatement(
                        "INSERT INTO noise VALUE ('" + staCode + "',STR_TO_DATE('" + strDate + "','%d/%m/%Y')," + noiseValue + ") ON DUPLICATE KEY UPDATE noise=VALUES (noise);"
                );
                int i = ps.executeUpdate();
                System.out.println("insert into noise returns: " + i);
            }
            lineCount++;
        }
        ps.close();
        c.close();
        brNoise.close();
    }

    /**
     * 读取天气文件数据
     *
     * @param filePath 文件路径
     */
    private void readWeatherFileData(String filePath) throws IOException, SQLException {
        String charset = "gbk";
        String separator = ",";
        BufferedReader brWeather = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;
        System.out.println("[ServerA.DataFileParser.INFO] Read weather file");
        int lineCount = 0;
        while ((lineContent = brWeather.readLine()) != null) {
            if (lineCount > 0) {
                String[] oneLineData = lineContent.split(separator);
                System.out.println(Arrays.toString(oneLineData));
//                new DataSaver().sendWeatherData(oneLineData);
            }
            lineCount += 1;
        }
        brWeather.close();

    }

    /**
     * 读取重力文件数据
     *
     * @param filePath 文件路径
     */
    private void readGravityFileData(String filePath) throws IOException, SQLException {
        String charset = "utf-8";
        String dataSeparator = "\\s+";//分割一个或多个空格
        String staSeparator = "[:|(|)]";
        BufferedReader brGravity = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;
        int lineCount = 0;
        String[] staInfo = null;
        while ((lineContent = brGravity.readLine()) != null) {
            if (lineCount == 6) {//[CHANNELS]的下一行
                staInfo = lineContent.split(staSeparator);
                System.out.println(Arrays.toString(staInfo));// 打印台站信息行
            } else if (lineCount > 10) {//[DATA]的下一行
                String[] oneLineData = lineContent.split(dataSeparator);
                System.out.println(Arrays.toString(oneLineData));// 打印数据行
//                new DataSaver().sendGravityData(staInfo, oneLineData);
            }
            lineCount++;
        }
        brGravity.close();
    }

    private void readLine(String filePath) {

    }
}
