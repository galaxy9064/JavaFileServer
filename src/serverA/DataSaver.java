package serverA;

import com.mysql.cj.jdbc.Driver;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/4 22:33
 * @Content: 数据存储器
 * 1. 服务器A读取数据文件，并按格式分析
 * 2. 分析后，向数据库C相应表中写入数据
 */
public class DataSaver extends Thread {

    private static final String DB_URL = "jdbc:mysql://localhost:3306";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "88465328";

    ArrayList<String> filePathList;

    public static void main(String[] args) throws SQLException, IOException {
        ArrayList<String> filePathList = new ArrayList<>();
        filePathList.add("E:\\serverA\\noise_20221221.csv");
        filePathList.add("E:\\serverA\\weather_20221129000000.csv");
        filePathList.add("E:\\serverA\\武汉九峰(42002)3(2121)20221129_20221129.TSF");
        filePathList.add("E:\\serverA\\武汉九峰(42002)3(2121)20221201_20221201.TSF");
        filePathList.add("E:\\serverA\\武汉九峰(42002)3(2121)20221203_20221203.TSF");
        DataSaver dataSaver = new DataSaver(filePathList);
    }

    /**
     * 构造方法，传入文件列表
     *
     * @param filePathList 文件列表，包含完整路径
     */
    public DataSaver(ArrayList<String> filePathList) {
        this.filePathList = filePathList;
    }

    @Override
    /*
      覆盖run()方法，调用sendToSql();
     */
    public void run() {
        try {
            sendToSql();
        } catch (SQLException eSQL) {
            System.out.println(new Date(System.currentTimeMillis()) + " [DataSaver.run.SQLException] " + eSQL);
        } catch (IOException eIO) {
            System.out.println(new Date(System.currentTimeMillis()) + " [DataSaver.run.IOException] " + eIO);
        }
        //数据存储完成，删除数据文件
        new FileDeleter(filePathList).deleteFiles();
    }

    public void sendToSql() throws SQLException, IOException {
        //分析文件名，判断文件是什么数据
        for (String filePath : filePathList) {
            //不能用split(File.separator)，原因如下：
            // https://blog.csdn.net/weixin_42110038/article/details/115997863
            String[] filePathArray = filePath.split("[/|\\\\]");//按路径分隔符分割文件路径，linux和Windows下都可以运行
            String fileName = filePathArray[filePathArray.length - 1];//获取文件名
            String[] fileNameArray = fileName.split("[_.]");//按下划线或点分割文件名
            String fileSuffix = fileNameArray[fileNameArray.length - 1];//获取文件后缀
            //匹配文件名前缀和后缀，判断是哪种类型的数据(忽略大小写差异)

            if (fileSuffix.equalsIgnoreCase("csv")) {
                String dataCategory = fileNameArray[0].toLowerCase(Locale.ROOT);
                switch (dataCategory) {
                    case "noise"://读取noise数据
                        sendNoiseData(filePath);
                        break;
                    case "weather"://读取weather数据
                        sendWeatherData(filePath);
                        break;
                }
            } else if (fileSuffix.equalsIgnoreCase("tsf")) {//读取gravity数据
                sendGravityData(filePath);
            }
        }
    }


    /**
     * 创建数据库
     *
     * @throws SQLException sql异常
     */
    public void createDataBase() throws SQLException {
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement ps = null;
        int i = -1;

        // 1.判断数据库是否存在
        //region 判断是否存在数据库class2022data的一种方法
        //        ResultSet resultSet = conn.getMetaData().getCatalogs();
//        boolean databaseExists = false;
//        while (resultSet.next()) {
//            String databaseName = resultSet.getString(1);
//            System.out.println(databaseName);
//            if (databaseName.equalsIgnoreCase("class2022data")) {
//                databaseExists = true;
//                break;
//            }
//        }
//        System.out.println("data exists:"+databaseExists);
        //endregion

        //也可以把删除数据库和判断是否存在放在一句SQL里
        // 2.如果数据库存在，则删除
        ps = conn.prepareStatement("drop schema if exists class2022data;");
        i = ps.executeUpdate();
        System.out.println("drop database class2022data returns:" + i);

        // 3.如果数据库不存在，则创建
        ps = conn.prepareStatement("CREATE DATABASE IF NOT EXISTS class2022data" +
                " CHARACTER SET 'utf8' COLLATE 'utf8_general_ci';");
        i = ps.executeUpdate();
        System.out.println("create database returns: " + i + "(affected row count)");

        //关闭数据库连接
        ps.close();
        conn.close();
    }

    /**
     * 删除并重新创建表
     *
     * @throws SQLException sql异常
     */
    public void createTables() throws SQLException {
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement ps;

        //切换到数据库class2022data
        ps = conn.prepareStatement("USE class2022data;");
        ps.execute();
        //删除表（weather外键引用wstation，要先删除weather，再删除wstation）
        String[] tables = {"noise", "weather", "wstation", "gravity", "gstation"};
        for (String tableName : tables) {
            ps = conn.prepareStatement("DROP TABLE IF EXISTS " + tableName + ";");
            int i = ps.executeUpdate();
            System.out.println("drop table executed:" + i);
        }
        //创建表（需要先设计一下202301051800）
        // 1.创建噪音数据表
        int i = 0;
        ps = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS noise" +
                        "(staCode VARCHAR(20) NOT NULL ," +
                        "date DATE NOT NULL," +
                        "noise FLOAT NOT NULL," +
                        "PRIMARY KEY (staCode,date));"
        );
        i = ps.executeUpdate();
        System.out.println("create noise data table returned: " + i + "(0:succeed).");

        // 2-1.创建气象台站表
        ps = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS wstation" +
                        "(staCode VARCHAR(20) PRIMARY KEY," +
                        "province VARCHAR(20)," +
                        "city VARCHAR(20)," +
                        "longitude DECIMAL(7,4)  CHECK ( longitude BETWEEN -180 AND 180)," +
                        "latitude DECIMAL(6,4)  CHECK ( latitude BETWEEN -90 AND 90));"
        );
        i = ps.executeUpdate();
        System.out.println("create weather station table returned: " + i + "(0:succeed).");
        // 2-1.创建气象数据表
        ps = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS weather" +
                        "(staCode VARCHAR(20)," +
                        "datetime DATETIME," +
                        "temperature DECIMAL(4,1) CHECK ( temperature BETWEEN -99.9 AND 99.9)," +
                        "pressure DECIMAL(5,1) CHECK ( pressure>0)," +
                        "humidity INT CHECK ( humidity BETWEEN 0 AND 100)," +
                        "rain INT CHECK ( rain>=0 )," +
                        "PRIMARY KEY (staCode,datetime)," +
                        "FOREIGN KEY (staCode) REFERENCES wstation(staCode));"
//                        "#temperature,pressure,humidity,rain,lon,lat,datetime)"
        );
        i = ps.executeUpdate();
        System.out.println("create weather data table returned: " + i + "(0:succeed).");

        // 3.1 创建重力台站表+重力数据表
        ps = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS gstation" +
                        "(staCode VARCHAR(20) PRIMARY KEY," +
                        "staName VARCHAR(20));"
        );
        i = ps.executeUpdate();
        System.out.println("create gravity station table returned: " + i + "(0:succeed).");
        // 3.2 创建重力台站表+重力数据表
        ps = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS gravity" +
                        "(staCode VARCHAR(20)," +
                        "datetime DATETIME," +
                        "gravity DECIMAL(7,4)," +
                        "PRIMARY KEY (staCode,datetime)," +
                        "FOREIGN KEY (staCode) REFERENCES gstation(staCode));"
        );
        i = ps.executeUpdate();
        System.out.println("create gravity data table returned: " + i + "(0:succeed).");
        //关闭sql连接
        ps.close();
        conn.close();
    }

    /**
     * 发送噪声数据到数据库
     * @param filePath 文件列表
     * @throws SQLException sql异常
     * @throws IOException  io异常
     */
    public void sendNoiseData(String filePath) throws SQLException, IOException {
        //1.用BufferedReader读取噪声文件
        String charset = "gbk";
        String separator = ",";
        BufferedReader brNoise = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;
        System.out.println(new Date(System.currentTimeMillis()) +
                "[DataToSQL.sendNoiseData.INFO] inserting noise data to database...");
        int lineCount = 0;
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        Connection c = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement ps = c.prepareStatement("USE class2022data;");//切换到对应数据库
        ps.execute();
        while ((lineContent = brNoise.readLine()) != null) {
            if (lineCount > 0) {//第一行是表头，跳过
                String[] singleLineData = lineContent.split(separator);
//                System.out.print(Arrays.toString(singleLineData));
                String staCode = singleLineData[1];
                String strDate = singleLineData[2];
                float noiseValue = Float.parseFloat(singleLineData[0]);
                ps = c.prepareStatement(
                        "INSERT INTO noise VALUE (?,STR_TO_DATE(?,'%d/%m/%Y'),?) ON DUPLICATE KEY UPDATE noise=VALUES (noise);"
                );
                ps.setString(1, staCode);
                ps.setString(2, strDate);
                ps.setFloat(3, noiseValue);
                int i = ps.executeUpdate();
//                System.out.println(" insert into noise returns: " + i);
            }
            lineCount++;
        }
        System.out.println(new Date(System.currentTimeMillis())+
                "[DataToSQL.sendNoiseData.INFO] inserting noise data complete.");
        ps.close();
        c.close();
        brNoise.close();
    }


    /**
     * 发送气象数据到数据库
     *
     * @param filePath 文件列表
     * @throws SQLException sql异常
     * @throws IOException  io异常
     */
    public void sendWeatherData(String filePath) throws SQLException, IOException {
        String charset = "gbk";
        String separator = ",";
        BufferedReader brWeather = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;int lineCount = 0;
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        Connection c = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement ps = c.prepareStatement("USE class2022data;");//切换到对应数据库
        ps.execute();
        String staCode, province, city;
        float temperature, pressure, humidity, rain, longitude, latitude;
        System.out.println(new Date(System.currentTimeMillis()) +
                "[DataToSQL.sendWeatherData.INFO] inserting weather data to database...");
        while ((lineContent = brWeather.readLine()) != null) {
            if (lineCount > 0) {
                String[] oneLineData = lineContent.split(separator);
//                System.out.print(Arrays.toString(oneLineData));
                staCode = oneLineData[0];
                province = oneLineData[1];
                city = oneLineData[2];
                temperature = Float.parseFloat(oneLineData[3]);
                pressure = Float.parseFloat(oneLineData[4]);
                humidity = Float.parseFloat(oneLineData[5]);
                rain = Float.parseFloat(oneLineData[6]);
                longitude = Float.parseFloat(oneLineData[7]);
                latitude = Float.parseFloat(oneLineData[8]);
                String datetime = oneLineData[9];
                //向wstation表插入数据
                ps = c.prepareStatement(
//                "INSERT INTO wstation VALUE ('54195','吉林','汪清',129.792,43.297) ON DUPLICATE  KEY UPDATE province=VALUES(province),city=VALUES(city),longitude=VALUES(longitude),latitude=VALUES(latitude);"
                        "INSERT INTO wstation VALUE (?,?,?,?,?) ON DUPLICATE KEY UPDATE province=VALUES(province),city=VALUES(city),longitude=VALUES(longitude),latitude=VALUES(latitude);"
                );
                ps.setString(1, staCode);
                ps.setString(2, province);
                ps.setString(3, city);
                ps.setFloat(4, longitude);
                ps.setFloat(5, latitude);
                int i = ps.executeUpdate();
//                System.out.print(" insert into wstation: " + i);
                //向weather表插入数据
                ps = c.prepareStatement(
//                "INSERT INTO weather VALUE ('54195',str_to_date('29/11/2022 00:00:00','%d/%m/%Y %H:%i:%s'),8.6,625,7,0) ON DUPLICATE  KEY UPDATE datetime=VALUES(datetime),temperature=VALUES(temperature),pressure=VALUES(pressure),humidity=VALUES(humidity),rain=VALUES(rain);"
                        "INSERT INTO weather VALUE (?,str_to_date(?,'%d/%m/%Y %H:%i:%s'),?,?,?,?) ON DUPLICATE  KEY UPDATE datetime=VALUES(datetime),temperature=VALUES(temperature),pressure=VALUES(pressure),humidity=VALUES(humidity),rain=VALUES(rain);"

                );
                ps.setString(1, staCode);
                ps.setString(2, datetime);
                ps.setFloat(3, temperature);
                ps.setFloat(4, pressure);
                ps.setFloat(5, humidity);
                ps.setFloat(6, rain);
                i = ps.executeUpdate();
//                System.out.println(", insert into weather: " + i);
            }
            lineCount++;
        }
        System.out.println(new Date(System.currentTimeMillis())+
                "[DataToSQL.sendWeatherData.INFO] inserting weather data complete.");
        //关闭SQL连接
        ps.close();
        c.close();
        //关闭IO流
        brWeather.close();
    }


    /**
     * 发送重力数据到数据库
     *
     * @param filePath 文件列表
     * @throws SQLException sql异常
     * @throws IOException  io异常
     */
    public void sendGravityData(String filePath) throws SQLException, IOException {
        String charset = "utf-8";
        String staSeparator = "[:|(|)]";
        String dataSeparator = "\\s+";//分割一个或多个空格
        BufferedReader brGravity = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset));
        String lineContent;
        int lineCount = 0;
        DriverManager.registerDriver(new Driver());
        Connection c = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement ps = c.prepareStatement("USE class2022data;");//切换到对应数据库
        ps.execute();
        String staCode = null, staName, datetime;
        float gravityValue;
        System.out.println(new Date(System.currentTimeMillis())+
                "[DataSaver.sendGravityData.INFO] inserting gravity data to database...");
        while ((lineContent = brGravity.readLine()) != null) {
            if (lineCount == 6) {//[CHANNELS]的下一行
                String[] staInfo = lineContent.split(staSeparator);
//                System.out.print(Arrays.toString(staInfo));// 打印台站信息行
                staCode = staInfo[0];
                staName = staInfo[1];
                //向gstation中插入数据
//                ps = c.prepareStatement("insert into gstation value ('42002','武汉九峰')");
                ps = c.prepareStatement("insert into gstation value (?,?) on duplicate key update staName=values(staName);");
                ps.setString(1, staCode);
                ps.setString(2, staName);
                int i = ps.executeUpdate();
//                System.out.println(" insert into gstation returns: " + i);
            } else if (lineCount > 10) {//[DATA]的下一行
                String[] oneLineData = lineContent.split(dataSeparator);
//                System.out.print(Arrays.toString(oneLineData));// 打印数据行
                //时间格式 "年-月-日 时:分:秒"
                datetime = oneLineData[0] + "-" + oneLineData[1] + "-" + oneLineData[2] + " " + oneLineData[3] + ":" + oneLineData[4] + ":" + oneLineData[5];
                gravityValue = Float.parseFloat(oneLineData[6]);
                //向gravity表中插入数据
//                ps = c.prepareStatement("insert into gravity value ('42002',str_to_date('2022-12-03 23:59:00','%Y-%m-%d %H:%i:%s'),'100.1111')");
                ps = c.prepareStatement("insert into gravity value (?,str_to_date(?,'%Y-%m-%d %H:%i:%s'),?) on duplicate key update gravity=values(gravity)");
                ps.setString(1, staCode);
                ps.setString(2, datetime);
                ps.setDouble(3, gravityValue);
                int i = ps.executeUpdate();
//                System.out.println("insert into gravity returns: " + i);
            }
            lineCount++;
        }
        System.out.println(new Date(System.currentTimeMillis())+
                "[DataSaver.sendGravityData.INFO] inserting gravity data complete.");
        //关闭SQL连接
        ps.close();
        c.close();
        //关闭IO流
        brGravity.close();


    }

}
