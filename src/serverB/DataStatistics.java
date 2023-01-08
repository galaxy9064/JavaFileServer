package serverB;

import java.sql.*;
import java.util.Date;

/**
 * to Change the template: Editor->File and Code Templates->Includes
 *
 * @Author: XYF
 * @User: XYF
 * @Date: 2023/1/8 13:33
 * @Content: 从数据库C查询数据，并保存在数据库C的统计结果表中
 */
public class DataStatistics extends Thread{
    private static final String DB_URL = "jdbc:mysql://localhost:3306";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "88465328";

    public static void main(String[] args) throws SQLException {
        //测试
        new DataStatistics().queryAndInsertData();
    }

    @Override
    public void run(){
        try {
            sleep(5000);
            queryAndInsertData();
        } catch (SQLException | InterruptedException sqlException) {
            sqlException.printStackTrace();
        }
    }

    private void queryAndInsertData() throws SQLException {
        // 1. 创建SQL连接
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        Connection c = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        PreparedStatement ps = c.prepareStatement("use class2022data;");//切换到数据库class2022data
        ps.execute();
        // 2. 查询各类数据,将查询到的统计结果保存到results表中
        //调用queryAndInsert()方法，统计各类数据
        queryAndInsertCategoryData(c, "noise", new String[]{"noise"});//统计噪声数据
        queryAndInsertCategoryData(c, "weather", new String[]{"temperature", "pressure", "humidity", "rain"});//统计气象数据
        queryAndInsertCategoryData(c, "gravity", new String[]{"gravity"});//统计重力数据
        // 2. 关闭连接
        ps.close();
        c.close();
    }

    private void queryAndInsertCategoryData(Connection c, String catName, String[] detailNames) throws SQLException {

        float maxValue = 0, minValue = 0, avgValue = 0;
        ResultSet resultSet;
        PreparedStatement ps;
        for (String detailName : detailNames) {//逐个查询各类气象数据的统计结果
            //查询最大值
            //SQL示例：SELECT MAX(noise) FROM noise;
            ps = c.prepareStatement("SELECT MAX(" + detailName + ") FROM " + catName);
            resultSet = ps.executeQuery();
            resultSet.next();
            maxValue = resultSet.getFloat(1);
            //查询最小值
            ps = c.prepareStatement("SELECT MIN(" + detailName + ") FROM " + catName);
            resultSet = ps.executeQuery();
            resultSet.next();
            minValue = resultSet.getFloat(1);
            //查询平均值
            ps = c.prepareStatement("SELECT AVG(" + detailName + ") FROM " + catName);
            resultSet = ps.executeQuery();
            resultSet.next();
            avgValue = resultSet.getFloat(1);
            //把查询结果存入results表
            ps = c.prepareStatement("INSERT INTO results value (?,?,?,?,?) on duplicate key update " +
                    "vMax=values(vMax),vMin=values(vMin),vAvg=values(vAvg);");
            ps.setString(1, catName);
            ps.setString(2, detailName);
            ps.setFloat(3, maxValue);
            ps.setFloat(4, minValue);
            ps.setFloat(5, avgValue);
            ps.executeUpdate();
            System.out.println(new Date(System.currentTimeMillis()) +
                    "[DataStatistics.queryAndInsertCategoryData.INFO]\n" +
                    "Query "+detailName+" from table["+catName+"] and save to table[result]");
        }
    }

}
