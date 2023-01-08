# 删除数据库
drop schema if exists class2022data;
# 创建数据库
CREATE DATABASE IF NOT EXISTS class2022data CHARACTER SET 'UTF8MB4' COLLATE 'UTF8MB4_general_ci';
# 切换数据库
USE class2022data;
# ----------------------
# 删除数据表
DROP TABLE IF EXISTS noise;
#创建噪声数据表
CREATE TABLE IF NOT EXISTS noise
(
    staCode VARCHAR(20),
    date    DATE,
    noise   FLOAT,
    PRIMARY KEY (staCode, date)
);

# 创建气象台站表
DROP TABLE IF EXISTS weather;
DROP TABLE IF EXISTS wstation;
CREATE TABLE IF NOT EXISTS wstation
(
    staCode   VARCHAR(20) PRIMARY KEY,
    province  VARCHAR(20),
    city      VARCHAR(20),
    longitude DECIMAL(7, 4) CHECK ( longitude BETWEEN -180 AND 180),
    latitude  DECIMAL(6, 4) CHECK ( latitude BETWEEN -90 AND 90)
);
# 创建气象观测数据表
CREATE TABLE IF NOT EXISTS weather
(
    staCode     VARCHAR(20),
    datetime    DATETIME,
    temperature DECIMAL(3, 1) CHECK ( temperature BETWEEN -99.9 AND 99.9),
    pressure    DECIMAL(5, 1) CHECK ( pressure > 0),
    humidity    INT CHECK ( humidity BETWEEN 0 AND 100),
    rain        INT CHECK ( rain >= 0 ),
    PRIMARY KEY (staCode, datetime),
    FOREIGN KEY (staCode) REFERENCES wstation (staCode)
);


DROP TABLE IF EXISTS gravity;
DROP TABLE IF EXISTS gstation;
# 创建重力台站表
CREATE TABLE IF NOT EXISTS gstation
(
    staCode VARCHAR(20) PRIMARY KEY,
    staName VARCHAR(20)
);

# 创建重力观测数据表
CREATE TABLE IF NOT EXISTS gravity
(
    staCode  VARCHAR(20),
    datetime DATETIME,
    gravity  DECIMAL(7, 4),
    PRIMARY KEY (staCode,datetime),
    FOREIGN KEY (staCode) REFERENCES gstation (staCode)
);

#统计数据表
use class2022data;
drop table if exists results;
create table if not exists results
(
    categoryName varchar(20),
    detailName VARCHAR(20),
    vMax float,
    vMin float,
    vAvg float,
    PRIMARY KEY (categoryName,detailName)
);

# 查询
SELECT MAX(noise) FROM noise; # 查询噪声最大值
SELECT COUNT(noise) FROM noise WHERE noise>3; # 查询噪声值大于3的记录数目
SELECT * from noise where noise=(SELECT MAX(noise) FROM noise);

