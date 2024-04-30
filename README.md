# DEMO DATA CLEANSING
This workshop provides an example of data cleaning with PySpark.

## Setup
- Install Spark
```bash
# อัพเดท Package ทั้งหมด
!apt-get update
# ติดตั้ง Java Development Kit (จำเป็นสำหรับการติดตั้ง Spark)
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
# ติดตั้ง Spark 3.1.2
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz
# Unzip ไฟล์ Spark 3.1.2
!tar xzvf spark-3.1.2-bin-hadoop2.7.tgz
# ติดตั้ง Package Python สำหรับเชื่อมต่อกับ Spark
!pip install -q findspark==1.3.0                                                         
```
- ติดตั้ง PySpark ลงใน Python
```bash
# PySpark
!pip install pyspark==3.1.2
```

## Download Data File
- wget = คำสั่งในการ download file
- wget -O = ตั้งชื่อ file
```bash
# Download Data File
!wget -O demo_data.zip https://example_data.com
!unzip demo_data.zip
```