# Set environment variable ให้ Python รู้จัก Spark
import os
os.environ["JAVA_HOME"] = "Path ที่ติดตั้ง Spark"
os.environ["SPARK_HOME"] = "Path ที่ติดตั้ง Spark"

# สร้าง Spark Session เพื่อที่จะใช้งาน Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.sql import functions as f
from pyspark.sql.function import when

# Load data(.csv) ใส่ Spark
dt = spark.read_csv('Path ที่ load data มา', header = True, inferSchema = True)

# Data Profiling
# ดูว่ามี column อะไรบ้าง
dt.printSchema()
# Show data 100 row
dt.show(100)
# นับจำนวน Row และ Column
print((dt.count(),len(dt.column)))
# สรุปข้อมููลสถิติ
dt.summary().show()
# สรุปข้อมููลสถิติเฉพาะ column ที่ระบุ
dt.select("book_id").describe().show()
# หา missing value 
dt.summary("count").show()
# แสดงข้อมูลแถว user_id ที่มี missing value
dt.where(dt.usr_id.isnull()).show()

# EDA - Exploratory Data Analysis
# หาข้อมูลแบบเฉพาะเจาะจง
dt.where(dt.price >= 1).show()
dt.where(dt.country == 'Canada').show()
dt.where(dt.timestamp.startwith("20021-04")).count()

# Data Cleansing with Spark 
# ดูว่าแต่ละ column มี Schema อะไรบ้าง
dt.printSchema()
# แปลง Data Type column timestamp(string) >> (date time)
dt.select("timestamp").show(10)
dt_clean = dt.withcolumn("timestamp", f.to_timestamp(dt.timestamp, 'yyyy-MM-dd HH:mm:ss'))
dt_clean.show()
dt_clean.printSchema()

# หาชื่อประเทศที่สะกดผิด
dt_clean.select("Country").distinct().count()
dt_clean.select("Country").distinct().sort("Country").show()
# Show ข้อมูลประเทศที่สะกดผิด
dt_clean.where(dt_clean['Country'] == 'Japane').show()
# แทนที่คำที่สะกดผิดด้วยคำที่ถูก
dt_clean_country = dt_clean.withcolume("CountryUpdate", when(dt_clean['Country'] == 'Japane','Japan').otherwise(dt_clean['Country']))
dt_clean_country.select("CountryUpdate").distinct().sort("CountryUpdate").show()
dt_clean = dt_clean_country.drop("country").withColumnRenamed('CountryUpdate', 'Country')

# ดู User ID อยู่ในรูปแบบที่ต้องการหรือไม่
dt_clean.select("user_id").show(10)
dt_clean.select("user_id").count()
# หาจำนวนของ user id ที่ตรงกับรูปแบบที่ต้องการ
dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$")).count()
# ดูข้อมูลที่ไม่ได้อยู่ในรูปแบบที่ต้องการ (8 ตัวอักษร)
dt_correct_userid = dt_clean.filter(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)
dt_incorrect_userid.show(10)
# แก้ไข user id ที่ไม่ได้อยู่ในรูปแบบที่ต้องการ
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'] == 'ca29jdsu200', 'ca29jdsu').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

# หา missing values
dt_clean.summary("count").show()
dt_clean.where(dt_clean.user_id.isNull()).show()

# หากให้แทนค่า NULL ด้วย 00000000
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id']))
dt_clean_userid.where(dt_clean_userid['user_id'].isNull()).show()
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')
dt_clean.where(dt_clean.user_id.isNull()).show()

# Save data to CSV 
dt_clean.coalesce(1).write.csv('Clean_data_single.CSV',header = True)