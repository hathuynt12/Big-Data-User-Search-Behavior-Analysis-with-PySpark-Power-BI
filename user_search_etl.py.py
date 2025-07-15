from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql import SQLContext
from pyspark.sql.window import Window 
import pyodbc
import pandas as pd
import os 
import datetime 
spark = SparkSession.builder.config("spark.driver.memory","10g").getOrCreate()

#tim most_search
def most_search(df):
    data=df.select('user_id','keyword')
    data=data.groupBy('user_id','keyword').count()
    data=data.withColumnRenamed('count','TotalSearch')
    data=data.orderBy('user_id',ascending = False)
    window=Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    data=data.withColumn('Rank',row_number().over(window))
    data=data.filter(col('Rank')==1)
    data=data.withColumnRenamed("keyword","most_search")
    data=data.select("user_id","most_search")
    return data

#date_list
def generate_date(start_time, end_time):
    date_list = pd.date_range(start=start_time, end=end_time ).strftime('%Y%m%d').to_list()
    return date_list
#import to mysql
def import_to_mysql(result):
    url = 'jdbc:mysql://' + {MySQL_HOST} + ':' + {MySQL_POST} + '/' + 'bigdata'
    driver = "com.mysql.cj.jdbc.Driver"
    user = {MySQL_USER}
    password = {MySQL_PASSWORD}
    result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','project_mostsearch').option('user',user).option('password',password).mode('overwrite').save()
    return print("Data Import Successfully")

#main_task
def main_task(start_date_1, end_date_1, start_date_2, end_date_2):
    path = 'E:\\bigdata\\Dataset\\log_search\\Total_File\\'
    list_file_1 =generate_date(start_date_1,end_date_1)
    list_file_2 =generate_date(start_date_2,end_date_2)

    file_name1= spark.read.parquet(path+list_file_1[0]+'.parquet')
    for i in list_file_1[1:]:
        file_name11 = spark.read.parquet(path+i+'.parquet')
        file_name1=file_name1.union(file_name11)
        file_name1.cache()

    final1= most_search(file_name1)
    final1=final1.withColumnRenamed("most_search","most_search_t6")
    print('final1')
    final1.show()

    file_name2=spark.read.parquet(path+list_file_2[0]+'.parquet')
    for j in list_file_2[1:]:
        file_name22=spark.read.parquet(path+j+'.parquet')
        file_name2=file_name2.union(file_name22)
        file_name2.cache()

    final2=most_search(file_name2)
    final2=final2.withColumnRenamed("most_search","most_search_t7")
    print('---------------------')
    print('final2')
    final2.show()

    # Hợp nhất dữ liệu tháng 6 và tháng 7 bằng user_id, chỉ lấy những user có tìm kiếm trong cả hai tháng.
    joined_user = final1.join(final2, on = 'user_id', how = 'inner')
    print('Lấy những user xuất hiện trong cả most_search_t6 và most_search_t7')
    print('----------------------------')
    print('joined_user: ')
    joined_user.show()

    #Đọc danh mục từ khóa (keyword_search) =>File này giúp xác định từ khóa thuộc nhóm danh mục nào.

    key_category=spark.read.csv("E:\\bigdata\\LabBTVN\\key_search_by_category.csv",header=True)

    # Ghép danh mục từ khóa vào dữ liệu tìm kiếm

    category_t6=joined_user.join(key_category, joined_user.most_search_t6 == key_category.Most_Search ,how='inner')
    category_t6 = category_t6.select('user_id','most_search_t6','Category')
    category_t6 = category_t6.withColumnRenamed('Category','category_t6')
    print('----------------------------')
    print('category_t6: ')
    category_t6.show(20)

    category_t7 = joined_user.join(key_category,joined_user.most_search_t7 == key_category.Most_Search,'inner' )
    category_t7 = category_t7.select('user_id','most_search_t7','Category').withColumnRenamed('Category','category_t7')
    print('----------------------------')
    print('category_t7: ')
    category_t7.show()

    result = category_t6.join(category_t7,on = 'user_id',how='inner')
    result = result.select('user_id','most_search_t6','category_t6','most_search_t7','category_t7')
    result = result.withColumn('category_change',when((col('category_t6')==col('category_t7')),'unchanged')
                                .otherwise(concat_ws(' and ',col('category_t6'),col('category_t7'))))
    print('----------------------------')
    print('result: ')
    result.show()
    import_to_mysql(result)
    return print('Finish job')

#execute
main_task('20220601','20220614','20220701','20220714')





