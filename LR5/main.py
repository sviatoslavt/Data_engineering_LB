import os 
import io 
import pandas as pd 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, date_format, desc, dense_rank 
from pyspark.sql.window import Window 
from zipfile import ZipFile 
from datetime import timedelta 
import shutil

def process_trip_data(trips_df): 
    average_trip_duration_per_day(trips_df) 
    trips_count_per_day(trips_df) 
    most_popular_starting_station_per_month(trips_df) 
    top_three_stations_per_day_last_two_weeks(trips_df) 
    average_trip_duration_by_gender(trips_df) 
    age_stats_top_ten(trips_df) 

def average_trip_duration_per_day(trips_df): 
    result_df = ( 
        trips_df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day")) 
        .agg({"tripduration": "avg"}) 
        .orderBy("day") 
    ) 
    write_csv(result_df, "average_trip_duration_per_day") 

def trips_count_per_day(trips_df): 
    result_df = ( 
        trips_df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day")) 
        .count() 
        .orderBy("day") 
    ) 
    write_csv(result_df, "trips_count_per_day") 

def most_popular_starting_station_per_month(trips_df): 
    result_df = ( 
        trips_df.withColumn("month", date_format("start_time", "yyyy-MM")) 
        .groupBy("month", "from_station_name") 
        .count() 
        .orderBy("month", col("count").desc()) 
        .groupBy("month") 
        .agg({"from_station_name": "first"}) 
        .orderBy("month") 
        .withColumnRenamed("first(from_station_name)", "most_popular_starting_station") 
    ) 
    write_csv(result_df, "most_popular_starting_station_per_month") 

def top_three_stations_per_day_last_two_weeks(trips_df): 
    df = filter_last_two_weeks(trips_df) 
    station_counts = df.groupBy("from_station_id", "from_station_name").count() 
    window_spec = Window.orderBy(desc("count")) 
    ranked_stations = station_counts.withColumn("rank", dense_rank().over(window_spec)) 
    top_stations = ranked_stations.filter(col("rank") <= 3) 
    write_csv(top_stations, "top_three_stations_per_day_last_two_weeks") 

def filter_last_two_weeks(trips_df): 
    df = trips_df.withColumn("start_time", col("start_time").cast("timestamp")) 
    end_date = df.agg({"start_time": "max"}).collect()[0][0] 
    start_date = end_date - timedelta(days=14) 
    return df.filter((col("start_time") >= start_date) & (col("start_time") <= end_date)) 

def average_trip_duration_by_gender(trips_df): 
    result_df = ( 
        trips_df.groupBy("gender") 
        .agg({"tripduration": "avg"}) 
        .orderBy("gender") 
    ) 
    write_csv(result_df, "average_trip_duration_by_gender") 

def age_stats_top_ten(trips_df): 
    result_df = ( 
        trips_df.groupBy("birthyear") 
        .agg({"tripduration": "avg"}) 
        .orderBy(col("avg(tripduration)").desc()) 
        .limit(10) 
    ) 
    write_csv(result_df, "age_stats_top_ten") 

def write_csv(dataframe, folder_name): 
    folder_path = f"reports/{folder_name}" 
    dataframe.write.csv(folder_path, header=True, mode="overwrite") 

def create_zip_archive(folder_name): 
    shutil.make_archive(f'reports/{folder_name}', 'zip', f'reports/{folder_name}') 

def zip_extract(x): 
    in_memory_data = io.BytesIO(x[1]) 
    file_obj = ZipFile(in_memory_data, "r") 
    csv_data = {} 

    for file in file_obj.namelist(): 
        if file.lower().endswith('.csv'): 
            csv_content = file_obj.read(file) 
            csv_data[file] = pd.read_csv(io.StringIO(csv_content.decode('ISO-8859-1'))) 

    return csv_data 

def process_zip_files(data_path): 
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate() 
    sc = spark.sparkContext 
    zips = sc.binaryFiles(f"{data_path}/*.zip") 

    files_data = zips.map(zip_extract).collect() 

    for data in files_data: 
        for _, content in data.items(): 
            if 'merged_df' not in locals(): 
                merged_df = spark.createDataFrame(content) 

    merged_df.show() 
    process_trip_data(merged_df) 
    spark.stop()

def main(): 
    data_folder = 'data' 
    current_directory = os.getcwd() 
    data_path = os.path.join(current_directory, data_folder) 
    process_zip_files(data_path) 

    create_zip_archive('average_trip_duration_per_day') 
    create_zip_archive('trips_count_per_day') 
    create_zip_archive('most_popular_starting_station_per_month') 
    create_zip_archive('top_three_stations_per_day_last_two_weeks') 
    create_zip_archive('average_trip_duration_by_gender') 
    create_zip_archive('age_stats_top_ten')

if __name__ == "__main__": 
    main()
