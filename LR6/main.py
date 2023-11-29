import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os

df = pd.read_csv('data/Electric_Vehicle_Population_Data.csv')

# Змінюємо тип даних на рядковий у DataFrame 
df['Postal Code'] = df['Postal Code'].astype(str) 

# Підключення до БД, створення таблиці
con = duckdb.connect(database=':memory:', read_only=False)
con.execute('CREATE TABLE electric_cars (' +
            'VIN VARCHAR, County VARCHAR, City VARCHAR, State VARCHAR, ' +
            '"Postal Code" VARCHAR, "Model Year" INT64, Make VARCHAR, Model VARCHAR, ' +
            'ElectricVehicleType VARCHAR, CAFVEligibility VARCHAR, ' +
            'ElectricRange INT, BaseMSRP INT, LegislativeDistrict INT, ' +
            'DOLVehicleID INT, VehicleLocation VARCHAR, ElectricUtility VARCHAR, ' +
            'CensusTract INT64)')

# Вставлення даних у таблицю
con.register('df', df)
con.execute('INSERT INTO electric_cars SELECT * FROM df')

# Створення папки з кінцевим результатом
output_directory = 'output_directory'
os.makedirs(output_directory, exist_ok=True)

# 1. Кількість електромобілів у кожному місті
city_car_count = con.execute('SELECT City AS City, COUNT(*) AS CarCount FROM electric_cars GROUP BY City').fetch_df()
city_car_count.to_parquet(os.path.join(output_directory, 'city_car_count.parquet'))

# 2. 3 найпопулярніші електромобілі
top_3_cars = con.execute('SELECT Make AS Make, Model AS Model, COUNT(*) AS CarCount FROM electric_cars GROUP BY Make, Model ORDER BY CarCount DESC LIMIT 3').fetch_df()
top_3_cars.to_parquet(os.path.join(output_directory, 'top_3_cars.parquet'))

# 3. Найпопулярніший електромобіль у кожному поштовому індексі
popular_cars_by_zip = con.execute('SELECT "Postal Code" AS "Postal Code", Make AS Make, Model AS Model, COUNT(*) AS CarCount FROM electric_cars GROUP BY "Postal Code", Make, Model ORDER BY "Postal Code", CarCount DESC').fetch_df()
popular_cars_by_zip = popular_cars_by_zip.groupby(['Postal Code']).head(1)
popular_cars_by_zip.to_parquet(os.path.join(output_directory, 'popular_cars_by_zip.parquet'))

# 4. Кількість електромобілів за роками випуску та запис результату у вигляді Parquet файлів, розділених за роками
for year in df['Model Year'].unique():
    cars_by_year = df[df['Model Year'] == year]
    table = pa.Table.from_pandas(cars_by_year, preserve_index=False)
    year_output_directory = os.path.join(output_directory, f'by_year/{year}')
    os.makedirs(year_output_directory, exist_ok=True)
    pq.write_to_dataset(table, root_path=year_output_directory, partition_cols=['Model Year'])
