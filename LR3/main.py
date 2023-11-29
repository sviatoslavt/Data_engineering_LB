import glob 
import json 
import csv 
import os 
from flatten_json import flatten 
 
def process_json_file(json_path): 
    with open(json_path, "r") as json_file: 
        my_json = json.load(json_file) 
         
        flattened_json = [] 
        if isinstance(my_json, list): 
            flattened_json.extend(flatten(item) for item in my_json if isinstance(item, dict)) 
        elif isinstance(my_json, dict): 
            flattened_json.append(flatten(my_json)) 
 
    return flattened_json 
 
def main(): 
    root_directory = 'data' 
 
    json_paths = [file for file in glob.glob( 
        os.path.join(root_directory, '**', '*.json'), recursive=True)] 
 
    for json_path in json_paths: 
        flattened_json = process_json_file(json_path) 
 
        csv_path = os.path.splitext(json_path)[0] + '.csv' 
        with open(csv_path, 'w', newline='') as csv_file: 
            csv_writer = csv.writer(csv_file) 
 
            header = flattened_json[0].keys() if flattened_json else [] 
            csv_writer.writerow(header) 
 
            for item in flattened_json: 
                csv_writer.writerow(item.values()) 
 
if __name__ == "__main__": 
    main()