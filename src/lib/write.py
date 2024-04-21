import os, sys, json
import pandas as pd

def filter_failures(data, output_dir, error_key='errors'):
    # for each item in data if it has an error key, write it to a jsonl file
    # otherwise yield the item
    os.makedirs(output_dir, exist_ok=True)

    for item in data:
        if 'error' in item:
            error_path = os.path.join(output_dir, f'{error_key}.jsonl')
            with open(error_path, 'a') as jsonl_file:
                jsonl_file.write(json.dumps(item) + '\n')
        else:
            yield item


def write_json_lines_partition(data, max_partition_size_mb, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    max_partition_size = max_partition_size_mb * 1024 * 1024
    
    current_partition = []
    current_partition_size = 0
    partition_count = 0
    
    for item in data:
        item_json = json.dumps(item, separators=(',', ':'))  # Convert to JSON without spaces
        item_size = sys.getsizeof(item_json)
        
        if current_partition_size + item_size > max_partition_size:
            partition_path = os.path.join(output_dir, f'partition_{partition_count}.jsonl')
            
            with open(partition_path, 'w') as jsonl_file:
                for json_item in current_partition:
                    jsonl_file.write(json_item + '\n')
            
            partition_count += 1
            current_partition = []
            current_partition_size = 0
        
        current_partition.append(item_json)
        current_partition_size += item_size
    
    if current_partition:
        partition_path = os.path.join(output_dir, f'partition_{partition_count}.jsonl')
        
        with open(partition_path, 'w') as jsonl_file:
            for json_item in current_partition:
                jsonl_file.write(json_item + '\n')

def load_jsonl_directory_pandas(directory):
    # List of JSON Lines files in the directory
    jsonl_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.jsonl')]

    # Load JSON Lines files into DataFrames
    dataframes = [pd.read_json(jsonl_file, lines=True) for jsonl_file in jsonl_files]

    # Concatenate the DataFrames
    combined_dataframe = pd.concat(dataframes, ignore_index=True)
    
    return combined_dataframe

def load_jsonl_files(directory):
    jsonl_files = [filename for filename in os.listdir(directory) if filename.endswith('.jsonl')]
    
    data = []
    for jsonl_file in jsonl_files:
        file_path = os.path.join(directory, jsonl_file)
        with open(file_path, 'r') as file:
            for line in file:
                json_data = json.loads(line)
                data.append(json_data)
    
    return data