# aggregator.py
import traceback
import os
import zipfile
import csv
from collections import defaultdict
import yaml

"""
Processes the 'Table data.csv' file within the 'Content*.zip' archive
  - csv_reader is the currently open csv file reader
  - folder_name is the name corresponding to the video being processed
"""
def process_content_table_data(csv_reader, all_video_data, folder_name):
    header = next(csv_reader, None)
    if header:
        data_row = next(csv_reader, None)
        views = data_row[1]
        watch_time_hours = data_row[2]
        subs = data_row[3]
        avg_view_duration = data_row[4]
        impressions = data_row[5]
        impressions_ctr = data_row[6]
        all_video_data[folder_name].append((folder_name, views, watch_time_hours, subs, avg_view_duration, impressions, impressions_ctr))

"""
Processes the 'Table data.csv' file within the 'Traffic source*.zip' archive
  - csv_reader is the currently open csv file reader
  - folder_name is the name corresponding to the video being processed
"""
def process_traffic_source_table_data(csv_reader, all_video_data, folder_name):
    sources_to_track = { "Browse features", "Channel pages", "Suggested videos", "YouTube search", "External", "Direct or unknown" }
    data_by_source = defaultdict(list)
    header = next(csv_reader, None)
    if header:
        for data_row in csv_reader:
            source_name = data_row[0]
            if source_name not in sources_to_track:
                continue

            views = data_row[1]
            watch_time_hours = data_row[2]
            avg_view_duration = data_row[3]
            impressions = data_row[4]
            impressions_ctr = data_row[5]

            data_by_source[source_name].extend([source_name, views, watch_time_hours, avg_view_duration, impressions, impressions_ctr])

        # combine 'external' and 'direct or unknown' as one entry
        external_data = data_by_source["External"]
        direct_or_unknown_data = data_by_source["Direct or unknown"]
        sum_of_views = int(external_data[1] or 0) + int(direct_or_unknown_data[1] or 0)

        # TODO if I need to do CTR aggregation again, some logic is already written here. But turns out external sources have empty CTR (makes sense, can't track)
        # sum_of_impressions = int(external_data[4] or 0) + int(direct_or_unknown_data[4] or 0)
        # combined_ctr = (int(external_data[4])/sum_of_impressions * float(external_data[5])) + (int(direct_or_unknown_data[4])/sum_of_impressions * float(direct_or_unknown_data[5]))
        # rounded_ctr = round(combined_ctr, 2)
        
        external_plus_data = (sum_of_views, "", "")

        collated = (folder_name,)
        collated = collated + (data_by_source["Browse features"][1], data_by_source["Browse features"][4], data_by_source["Browse features"][5])
        collated = collated + (data_by_source["Channel pages"][1], data_by_source["Channel pages"][4], data_by_source["Channel pages"][5])
        collated = collated + (data_by_source["Suggested videos"][1], data_by_source["Suggested videos"][4], data_by_source["Suggested videos"][5])
        collated = collated + (data_by_source["YouTube search"][1], data_by_source["YouTube search"][4], data_by_source["YouTube search"][5])
        collated = collated + external_plus_data

        all_video_data[folder_name].append(collated)

def process_content_data(root_directory, reports_directory):
    """
    Processes zipped CSV files in subdirectories of the root directory.
    """
    all_content_video_data = defaultdict(list)
    all_traffic_source_video_data = defaultdict(list)
    for folder_name in os.listdir(root_directory):
        folder_path = os.path.join(root_directory, folder_name)
        if os.path.isdir(folder_path):  # Ensure it's a directory
            for filename in os.listdir(folder_path):
                if filename.endswith(".zip"):
                    is_content_archive = filename.startswith("Content")
                    is_traffic_source_archive = filename.startswith("Traffic source")
                    zip_file_path = os.path.join(folder_path, filename)

                    if not (is_content_archive or is_traffic_source_archive):
                        continue

                    try:
                        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                            # Assuming only one CSV file per zip
                            csv_filenames = [f.filename for f in zip_ref.filelist if f.filename.endswith(".csv")]

                            if not csv_filenames:
                                print(f"No CSV files found in {zip_file_path}")
                                continue

                            table_data_csv_filename = next(x for x in csv_filenames if x.startswith("Table data"))
                            with zip_ref.open(table_data_csv_filename) as csv_file:
                                # Read and process the table data CSV data
                                csv_content_bytes = csv_file.read() #read bytes
                                csv_content_string = csv_content_bytes.decode('utf-8') #decode bytes to string
                                csv_reader = csv.reader(csv_content_string.splitlines()) #create csv reader
                                if (is_content_archive):
                                    process_content_table_data(csv_reader, all_content_video_data, folder_name)
                                elif (is_traffic_source_archive):
                                    process_traffic_source_table_data(csv_reader, all_traffic_source_video_data, folder_name)
                    except zipfile.BadZipFile:
                        print(f"Error: {zip_file_path} is not a valid zip file.")
                    except Exception as e:
                        print(f"An error occurred processing {zip_file_path}: {e}")

    content_aggregate_data = []
    content_aggregate_report_header = ["Video ID", "Views", "Watch time (hours)" , "Subscribers", "Average view duration", "Impressions", "Impressions click-through rate (%)"]
    content_aggregate_data.append(content_aggregate_report_header)
    for video_id, row_data in all_content_video_data.items():
        if (row_data):
            content_aggregate_data.append(*row_data)

    content_aggregate_report_file_path = os.path.join(reports_directory, "Aggregate - Content - Table data.csv")
    with open(content_aggregate_report_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(content_aggregate_data)

    traffic_source_aggregate_data = []
    traffic_source_aggregate_report_header = ["Video ID", "Browse Views", "Browse Impressions", "Browse CTR", "Channel pages Views", "Channel pages Impressions", "Channel pages CTR", "Suggested videos Views", "Suggested videos Impressions", "Suggested videos CTR", "YouTube search Views", "YouTube search Impressions", "YouTube search CTR", "External+ Views", "External+ Impressions", "External+ CTR"]
    traffic_source_aggregate_data.append(traffic_source_aggregate_report_header)
    for video_id, row_data in all_traffic_source_video_data.items():
        if (row_data):
            traffic_source_aggregate_data.append(*row_data)

    traffic_source_aggregate_report_file_path = os.path.join(reports_directory, "Aggregate - Traffic source - Table data.csv")
    with open(traffic_source_aggregate_report_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(traffic_source_aggregate_data)

def load_config(config_file):
    """Loads configuration from a YAML file."""
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None

def main():
    config = load_config("config.yaml")  # Load configuration from config.yaml
    if config and "root_directory" and "reports_directory" in config:
        root_directory = config["root_directory"]
        reports_directory = config["reports_directory"]
        process_content_data(root_directory, reports_directory)
        print("Aggregation complete!")
    else:
        print("Error: Root directory or reports directory not specified in config.yaml")

if __name__ == "__main__":
    main()
