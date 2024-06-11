import pandas as pd
import os

# Folders/variables used for running ROI
map_roi = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Monthly Executive Reports\\Raw Data\\March 2024\\mapping_table.csv"
folder_roi = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Monthly Executive Reports\\Raw Data\\March 2024"
folder_mac_roi = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Monthly Executive Reports\\Raw Data\\March 2024\\MACs"
folder_imsi_roi = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Monthly Executive Reports\\Raw Data\\March 2024\\IMSIs"


# Folders/variables used for more frequent performance dashboard
map_perf = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Python Scripts\\Final Versions\\Daily Counts Collection\\mapping_table.csv"
folder_perf = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\\Data Department\\Python Scripts\\Final Versions\\Daily Counts Collection"
folder_mac_perf = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Daily_Collections\\MACs"
folder_imsi_perf = "C:\\Users\\Local Admin\\OneDrive - Wireless Guardian\\Data Department\\Daily_Collections\\IMSIs"



def counts_aggregator(folder, info_path, dest_folder, report_type):
    # empty DataFrame objects to aggregate all of files
    master_df = pd. DataFrame()
    data_purpose = report_type
    # for loop that goes through all of the files in a folder to combine them
    files_in_folder = 0
    for filename in os.listdir(folder):
            files_in_folder += 1
    file_list = []
    for filename in os.listdir(folder):
                
        if filename.endswith("_counts.csv"):
            file_path = os.path.join(folder, filename)
            name = filename[-14:]
      
            df = pd.read_csv(file_path)
            df_mapping = pd.read_csv(info_path, encoding='latin1')

            site_list = []
            i = 0
            while i < len(df):
                j = 0
                if df.iloc[i]['SN'] == df_mapping.iloc[i]['Serial Number']:
                    site_list.append(df_mapping.iloc[i]['WGSITEID'])
                else:
                    while j < len(df_mapping):
                        if df.iloc[i]['SN'] == df_mapping.iloc[j]['Serial Number']:
                            site_list.append(df_mapping.iloc[j]['WGSITEID'])
                            break
                        elif j == len(df_mapping) - 1:
                            site_list.append('Not Found')
                            break
                        else:
                            j += 1
                i += 1

            df['site'] = site_list
            master_df = pd.concat([master_df, df], ignore_index=True) 
            file_list.append(filename)   
            print(f"\nThis {filename} just processed.\n\n{len(file_list)} files out {files_in_folder} have been processed.") 
    if data_purpose == 'roi':
        master_df.to_csv(f"{dest_folder}\\ROI_Table_{name}", index=False)
    else:
        master_df.to_csv(f"{dest_folder}\\Reporting_Table_{name}", index=False)



# ROI Counts

# counts_aggregator(folder=folder_mac_roi, info_path=map_roi, dest_folder=folder_roi, report_type='roi')
# counts_aggregator(folder=folder_imsi_roi, info_path=map_roi, dest_folder=folder_roi, report_type='roi')

# Performance Counts
counts_aggregator(folder=folder_mac_perf, info_path=map_perf, dest_folder=folder_perf, report_type='dashboard')
counts_aggregator(folder=folder_imsi_perf, info_path=map_perf, dest_folder=folder_perf, report_type='dashboard')

print("Now it's finally over!")