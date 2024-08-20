import json
import os
from pathlib import Path
import time


out_folder = './error_output/'
success_out_folder = '/root/qwen-long/english/data_process_09_en_q1_2024_0812/success_all'
error_folder = os.path.join(out_folder,'error')
success_folder = success_out_folder

all_type_finish_list = os.listdir(success_folder)
error_process_id_path_list = []

for folder in os.listdir(error_folder):   
    if folder.startswith('p'):      
        folder_path = os.path.join(error_folder,folder)
        error_process_id_path_list.append(folder_path)
model_error_number = 0
pipei_error_number = 0
process_num = len(error_process_id_path_list)

for error_file_folder in error_process_id_path_list:
    model_file_path = os.path.join(error_file_folder,'_model_error_log.json')
    pipei_file_path = os.path.join(error_file_folder,'_pipei_error_log.json')
    with open(model_file_path,'r') as f:
        model_error_data = json.load(f)
    model_error_number = model_error_number+ len(model_error_data)
    with open(pipei_file_path,'r') as f:
        pipei_error_data = json.load(f)
    pipei_error_number = pipei_error_number+ len(pipei_error_data)


print(process_num,'个进程，','解析成功共计',len(all_type_finish_list))
sum = model_error_number+pipei_error_number
print('报错总个数为',sum,',model error 个数为',model_error_number,',pipei error 个数为',pipei_error_number)

with open (os.path.join(error_folder,'error_log_jiexi.txt'),'a+') as f :
    str1 = '  ' + str(process_num)+'个进程，'+',解析成功共计'+str(len(all_type_finish_list))
    str2 = ', 报错总个数为'+str(sum)+', model error 个数为'+str(model_error_number)+', pipei error 个数为'+str(pipei_error_number)
    
    
    local_time = time.localtime(time.time())  # 将时间戳转换为本地时间元组

    # 将时间元组转换为字符串，格式为 "YYYY-MM-DD HH:MM:SS"
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    
    f.write(formatted_time)  
    f.write(str1)
    f.write(str2)
    f.write('\n')
