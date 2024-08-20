import json
import os



def read_finished_dbid_list():
    # /root/qwen-long/english/data_process_08_en_q1_2024_0801/cohort-en/output/success/2022/3
    folder_list = ['data_process_08_en_q1_2024_0801']
    type_list = ['meta-en','rct-en','cohort-en','economic-en']
    count_num = 0
    fini_list = []
    for folder in folder_list:
        root_folder_path = os.path.join('/root/Data2/ZXN/qwen-long/english',folder)
        for type in type_list:
            
            root_folder_type_out_success_path = os.path.join(root_folder_path,type,'output/success')
            for year in os.listdir(root_folder_type_out_success_path):
                root_folder_type_out_success_year_path = os.path.join(root_folder_type_out_success_path,year)
                type_num = os.listdir(root_folder_type_out_success_year_path)[0]
                root_folder_type_out_success_year_num_path = os.path.join(root_folder_type_out_success_year_path,type_num)
                
                for pdf in os.listdir(root_folder_type_out_success_year_num_path):
                    pdf_path = os.path.join(root_folder_type_out_success_year_num_path,pdf)
                    dbid = pdf[:-5]
                    fini_list.append(dbid)
                    # print(dbid)
                    # input()
                    
                    count_num+=1
    
    print('共计dbid个数',count_num)
    return fini_list


def readlist_to_dic(path):
    all_dic = {}
    n = 0
    with open(path,'r+',encoding='utf-8')as f:
        for line in f:
            partition1 = line.rpartition('/')[2]
            dbid = partition1.rpartition('.')[0]
            path = line.strip()
            all_dic[dbid] = path
            # all_dic[path] = dbid
            # print(all_dic)
            # input()
            n += 1
    print('循环次数：',n)   
    return all_dic


if __name__=='__main__':

    fini_dbid_list = read_finished_dbid_list()
    print('len fini_list',len(fini_dbid_list))

    orig_data_folder = '/root/qwen-long/english/data_process_08_en_q1_2024_0801/data_count/q1_path.txt'
    all_dic_ = readlist_to_dic(orig_data_folder)
    print('len all_list',len(all_dic_))
    count = 0
    with open('./dedup.txt','w',encoding='utf-8') as f:

        for k,v in all_dic_.items():
            if k not in fini_dbid_list:
                f.write(str(v) + '\n')
                count += 1
                # print(v)
                if count % 5000 == 0:
                    print('非重复pdf个数',count)
    print('非重复：',count)
    print('sum:',len(all_dic_))
    print('fini:',len(fini_dbid_list))
    print('是否数量之和正确:',bool(count+len(fini_dbid_list)==len(all_dic_)))


     

    

    
