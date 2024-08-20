import shutil
import os


new_path = '../'
old_path = '/root/qwen-long/english/data_process_08_en_q1_2024_0801'
type_list = ['meta-en','rct-en','cohort-en','economic-en']

for pdf_type in type_list:
    new_type_path = os.path.join(new_path,pdf_type)
    old_type_path = os.path.join(old_path,pdf_type)
    os.makedirs(new_type_path, exist_ok=True)
    pdf_analyze = os.path.join(old_type_path,'pdf_data_analyze_v3.py')
    out_analyze = os.path.join(old_type_path,'out_put_analyze.py')
    shutil.copy2(pdf_analyze, new_type_path)
    shutil.copy2(out_analyze,new_type_path)
    
    
