import json
import os
from pathlib import Path
import openai
import multiprocessing
import asyncio
import PyPDF2
import pdfplumber
import random
import time

# task1
task1_prompt = 'You are an academic assistant who is good at reading medical literature and summarizing the main points of articles. Your task is to read and understand medical literature in depth and extract key information from it. Please generate question-answer pairs based on the full text of the medical literature provided\n\
Requirements:\n\
1. Summarize the question-answer pairs from different angles. The summarized question-answer pairs should cover some essential elements of reading medical literature, including but not limited to the following points: the background, purpose, research object, research method (the method description should be detailed and specific), research results (if there are corresponding data, please list them), research conclusions (the conclusions should be comprehensive) and deficiencies of the research (if not mentioned in the text, no need to answer), etc.\n\
2. Make sure your answers are detailed, specific, and completely based on the content of the literature. <The answer to each question should not be less than 300 words!>. Readers can understand the content of the literature research by reading these question-answer pairs. \n\
3. The output format is Q1:xxx\nA1:xxx\n\nQ2:xxx\nA2:xxx...\n\n...\n\n\
4. Be careful not to output unnecessary explanations or comments beyond the content of the document. '
# task2  
task2_prompt = 'You are an academic assistant who is good at reading and refining medical literature. Your task is to read this literature and then analyze the clinical problems and clinical conclusions in the literature. Please present your analysis results in the form of multiple sets of question and answer pairs based on the full text of the medical literature provided\n\
Requirements:\n\
1. <Your question and answer pair must be an independent clinical conclusion, and should not refer to the concept in the literature>. Your conclusion must be extracted from the literature, but <must be unrelated to the details of the research process of this literature>, and questions such as "Are there limitations to the research in the literature?" should not appear. At the same time, the description of the questions and answers raised should not be expressed in a tone targeting the current literature information (do not use words such as "this article", "in the literature", "this study", "this literature" targeting the current literature), but should be more general and universal to describe the problems and answers of clinical research, and answer in English.\n\
2. Under the premise of ensuring quality, the more question and answer pairs generated, the better.\n\
3. Make sure your answer is detailed and specific, and completely based on the content of the literature, <the answer to each question should not be less than 300 words!>. Readers can obtain independent medical clinical research conclusions by reading these questions and answers. \n\
4. The output format is Q1:xxx\nA1:xxx\n\nQ2:xxx\nA2:xxx...\n\n...\n\n\
5. Be careful not to output redundant explanations or comments. '


#cohort prompt task3
task3_info_00 = '''
1.Basic information
1.1 title:\n
1.2 publication year (only year):\n
1.3 the last name of the first author:\n
1.4 study country, location and setting:\n
1.5 recruitment period:\n
1.6 foundation and funding:\n

2.Exposure
2.1 exposure ("exposure" can be lifestyle factors (such as smoking or diet), environmental factors (such as pollution or radiation), genetic factors, or any other variables that might impact the health outcome.):\n
2.2 exposure levels and the standards for categorizing:\n
2.3 information source or measurement of exposure:\n
2.4 eligibility criteria for exposure group:\n
2.5 sources and methods of selection of participants:\n
2.6 eligibility criteria for non-exposure group:\n

3.Follow-up
3.1 Follow-up time (eg, average and total amount):\n
3.2 Follow-up method:\n
3.3 Loss of follow (number and reason):\n

4.Study method
4.1 Outcomes:\n
4.2 Assessment or diagnostic criteria of the outcomes:\n
4.3 Determination of sample size:\n
4.4 Statistical method (control for confounding, examine subgroups and interaction, address the missing data, and perform sensitivity analysis):\n

5.Study results
5.1 Ratio of males to females in exposure group:\n
5.2 Ratio of males to females in non-exposure group:\n
5.3 Age in exposure group:\n
5.4 Age in non-exposure group:\n
5.5 Whether the characteristics of the baseline were similar or balanced?:\n
5.6 number of participants in exposure group:\n
5.7 number of participants in non-exposure group:\n
5.8 Numbers of individuals at each stage of study:\n
5.9 Numbers of outcome events in exposure group:\n
5.10 Numbers of outcome events in non-exposure group:\n
5.11 Unadjusted estimates (eg, RR or HR, and 95%CI):\n
5.12 Confounder-adjusted estimates(eg, RR or HR, and 95%CI):\n
5.13 corresponding adjusted confounders:\n
5.14 Data source and measurement of confounders:\n
5.15 Other analyses (eg, analyses of subgroups and interactions, and sensitivity analyses):\n
5.16 Main findings:\n
5.17 Limitation:\n
'''

#cohort
task3_info = '''
1.Basic information
1.1 study country, location and setting:\n
1.2 recruitment period:\n

2.Exposure
2.1 Exposure ("exposure" can be lifestyle factors (such as smoking or diet), environmental factors (such as pollution or radiation), genetic factors, or any other variables that might impact the health outcome.):\n
2.2 information source or measurement of exposure:\n
2.3 sources and methods of selection of participants:\n

3.Follow-up
3.1 Follow-up time (eg, average and total amount):\n
3.2 Loss of follow (number and reason):\n

4.Study method
4.1 Outcomes:\n
4.2 Assessment or diagnostic criteria of the outcomes:\n

5.Study results
5.1 Number of participants in exposure group:\n
5.2 Number of participants in non-exposure group:\n
5.3 Numbers of outcome events in exposure group:\n
5.4 Numbers of outcome events in non-exposure group:\n
5.5 Confounder-adjusted estimates (eg, RR or HR, and 95%CI):\n
5.6 Corresponding adjusted confounders:\n
5.7 Main findings:\n
5.8 Limitation:\n
'''


#prompt task3
task3_prompt = 'Basing on the given questions, please search for answer for each question in the document, and answer the questions with the original information from document. Do not omit the blanks of each item, and do not abbreviate it into the form of see so and so. If the original text does not mention the item, the item will be marked as "not mentioned". \nOutput according to the format of the given questions, and do not change the order, number or content of the given questions:\nThe questions are:\n' + task3_info

openai_api_key = "xxxxxxxx"

#异步的qwen模型调用
async def async_qwen_long(file, prompt, client):
    completion = await client.chat.completions.create(
        model="qwen-long",
        messages=[
            {'role': 'system',
            'content': 'You are an academic assistant who is good at reading medical literature and summarizing the main points of the text. Answer in english.'
            },
            {'role': 'system',
            'content': f'fileid://{file.id}'
            },
            {'role': 'user',
            'content': prompt
            }
            ],
        stream=False
    )
    result = completion.choices[0].message.content
    return result
    

#读取pdf列表的函数
def readlist(path,want_type_num):
    type_list = []
    with open(path,'r+',encoding='utf-8')as f:
        for line in f:
            file_type_num = line.rpartition('/')[0].rpartition('/')[2]
            if str(want_type_num)==file_type_num:
                type_list.append(line.strip())
    random.shuffle(type_list)         
    return type_list


# def is_pdf_broken(pdf_file_path):
#     try:
#         with open(pdf_file_path, 'rb') as file:
#             reader = PyPDF2.PdfReader(file)
#             # 如果没有抛出异常，那么文件很可能没有问题
#             # 可以尝试读取第一页，如果文件损坏，这里可能会抛出异常
#             first_page = reader.pages[0]
#     except PyPDF2.errors.PdfReadError as e:
#         print(f"Error reading PDF: {e}")
#         return True
#     except Exception as e:
#         print(f"Error reading PDF: {e}")
#         return True
#     return False

def is_pdf_broken(pdf_file_path):
    try:
        with pdfplumber.open(pdf_file_path) as pdf:
            first_page = pdf.pages[0]
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return True
    return False

async def model_process(p_index,batch,new_file_folder_path,error_log_folder_path):
    pdf_list = batch
    pdf_sum_number = len(batch)
    # jiexi_success_pdf_number + model_error_number + pipei_error_number = sum_jiexi_number
    jiexi_success_pdf_number = 0
    model_error_number = 0
    pipei_error_number = 0

    #创建error_log文件夹
    error_log_folder = error_log_folder_path
    os.makedirs(error_log_folder,exist_ok=True)

    #model error的log文件
    error_log_file_model = os.path.join(error_log_folder,'_model_error_log.json')
    error_list_model = []

    #pipei error的log文件
    error_log_file_pipei = os.path.join(error_log_folder,'_pipei_error_log.json')
    error_list_pipei = []

    #file_broken的log文件
    error_log_file_broken = os.path.join(error_log_folder,'_file_broken_error_log.json')
    error_list_file_broken = []

    with open(error_log_file_model,'w',encoding='utf-8') as f:
        json.dump(error_list_model,f,ensure_ascii=False,indent=2)
    with open(error_log_file_pipei,'w',encoding='utf-8') as f:
        json.dump(error_list_pipei,f,ensure_ascii=False,indent=2)
    with open(error_log_file_broken,'w',encoding='utf-8') as f:
        json.dump(error_list_file_broken,f,ensure_ascii=False,indent=2)

    for _pdf_path in pdf_list:
        # print(_pdf_path)
        split1 = _pdf_path.rpartition('/')
        # print(split1)
        _pdf = split1[-1]
        # print(_pdf)
        split2 = split1[0].rpartition('/')
        _num = split2[-1]
        # print(_num)
        split3 = split2[0].rpartition('/')
        _year = split3[-1]
        # print(_year)
        out_dic_for_each_pdf = {}
        # print(_pdf_path)

         #创建year下面的num文件夹
        new_path_year_num = os.path.join(new_file_folder_path,_year,_num)
        os.makedirs(new_file_folder_path,exist_ok=True)
        
        new_path_year_num_pdf = os.path.join(new_file_folder_path,_pdf.replace('.pdf', '.json'))
        #断点检测，如果此json文件存在且非空，则说明已经有结果，不需要再跑此pdf。每次重新跑都把error_log删掉，这样可以覆盖错误的pdf
        if os.path.exists(new_path_year_num_pdf) and os.path.isfile(new_path_year_num_pdf) and os.path.getsize(new_path_year_num_pdf) > 0:
            continue
        
        if is_pdf_broken(_pdf_path):
            with open(error_log_file_broken,'r',encoding='utf-8') as f:
                _err_list = json.load(f)
            _err_list.append(_pdf_path)
            with open(error_log_file_broken, 'w', encoding='utf-8') as f:
                json.dump(_err_list,f,ensure_ascii=False,indent=2)
            continue
        
        try:
            try:
                #创建openai client
                client = openai.AsyncOpenAI(
                    api_key=openai_api_key,
                    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
            except Exception as e:
                print(str(e))
                print('1---获取client连接时报错')
                raise
            try:
                file = await client.files.create(file=Path(_pdf_path), purpose="file-extract")
            except Exception as e:
                print(str(e))
                print('上传文件时报错')
                raise
            try:
                try:
                    _task1 = asyncio.create_task(async_qwen_long(file, task1_prompt, client))
                    _task2 = asyncio.create_task(async_qwen_long(file, task2_prompt, client))
                    _task3 = asyncio.create_task(async_qwen_long(file, task3_prompt, client))
                    
                    task1_result, task2_result, task3_result = await asyncio.gather(_task1, _task2, _task3)
                    await client.files.delete(file.id)
                except Exception as e:
                    print('file parsing, retry after 10s')
                    time.sleep(10)
                    _task1 = asyncio.create_task(async_qwen_long(file, task1_prompt, client))
                    _task2 = asyncio.create_task(async_qwen_long(file, task2_prompt, client))
                    _task3 = asyncio.create_task(async_qwen_long(file, task3_prompt, client))
                    
                    # task1_result, task2_result, task3_result = await asyncio.gather(_task1, _task2, _task3)
                    results = await asyncio.gather(_task1, _task2, _task3)
                    (task1_result,id1), (task2_result,id2 ),(task3_result,id3) = results
                    await client.files.delete(file.id)
            except Exception as e:
                await client.files.delete(file.id)
                print(str(e))
                raise
        except Exception as e:
            error_dic = {}
            error_dic['file_name'] = _pdf_path
            error_dic['error_part'] = '获取模型服务，模型处理输入时报错'
            error_dic['error_info'] = str(e)
            
            with open(error_log_file_model,'r',encoding='utf-8') as f:
                _err_list = json.load(f)
            _err_list.append(error_dic)
            with open(error_log_file_model, 'w', encoding='utf-8') as f:
                json.dump(_err_list,f,ensure_ascii=False,indent=2)
            model_error_number += 1

            print('进程编号：', p_index, ',本进程总pdf数:',pdf_sum_number,',解析成功总数：', jiexi_success_pdf_number, ',按规则解析失败累计：', pipei_error_number, ',模型推理失败累计：', model_error_number)
            continue
        try:
            task1_dic = {}
            task2_dic = {}
            task3_dic = {}

            out_dic_for_each_pdf['file_name'] = _pdf_path
            out_dic_for_each_pdf['task1'] = task1_dic
            out_dic_for_each_pdf['task2'] = task2_dic
            out_dic_for_each_pdf['task3'] = task3_dic

            #task1和task2的模型输出处理
            _qa_list_task1 = task1_result.split('\n\n')
            _qa_list_task2 = task2_result.split('\n\n')

            qa_list_task1 = [line for line in _qa_list_task1 if len(line)>0 and line[0]=='Q']
            qa_list_task2 = [line for line in _qa_list_task2 if len(line)>0 and line[0]=='Q']

            for qa in qa_list_task1:
                q,a = qa.split('\n')[0:2]
                task1_dic[q] = a
            for qa in qa_list_task2:
                q,a = qa.split('\n')[0:2]
                task2_dic[q] = a
                        
            #task3的模型输出处理
            _qa_list_task3 = task3_result.split('\n')
            qa_list_task3 = [line for line in _qa_list_task3 if len(line)>0 and line[0].isdigit() and line[2].isdigit()]
            
            #异常判断
            if qa_list_task1 == [] or qa_list_task2 == [] or qa_list_task3 == []:
                print('正则匹配，截取后为空')
                raise Exception
            
            for qa in qa_list_task3:
                q = qa.split(':')[0]
                a = qa.split(':')[1]
                task3_dic[q] = a
            
            with open(new_path_year_num_pdf, 'w+', encoding='utf-8') as f:
                json.dump(out_dic_for_each_pdf, f, ensure_ascii=False, indent=2)
            jiexi_success_pdf_number +=1
            print('进程编号：', p_index, ',本进程总pdf数:',pdf_sum_number,',解析成功总数：', jiexi_success_pdf_number, ',按规则解析失败累计：', pipei_error_number, ',模型推理失败累计：', model_error_number)

        except Exception as e:
            #
            print('解析模型结果报错：',str(e))
            
            print('file，-----------:',_pdf_path,'ids:')
            # print(str(e))
            error_dic = {}
            error_dic['file_name'] = _pdf_path
            error_dic['error_part'] = '解析匹配模型输出时报错'
            error_dic['error_info'] = str(e)
            error_dic['task1'] = task1_result
            error_dic['task2'] = task2_result
            error_dic['task3'] = task3_result

            with open(error_log_file_pipei,'r',encoding='utf-8') as f:
                _err_list = json.load(f)
            _err_list.append(error_dic)
            with open(error_log_file_pipei, 'w', encoding='utf-8') as f:
                json.dump(_err_list,f,ensure_ascii=False,indent=2)
            pipei_error_number += 1
            print('进程编号：', p_index, ',本进程总pdf数:',pdf_sum_number,',解析成功总数：', jiexi_success_pdf_number, ',按规则解析失败累计：', pipei_error_number, ',模型推理失败累计：', model_error_number)
            continue

def run_async_in_process(p_number,batch,new_file_folder_path,error_log_folder_path):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # 运行协程
        loop.run_until_complete(model_process(p_number,batch,new_file_folder_path,error_log_folder_path))
    finally:
        # 清理工作
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == '__main__':

    orig_data_folder = '/root/Data2/qwen-long/english/data_process_09_en_q1_2024_0812/countdata/dedup.txt'
    process_type = 3
    output_folder = './error_output/'
    all_pdf_list_type2 = readlist(orig_data_folder,process_type)
    # print(len(all_pdf_list_type2))
    # input()

    #cohort数量 302534
    num_batches = 100
    batch_size = 3026
    process_num = list(range(num_batches))
    

    last_batch_size = len(all_pdf_list_type2) % batch_size
    
    batch_list = []
    _error_log_folder_list = []
    for i in range(num_batches):
        #数据分批
        start_index = i * batch_size
        end_index = (i + 1) * batch_size if i < num_batches - 1 else start_index + last_batch_size
        # 当前批次的数据
        current_batch = all_pdf_list_type2[start_index:end_index]
        batch_list.append(current_batch)

        error_log_folder_name = 'process_' + str(i).zfill(2) + '_error_log'
        _error_log_folder_path = os.path.join(output_folder,'error',error_log_folder_name)
        os.makedirs(_error_log_folder_path,exist_ok=True)
        _error_log_folder_list.append(_error_log_folder_path)
    
    success_path = '/root/Data2/ZXN/qwen-long/english/data_process_09_en_q1_2024_0812/'
    success_out_folder = os.path.join(success_path,'success_all')

    # 创建并启动多个进程
    processes = []
    for p_number,batch,error_folder in zip(process_num, batch_list, _error_log_folder_list):
        # 在这里向多进程传递batch_list中每一个batch，存储新pdf的文件夹路径，报错文件夹路径
        p = multiprocessing.Process(target=run_async_in_process,args=(p_number,batch,success_out_folder,error_folder))
        p.start()
        processes.append(p)

    # 等待进程都结束
    for p in processes:
        p.join()


