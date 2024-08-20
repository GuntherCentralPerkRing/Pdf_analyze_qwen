# Pdf_analyze_qwen



pdf Analyze and info withdraw with qwen-long service, support muti-process to process large amount of data at the same time.
Also uses asyncio to process multitask at the same time.

meta, rct, cohort, economic四类别文献分别有不同提取条目，分别设置不同进程数

pdf_analyze.py为解析程序 out_analyze.py为计数，错误统计程序

一键setup在countdata文件夹内，可实现一键部署整个项目到不同位置，运行不同批次。

