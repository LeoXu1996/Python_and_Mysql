import pymysql
import pandas as pd
import numpy as np
import xlwt
from collections import Counter
conn = pymysql.connect(host='localhost', user="casdev", password='123456', database='2020data', charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
sql = "select release_id,answer from iep_paper_question_answer where question_id='bb1e86e5004e4ee19bb9be0a4ac28d82'"
df = pd.read_sql(sql, con=conn)
df1 = np.array(df)  # 先使用array()将DataFrame转换一下
df2 = df1.tolist()  # 再将转换后的数据用tolist()转成列表

# cur.execute(sql)
# data = cur.fetchone()
print(df)
i=0
ins_ans=[]
f = xlwt.Workbook()
sheet1 = f.add_sheet(u'sheet1',cell_overwrite_ok=True)

for each in df.release_id:
    sql1 = "select id,institute from iep_paper_release where id=%s"%(each)
    dfs = pd.read_sql(sql1, con=conn)
    # print(dfs)
    df1s = np.array(dfs)  # 先使用array()将DataFrame转换一下
    df2s = df1s.tolist()

# # # ----------------------------for fills-----------------------------#
    ins_ans.append(df2s[0][1])
    ins_ans.append(df2[i][1])
    for j in range(len(ins_ans)):
        sheet1.write(i, j, ins_ans[j])
    ins_ans=[]

    i=i+1




# # # ----------------------------for fills end-----------------------------#



# # # ----------------------------for tables-----------------------------#

#     if i%11==0:
#         print("================================================================================================")
#         # print(df2s[0][1],df2[i][1])
#         ins_ans.append(df2s[0][1])
#         ins_ans.append(df2[i][1])
#     else:
#         # print(df2[i][1])
#         ins_ans.append(df2[i][1])
#     if i%11==10:
#         print(i,ins_ans)
#         for j in range(len(ins_ans)):
#             sheet1.write(i, j, ins_ans[j])
#         ins_ans=[]
#     i=i+1

# # # ----------------------------for tables end-----------------------------#


f.save('/Users/leixu/PycharmProjects/pythonProject/fills2.xls')
