import pymysql
import pandas as pd
import numpy as np
from collections import Counter

class Nstr:
    def __init__(self, arg):
       self.x=arg
    def __sub__(self,other):
        c=self.x.replace(other.x,"")
        return c


conn = pymysql.connect(host='localhost', user="casdev", password='123456', database='jiahui', charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
sql = "select question_id from iep_question_score_rule"
df = pd.read_sql(sql, con=conn)
df1 = np.array(df)  # 先使用array()将DataFrame转换一下
df2 = df1.tolist()  # 再将转换后的数据用tolist()转成列表

# cur.execute(sql)
# data = cur.fetchone()
df3=[]
for each in df2:
    if each not in df3:
        df3.append(each)
print(len(df3))

# print(type(df1))
# print(type(df2))

# cou={}
# for i in df2:
#     if df2.count(i)>1:
#         cou[i]=df2.count(i)
# print(cou)
# df3 = []
# for i in df2:
#     for j in i:
#         df3.append(j)
#
# result = Counter(df3)
# answers = []
count = 0
type1='fill'
# # print("result:",result)
# # print(result)
for i in df3:
    sql_1 = "select answer from iep_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_answer = pd.read_sql(sql_1, con=conn)
    sql_1_1 = "select release_id from iep_paper_question_answer " \
              "where question_id= '%s'" % (i[0])
    df_answer_user = pd.read_sql(sql_1_1, con=conn)
    if len(df_answer) != 0:
        print('---------------------------------------------------------------------------------------------')
        print(i)
        # df1_answer = np.array(df_answer)  # 先使用array()将DataFrame转换一下
        # df2_answer = df1_answer.tolist()
        # # print(df2_answer)
        # df1_answer_user = np.array(df_answer_user)  # 先使用array()将DataFrame转换一下
        # df2_answer_user = df1_answer_user.tolist()
#         # print(df2_answer_user)
#         if result[i] == 1:
#             result[i] += 1
#         print(result[i])
#         usercount = 0
#         for j in df2_answer:
# #             answerindex = []
# #             for each in j[0]:
# #                 if each != ',':
# #                     answerindex.append(int(each))
# #
# # #-----------------------------Binary Process-----------------------------#
# # # ----------------------------Use this part first-----------------------------#
# #
#             # print('用户', df2_answer_user[usercount], '答案',j[0],'\t\t','单个题目答案数',usercount,'总数',count)
#             count+=1
#             # answer = ''
#             # index = 0
#             # u=0
#             # while index<result[i]:
#             #     if u<len(answerindex):
#             #         if index==answerindex[u]:
#             #             answer=answer+'1,'
#             #             index+=1
#             #             u+=1
#             #         else:
#             #             answer=answer+'0,'
#             #             index+=1
#             #     else:
#             #         answer = answer + '0,'
#             #         index += 1
#             # print('二进制写法', answer)
#             # sql_2 = "update iep_history_paper_question_answer set answer=%(answer)s where blank_id= %(id)s and release_id=%(user)s"
#             #
#             # values={"answer":answer,"id":i,'user':df2_answer_user[usercount][0]}
#             # cursor.execute(sql_2,values)
#             # conn.commit()
#
# # -----------------------------Binary Process End-----------------------------#
#
# # -----------------------------Data Process and Writing into new datasaet-----------------------------#
#
            # usercount += 1
    sql_2 = "select answer from iep_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_answer_a = pd.read_sql(sql_2, con=conn)
    df_answer_all = np.array(df_answer_a)  # 先使用array()将DataFrame转换一下
    df2_answer_all_1 = df_answer_all.tolist()
#
#
    sql_3 = "select blank_id from iep_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_blank_a = pd.read_sql(sql_3, con=conn)
    df_blank_all = np.array(df_blank_a)  # 先使用array()将DataFrame转换一下
    df_blank_all_1 = df_blank_all.tolist()
#
    sql_4 = "select release_id from iep_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_release_a = pd.read_sql(sql_4, con=conn)
    df_release_all = np.array(df_release_a)  # 先使用array()将DataFrame转换一下
    df_release_all_1 = df_release_all.tolist()
#
    # sql_5 = "select score from iep_question_score_rule " \
    #         "where tiankong_question_id= '%s' and blank_id=%s" % (i[0]) %(blank_index)
    # df_score_a = pd.read_sql(sql_5, con=conn)
    # df_score_all = np.array(df_score_a)  # 先使用array()将DataFrame转换一下
    # df_score_all_1 = df_score_all.tolist()
#
    sql_6 = "select score from iep_question_indicator " \
            "where quest_id= '%s'" % (i[0])
    df_full_a = pd.read_sql(sql_6, con=conn)
    df_full_all = np.array(df_full_a)  # 先使用array()将DataFrame转换一下
    df_full_all_1 = df_full_all.tolist()
#     if len(df_full_all_1)>0:
#         fullmark=df_full_all_1[0]
#
#     detail1=[]
#     detail2=[]
#     for each in df_score_all_1:
#         detail1.append(each[0])
#     # print(detail1)
#     check=fullmark[0]/result[i]
#     # print(check)
#     n=1
#     for each in detail1:
#         # print(each,check*n)
#         if each==round(check*n,2):
#             n+=1
#     # print(n,result[i])
#     if n-1==result[i]:
#         for each in detail1:
#             detail2.append(check)
#     else:
#         detail2=detail1
#     # print(detail2)
#     # print(detail)
#     # fullmark=sum(detail)
#
#     # for each in df2_answer_all_1:
#     #     print('答案',each)
#     #     answers.append(each[0])
#     # conn1 = pymysql.connect(host='localhost', user="casdev", password='123456', database='calcutest', charset='utf8')
#     # cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
    singlecount=0
    for x in range(len(df2_answer_all_1)):
        # print(type,df_release_all_1[x][0],i,df_blank_all_1[x][0],df2_answer_all_1[x][0],detail2,fullmark[0])
        m = Nstr(df_blank_all_1[x][0])
        n = Nstr(i[0])
        p=Nstr("_")
        blank_index=Nstr(m-n)-p
        # print(blank_index)
        sql_5 = "select score from iep_question_score_rule " \
                "where question_id= '%s' and blank_index=%s" % (i[0],blank_index)
        df_score_a = pd.read_sql(sql_5, con=conn)
        df_score_all = np.array(df_score_a)  # 先使用array()将DataFrame转换一下
        df_score_all_1 = df_score_all.tolist()
        scores=''
        fullm=0
        for each in df_score_all_1:
            if fullm<=each[0]:
                fullm=each[0]
            scores=scores+str(each[0])+','

        if fullm>0 and len(df_score_all_1)==1:
            type1='auto_notNull'
            Rule=''
        else:
            type1='auto_rise'
            Rule = 'off'
        # print(type1,df_release_all_1[x][0],i[0],df_blank_all_1[x][0],df2_answer_all_1[x][0],scores,'\t',fullm,'\t\t','单个题目答案数',singlecount,'总数',count)
        print(type1,df_release_all_1[x][0],i[0],df_blank_all_1[x][0],df2_answer_all_1[x][0],scores,'\t',fullm)

        singlecount+=1

        sql = "insert into test (scoring_type,question_id,blank_id,answer,user_id,full_mark,detail,Rule) " \
              "values (%s,%s,%s,%s,%s,%s,%s,%s)"
        scoring_type = type1
        question_id = i[0]
        blank_id =df_blank_all_1[x][0]
        answer = df2_answer_all_1[x][0]
        user_id = df_release_all_1[x][0]
        full_mark = fullm
        detail=scores
#         for each in detail2:
#             detail=detail+str(each)+','
        values = (scoring_type, question_id, blank_id, answer, user_id, full_mark, detail,Rule)
#         print(scoring_type,user_id,i,blank_id,answer,detail,full_mark)
#
        cursor.execute(sql, values)
        conn.commit()
        count+=1
#     # cursor1.close()
#     # conn1.close()
#
# # -----------------------------Data Process and Writing into new datasaet
# # END-----------------------------#
#
#
#
# # print(len(answers))
#         # print(i,"  done")
# print(count)
conn.close()
