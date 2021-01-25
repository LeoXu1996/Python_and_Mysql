import pymysql
import pandas as pd
import numpy as np
class Nstr:
    def __init__(self, arg):
       self.x=arg
    def __sub__(self,other):
        c=self.x.replace(other.x,"")
        return c

conn = pymysql.connect(host='localhost', user="casdev", password='123456', database='2019data', charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
sql = "select distinct (question_id) from iep_exam_detail where question_id not in(select choice_question_id from iep_question_score_choice_rule union select tiankong_question_id from iep_question_score_rule) "
df = pd.read_sql(sql, con=conn)
df1 = np.array(df)  # 先使用array()将DataFrame转换一下
df2 = df1.tolist()
df3=[]
for each in df2:
    if each not in df3:
        df3.append(each)
print(len(df3))
count = 0
type1='expert'
for i in df3:
    sql_1 = "select answer from iep_history_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_answer = pd.read_sql(sql_1, con=conn)
    sql_1_1 = "select release_id from iep_history_paper_question_answer " \
              "where question_id= '%s'" % (i[0])
    df_answer_user = pd.read_sql(sql_1_1, con=conn)
    if len(df_answer) != 0:
        print('---------------------------------------------------------------------------------------------')
        print(i)
    sql_2 = "select answer from iep_history_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_answer_a = pd.read_sql(sql_2, con=conn)
    df_answer_all = np.array(df_answer_a)  # 先使用array()将DataFrame转换一下
    df2_answer_all_1 = df_answer_all.tolist()
    #
    #
    sql_3 = "select blank_id from iep_history_paper_question_answer " \
            "where question_id= '%s'" % (i[0])
    df_blank_a = pd.read_sql(sql_3, con=conn)
    df_blank_all = np.array(df_blank_a)  # 先使用array()将DataFrame转换一下
    df_blank_all_1 = df_blank_all.tolist()
    #
    sql_4 = "select release_id from iep_history_paper_question_answer " \
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
    sql_6 = "select score from iep_history_question_indicator " \
            "where quest_id= '%s'" % (i[0])
    df_full_a = pd.read_sql(sql_6, con=conn)
    df_full_all = np.array(df_full_a)  # 先使用array()将DataFrame转换一下
    df_full_all_1 = df_full_all.tolist()
    print(df_full_all_1)
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
    singlecount = 0
    fullm = df_full_all_1
    for x in range(len(df2_answer_all_1)):
        # print(type,df_release_all_1[x][0],i,df_blank_all_1[x][0],df2_answer_all_1[x][0],detail2,fullmark[0])
        m = Nstr(df_blank_all_1[x][0])
        n = Nstr(i[0])
        p = Nstr("_")
        blank_index = Nstr(m - n) - p
        # print(blank_index)
        # sql_5 = "select score from iep_question_score_rule " \
        #         "where tiankong_question_id= '%s' and blank_index=%s" % (i[0], blank_index)
        # df_score_a = pd.read_sql(sql_5, con=conn)
        # df_score_all = np.array(df_score_a)  # 先使用array()将DataFrame转换一下
        # df_score_all_1 = df_score_all.tolist()
        scores = ''
        # for each in df_score_all_1[::-1]:
        #     if fullm <= each[0]:
        #         fullm = each[0]
        #     scores = scores + str(each[0]) + ','
        #
        # if fullm > 0 and len(df_score_all_1) == 1:
        #     type1 = 'auto_notNull'
        #     Rule = ''
        # else:
        #     type1 = 'auto_rise'
        #     Rule = 'off'
        # print(type1,df_release_all_1[x][0],i[0],df_blank_all_1[x][0],df2_answer_all_1[x][0],scores,'\t',fullm,'\t\t','单个题目答案数',singlecount,'总数',count)
        Rule=''
        print(type1, df_release_all_1[x][0], i[0], df_blank_all_1[x][0], df2_answer_all_1[x][0], scores, '\t', fullm)

        singlecount += 1

        sql = "insert into test (scoring_type,question_id,blank_id,answer,user_id,full_mark,detail,Rule) " \
              "values (%s,%s,%s,%s,%s,%s,%s,%s)"
        scoring_type = type1
        question_id = i[0]
        blank_id = df_blank_all_1[x][0]
        answer = df2_answer_all_1[x][0]
        user_id = df_release_all_1[x][0]
        full_mark = fullm
        detail = scores
        #         for each in detail2:
        #             detail=detail+str(each)+','
        values = (scoring_type, question_id, blank_id, answer, user_id, full_mark, detail, Rule)
        #         print(scoring_type,user_id,i,blank_id,answer,detail,full_mark)
        #
        cursor.execute(sql, values)
        conn.commit()
        count += 1
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
print(count)
conn.close()
