import xlrd,pymysql
conn = pymysql.connect(host='localhost', user="casdev", password='123456', database='calcutest', charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
sql = "insert into test (scoring_type,question_id,blank_id,answer,user_id,full_mark,detail,Rule) " \
      "values (%s,%s,%s,%s,%s,%s,%s,%s)"

file = xlrd.open_workbook("/Users/leixu/Desktop/data.xlsx")
sheet_1 = file.sheet_by_index(0) #根据sheet页的排序选取sheet
row_content = sheet_1.row_values(0) #获取指定行的数据，返回列表，排序自0开始
row_number = sheet_1.nrows #获取有数据的最大行数
for i in range(2,row_number):
    scoring_type = sheet_1.cell(i,0).value
    question_id = sheet_1.cell(i,1).value
    blank_id = sheet_1.cell(i,2).value
    answer = sheet_1.cell(i,3).value
    user_id = sheet_1.cell(i,4).value
    full_mark=sheet_1.cell(i,5).value
    detail=sheet_1.cell(i,6).value
    Rule=sheet_1.cell(i,7).value
    values = (scoring_type,question_id,blank_id,answer,user_id,full_mark,detail,Rule)

    cursor.execute(sql,values)
    conn.commit()
cursor.close()
conn.close()