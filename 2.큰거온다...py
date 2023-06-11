from datetime import datetime

import mysql.connector
import pandas as pd

# 출력 옵션 설정
pd.set_option("display.max_rows", None)  # 모든 행 표시
pd.set_option("display.max_columns", None)  # 모든 열 표시

# csv import
data22 = pd.read_csv("22subway.csv", encoding="cp949")
data21 = pd.read_csv("21subway.csv", encoding="cp949")
data20 = pd.read_csv("20subway.csv", encoding="cp949")

# change code to int
data22['고유역번호(외부역코드)'] = data22['고유역번호(외부역코드)'].apply(lambda x: int(x) if str(x).isdigit() else x)
data21['역번호'] = data21['역번호'].apply(lambda x: int(x) if str(x).isdigit() else x)
data20['역번호'] = data20['역번호'].apply(lambda x: int(x) if str(x).isdigit() else x)

# MySQL 서버에 연결
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="datatech",
    charset="utf8"
)

# 커서 생성
cursor = connection.cursor()

# TODO: SUBWAY_LINE_QUERY_______________________________________________________________________________________________
#
# subway_line_query = """
# INSERT into subway_line(id, line_name)
# VALUES (%s, %s)
# """
#
# line_data = [[1, "1호선"], [2, "2호선"], [3, "3호선"], [4, "4호선"], [5, "5호선"], [6, "6호선"], [7, "7호선"], [8, "8호선"]]
#
# for row in line_data:
#     cursor.execute(subway_line_query, row)
#

# TODO: SUBWAY_STATION_QUERY____________________________________________________________________________________________

# # 쿼리 설정
# subway_station_query = """
# INSERT into subway_station(id, station_name, subway_line_id)
# VALUES (%s, %s, %s)
# """
#
# # 필요 데이터만 추출, 중복 제거
# station_data = data22[['고유역번호(외부역코드)', '역명', '호선']].drop_duplicates(subset=['고유역번호(외부역코드)']).reset_index(drop=True)
# station_data.columns = ['역번호', '역명', '호선']
# # 22, 21은 호선 이 int64
# station_data['호선'] = station_data['호선'].astype(str) + "호선"
#
# # 다른 연도 데이터
# test21 = data21[['역번호', '역명', '호선']].drop_duplicates(subset=['역번호']).reset_index(drop=True)
# test21['호선'] = test21['호선'].astype(str) + "호선"
# # 20 은 object
# test20 = data20[['역번호', '역명', '호선']].drop_duplicates(subset=['역번호']).reset_index(drop=True)
#
# # 20, 21, 22 역 데이터 합치기
# concatenated_df = pd.concat([station_data, test21, test20], axis=0)
# concatenated_df = concatenated_df.drop_duplicates(subset=['역번호']).reset_index(drop=True)
# print(concatenated_df)
# # 역번호 역명 호선( int . object . object )
#
# # subway_line 테이블에서 일치하는 값 찾기
# # 참조 테이블 쿼리
# line_sub_query = "SELECT id, line_name FROM subway_line"
# cursor.execute(line_sub_query)
#
#
# for line_id, line_name in cursor:
#     mask = concatenated_df['호선'] == line_name
#     concatenated_df.loc[mask, 'subway_line_id'] = line_id
#
# # 테이블에 넣을 값만 crop
# concatenated_df = concatenated_df[['역번호', '역명', 'subway_line_id']]
#
# # index 초기화
# concatenated_df = concatenated_df.reset_index(drop=True)
#
# # 실제 table 의 column name 과 통일 시키기
# mysql_columns = ['id', 'station_name', 'subway_line_id']
# concatenated_df.columns = mysql_columns
#
# # 데이터 넣기
# for row in concatenated_df.itertuples(index=False):
#     cursor.execute(subway_station_query, row)
#
# # # 해당 내용이 있는지 확인하기
# # filtered_df = station_data[station_data['id'] == 2750]
# # print(filtered_df)
# #
# # # 해당 내용을 다른 내용으로 바꿔주기
# # station_data['id'] = station_data['id'].replace(' ', 2750)
# # print(station_data)
# #
# # # 빈 값이 있는지 확인하기
# # missing_values = station_data.isnull().sum()
# # print(missing_values)

# TODO: SUBWAY_USAGE_QUERY____________________________________________________________________________________________

# id, 하차, 승차, precipitation id, subway_station_id

# 1 번 # TODO: 데이터 합치기
# 데이터 가져오기
rename20 = data20[
    ['날짜', '호선', '역번호', '역명', '구분', '06:00 이전', '06:00 ~ 07:00', '07:00 ~ 08:00', '08:00 ~ 09:00', '09:00 ~ 10:00',
     '10:00 ~ 11:00', '11:00 ~ 12:00', '12:00 ~ 13:00', '13:00 ~ 14:00', '14:00 ~ 15:00', '15:00 ~ 16:00',
     '16:00 ~ 17:00', '17:00 ~ 18:00', '18:00 ~ 19:00', '19:00 ~ 20:00', '20:00 ~ 21:00', '21:00 ~ 22:00',
     '22:00 ~ 23:00', '23:00 ~ 24:00']]
rename21 = data21[
    ['날짜', '호선', '역번호', '역명', '구분', '06시 이전', '06시-07시', '07시-08시', '08시-09시', '09시-10시', '10시-11시', '11시-12시',
     '12시-13시', '13시-14시', '14시-15시', '15시-16시', '16시-17시', '17시-18시', '18시-19시', '19시-20시', '20시-21시', '21시-22시',
     '22시-23시', '23시 이후']]
rename22 = data22[
    ['수송일자', '호선', '고유역번호(외부역코드)', '역명', '승하차구분', '06시이전', '06-07시간대', '07-08시간대', '08-09시간대', '09-10시간대', '10-11시간대',
     '11-12시간대', '12-13시간대', '13-14시간대', '14-15시간대', '15-16시간대', '16-17시간대', '17-18시간대', '18-19시간대', '19-20시간대',
     '20-21시간대', '21-22시간대', '22-23시간대', '23-24시간대']]
# 컬럼명 통일
new_column_name = ['날짜', '호선', '역번호', '역명', '구분', '5시-6시', '6시-7시', '7시-8시', '8시-9시', '9시-10시', '10시-11시',
                   '11시-12시', '12시-13시', '13시-14시', '14시-15시', '15시-16시', '16시-17시', '17시-18시', '18시-19시', '19시-20시',
                   '20시-21시', '21시-22시', '22시-23시', '23시-24시']

rename20.columns = new_column_name
rename21.columns = new_column_name
rename22.columns = new_column_name

# 호선 object 형식으로 통일
rename22.loc[:, '호선'] = rename22['호선'].astype(str) + "호선"
rename21.loc[:, '호선'] = rename21['호선'].astype(str) + "호선"


# 날짜 형식 변환
rename22['날짜'] = pd.to_datetime(rename22['날짜'], format='%Y.%m.%d')

# 20, 21, 22 역 데이터 합치기
concatenated_df = pd.concat([rename20, rename21, rename22], axis=0)
# print(concatenated_df.shape)  # 데이터 개수 20 - 202280 21 - 204374 22 - 199080 : 총 605734개

# 2 번 # TODO: 승차 하차 컬럼 분할

# 승하차 구분
onBoard = concatenated_df[concatenated_df['구분'].str.contains('승차')]
offBoard = concatenated_df[concatenated_df['구분'].str.contains('하차')]
# print(onBoard.shape)
# print(offBoard.shape)

# 한 날짜에 한 역 당 하나의 행으로 표 변환
oneline_onoff = pd.merge(onBoard, offBoard, on=['날짜', '역번호'], how='inner')

# 날짜 형식 변환 -왜 두번,,,
oneline_onoff['날짜'] = pd.to_datetime(oneline_onoff['날짜'], infer_datetime_format=True)

# 날짜에 대한 id 열 추가
date_sub_query = "SELECT id, date FROM date"
cursor.execute(date_sub_query)

for date_id, date in cursor:
    mask = oneline_onoff['날짜'] == date
    oneline_onoff.loc[mask, 'date_id'] = date_id

# 시간대별 승하차 열을 순차적으로 합치고 열 이름 변경하여 새로운 열 생성
df_new = pd.DataFrame()
for i in range(5, 24):  # 변경 해야할 열의 개수에 맞게 설정
    df_temp = oneline_onoff[['날짜', '호선_x', '역번호', '역명_x', 'date_id', f'{i}시-{i + 1}시_x', f'{i}시-{i + 1}시_y']].rename(
        columns={'호선_x': '호선', '역명_x': '역명', f'{i}시-{i + 1}시_x': 'on_board', f'{i}시-{i + 1}시_y': 'off_board'})
    df_temp['time_stamp_id'] = i + 8  # 'i' 열 추가
    df_new = pd.concat([df_new, df_temp])

# 총 605734개/2 *19(시간대 별) = 5754473개 행이 존재

# TODO: testtestestestestestest# TODO: testtestestestestestest# TODO: testtestestestestestest

# Precipitation 테이블 참조
precipitation_sub_query = "SELECT id, date_id, time_stamp_id FROM precipitation"
cursor.execute(precipitation_sub_query)

for id, date_id, time_stamp_id in cursor:
    mask = (df_new['date_id'] == date_id) & (df_new['time_stamp_id'] == time_stamp_id)

    if mask.sum() == 0:
        print("No such row found", date_id, time_stamp_id)
    else:
        df_new.loc[mask, 'precipitation_id'] = id
        # 필터링된 유일한 행에 대한 추가 작업 수행

#
# print(df_new.head())
# TODO: testtestestestestestest# TODO: testtestestestestestest# TODO: testtestestestestestest


# # 결측치가 있는 행 찾기
# missing_rows = df_new[df_new['precipitation_id'].isna()]
# print(missing_rows.shape)

final_result = df_new[['off_board', 'on_board', 'precipitation_id', '역번호']]
# id 부여
final_result = final_result.assign(id=range(1, len(final_result) + 1))
# 역번호 열 이름 변경
final_result = final_result.rename(columns={'역번호': 'subway_station_id'})
# 순서 변경
new_column_order = ['id', 'off_board', 'on_board', 'precipitation_id', 'subway_station_id']
final_result = final_result.reindex(columns=new_column_order)

# print(final_result.head(20))z

# # 결측치가 있는 행 찾기
# missing_rows = final_result[final_result['precipitation_id'].isna()]
# print(missing_rows.head(100))


final_result = final_result.fillna(0)

print("done preprocess")

# 데이터 넣기
subway_usage_query = """
INSERT into subway_usage(id, off_board, on_board, precipitation_id, subway_station_id)
VALUES (%s, %s, %s, %s, %s)
"""

for row in final_result.itertuples(index=False):
    cursor.execute(subway_usage_query, row)
print("done execution")

# 변경 사항 커밋
connection.commit()

# 연결 종료
cursor.close()
connection.close()
