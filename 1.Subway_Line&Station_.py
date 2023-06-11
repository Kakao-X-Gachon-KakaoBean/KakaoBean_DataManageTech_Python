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

subway_line_query = """
INSERT into subway_line(id, line_name)
VALUES (%s, %s)
"""

line_data = [[1, "1호선"], [2, "2호선"], [3, "3호선"], [4, "4호선"], [5, "5호선"], [6, "6호선"], [7, "7호선"], [8, "8호선"]]

for row in line_data:
    cursor.execute(subway_line_query, row)


# TODO: SUBWAY_STATION_QUERY____________________________________________________________________________________________

# 쿼리 설정
subway_station_query = """
INSERT into subway_station(id, station_name, subway_line_id)
VALUES (%s, %s, %s)
"""

# 필요 데이터만 추출, 중복 제거
station_data = data22[['고유역번호(외부역코드)', '역명', '호선']].drop_duplicates(subset=['고유역번호(외부역코드)']).reset_index(drop=True)
station_data.columns = ['역번호', '역명', '호선']
# 22, 21은 호선 이 int64
station_data['호선'] = station_data['호선'].astype(str) + "호선"

# 다른 연도 데이터
test21 = data21[['역번호', '역명', '호선']].drop_duplicates(subset=['역번호']).reset_index(drop=True)
test21['호선'] = test21['호선'].astype(str) + "호선"
# 20 은 object
test20 = data20[['역번호', '역명', '호선']].drop_duplicates(subset=['역번호']).reset_index(drop=True)

# 20, 21, 22 역 데이터 합치기
concatenated_df = pd.concat([station_data, test21, test20], axis=0)
concatenated_df = concatenated_df.drop_duplicates(subset=['역번호']).reset_index(drop=True)
print(concatenated_df)
# 역번호 역명 호선( int . object . object )

# subway_line 테이블에서 일치하는 값 찾기
# 참조 테이블 쿼리
line_sub_query = "SELECT id, line_name FROM subway_line"
cursor.execute(line_sub_query)


for line_id, line_name in cursor:
    mask = concatenated_df['호선'] == line_name
    concatenated_df.loc[mask, 'subway_line_id'] = line_id

# 테이블에 넣을 값만 crop
concatenated_df = concatenated_df[['역번호', '역명', 'subway_line_id']]

# index 초기화
concatenated_df = concatenated_df.reset_index(drop=True)

# 실제 table 의 column name 과 통일 시키기
mysql_columns = ['id', 'station_name', 'subway_line_id']
concatenated_df.columns = mysql_columns

# 데이터 넣기
for row in concatenated_df.itertuples(index=False):
    cursor.execute(subway_station_query, row)

# TODO: END OF QUERY____________________________________________________________________________________________________

print("done execution")

# 변경 사항 커밋
connection.commit()

# 연결 종료
cursor.close()
connection.close()