# naver_finance.py

import requests
import pandas as pd

URL = 'https://finance.naver.com/api/sise/etfItemList.nhn'

class NaverFinanceAPI:
    @classmethod    
    def get_etf_item_list(cls):
        # 데이터 요청
        response = requests.get(URL)
        data = response.json().get('result').get('etfItemList')
        # 테이블화 & 컬럼 정리
        df = pd.DataFrame(data)
        df.columns = ['종목코드', '카테고리', '종목명', '현재가', '등락여부', '전일비', '등락률', 'NAV', '3개월수익률', '거래량', '거래대금', '시가총액']
        # 변수 변환
        df.등락여부 = df.등락여부.map({'2': '▲', '5': '▼'})
        df.카테고리 = df.카테고리.map({
            1: '국내 시장지수', 2: '국내 업종/테마', 3: '국내 파생',
            4: '해외 주식', 5: '원자재', 6: '채권', 7: '기타'})
        # 타입 처리
        df = df.astype({
            '현재가': int, '등락률': float, 'NAV': int, '3개월수익률': float,
            '거래량': int, '거래대금': int, '시가총액': int,
        }).set_index('종목코드')
        return df

if __name__ == '__main__':
    print(NaverFinanceAPI.get_etf_item_list())