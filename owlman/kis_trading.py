import time
from multiprocessing import Pool, cpu_count

import requests
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.cluster import AgglomerativeClustering
import plotly.express as px

class KISTrading:
    '''https://apiportal.koreainvestment.com/apiservice/'''
    domain = 'https://openapi.koreainvestment.com:9443'

    def __init__(self,
                 appkey, appsecret, CANO, ACNT_PRDT_CD,
                 access_token=None):
        self.appkey = appkey
        self.appsecret = appsecret
        if not access_token:
            self.access_token = self.get_access_token()
        else:
            self.access_token = access_token
        self.CANO = CANO
        self.ACNT_PRDT_CD = ACNT_PRDT_CD

    def get_access_token(self) -> str:
        '''
        #### 접속 토큰 발급
        '''
        URL = f'{self.domain}/oauth2/tokenP'
        json = dict(
            grant_type='client_credentials',
            appkey=self.appkey,
            appsecret=self.appsecret)
        try:
            res = requests.post(URL, json=json)
            if res.status_code != 200:
                err_msg = f'Request Error ({res.status_code}) : {res.text}'
                raise Exception(err_msg)
            access_token = res.json()['access_token']
            return access_token
        except Exception as ex:
            print(type(ex), ex)

    def get_headers(self, tr_id, tr_cont='') -> dict:
        return {
            'content-type': 'application/json; charset=utf-8',
            'authorization': f'Bearer {self.access_token}',
            'appkey': self.appkey,
            'appsecret': self.appsecret,
            'tr_id': tr_id,
            'custtype': 'P',
            'tr_cont': tr_cont,
        }

    @property
    def default_params(self):
        return dict(CANO=self.CANO,
                    ACNT_PRDT_CD=self.ACNT_PRDT_CD)

    def get_account_balance(self) -> pd.DataFrame:
        '''
        #### 투자계좌 자산현황 조회
        '매입금액'(0), '평가금액'(1), '평가손익금액'(2),
        '신용대출금액'(3), '실제순자산금액'(4), '전체비중율'(5)
        '''
        URL = f'{self.domain}/uapi/domestic-stock/v1/trading/inquire-account-balance'
        params = dict(
            INQR_DVSN_1='',
            BSPR_BF_DT_APLY_YN='',
            **self.default_params,
        )
        try:
            res = requests.get(URL, params=params,
                                    headers=self.get_headers('CTRP6548R'))
            if res.status_code != 200:
                print(res.json())
                err_msg = f'Request Error ({res.status_code}) : {res.text}'
                raise Exception(err_msg)
            data = res.json()
            columns = ['매입금액', '평가금액', '평가손익금액',
                       '신용대출금액', '실제순자산금액', '전체비중율']
            df = pd.DataFrame(data['output1'],)
            df.columns = columns
            df.index = [
                '주식', '펀드/MMW', '채권', 'ELS/DLS', 'WRAP',
                '신탁/퇴직연금/외화신탁', 'RP/발행어음', '해외주식', '해외채권',
                '금현물', 'CD/CP', '단기사채', '타사상품', '외화단기사채',
                '외화 ELS/DLS', '외화', '예수금+CMA', '청약자예수금', '<합계>']
            df = df.astype('float')
            return df
        except Exception as ex:
            print(type(ex), ex)

    def get_stock_account(self, simple=True):
        '''
        #### 주식잔고 조회
        '상품번호'(0), '상품명'(1), '매매구분명'(2), '전일매수수량'(3), '전일매도수량'(4),
        '금일매수수량'(5), '금일매도수량'(6), '보유수량'(7), '주문가능수량'(8),
        '매입평균가격'(9), '매입금액'(10), '현재가'(11), '평가금액'(12), '평가손익금액'(13),
        '평가손익율'(14), '평가수익율'(15), '대출일자'(16), '대출금액'(17), '대주매각대금'(18),
        '만기일자'(19), '등락율'(20), '전일대비증감'(21), '종목증거금율명'(22),
        '보증금율명'(23), '대용가격'(24), '주식대출단가'(25)
        '''
        URL = f'{self.domain}/uapi/domestic-stock/v1/trading/inquire-balance'
        params = dict(
            **self.default_params,
            AFHR_FLPR_YN='N', OFL_YN='', INQR_DVSN='02', UNPR_DVSN='01',
            FUND_STTL_ICLD_YN='N', FNCG_AMT_AUTO_RDPT_YN='N',
            PRCS_DVSN='01', CTX_AREA_FK100='', CTX_AREA_NK100='')
        try:
            res = requests.get(URL, params=params,
                                    headers=self.get_headers('TTTC8434R'))
            if res.status_code != 200:
                print(res.json())
                err_msg = f'Request Error ({res.status_code}) : {res.content}'
                raise Exception(err_msg)
            data = res.json()
            df = pd.DataFrame(data['output1'],)
            df.columns = [
                '상품번호', '상품명', '매매구분명', '전일매수수량', '전일매도수량',
                '금일매수수량', '금일매도수량', '보유수량', '주문가능수량',
                '매입평균가격', '매입금액', '현재가', '평가금액', '평가손익금액',
                '평가손익율', '평가수익율', '대출일자', '대출금액', '대주매각대금',
                '만기일자', '등락율', '전일대비증감', '종목증거금율명',
                '보증금율명', '대용가격', '주식대출단가']
            df.set_index('상품번호', inplace=True)
            df = df.astype({'보유수량': int, '매입평균가격': float,
                '현재가': int, '평가손익금액': int})
            return df.iloc[:, [0, 6, 8, 10, 12, 11]]\
                    .loc[df.보유수량 > 0] if simple else df
        except Exception as ex:
            print(type(ex), ex)

    def get_daily_price(self, symbol, period='D'):
        '''
        #### 주식현재가 일자별
        '''
        URL = f'{self.domain}/uapi/domestic-stock/v1/quotations/inquire-daily-price'
        params = dict(
            FID_COND_MRKT_DIV_CODE='J', FID_INPUT_ISCD=symbol,
            FID_PERIOD_DIV_CODE=period, FID_ORG_ADJ_PRC=0)
        try:
            res = requests.get(URL, params=params,
                                    headers=self.get_headers('FHKST01010400'))
            if res.status_code != 200:
                print(res.json())
                err_msg = f'Request Error ({res.status_code}) : {res.content}'
                raise Exception(err_msg)
            data = res.json()
            df = pd.DataFrame(data.get('output'))
            df.columns = [
                '영업일자', '시가', '고가', '저가', '종가', '거래량',
                '전일대비거래량비율', '전일대비', '전일대비부호', '전일대비율',
                '외국인소진율', '외국인순매수', '락구분코드', '누적분할비율']
            df['영업일자'] = pd.to_datetime(df['영업일자'], format='%Y%m%d')
            df.set_index('영업일자', inplace=True)
            df = df.astype({
                '시가': int, '고가': int, '저가': int, '종가': int,
                '전일대비거래량비율': float, '전일대비': int, '전일대비율': float,
                '외국인소진율': float, '외국인순매수': int, '누적분할비율': float})\
                .sort_index()
            return df
        except Exception as ex:
            print(type(ex), ex)
    
    def is_holiday(self, base_date):
        '''
        #### 국내휴장일조회
        '''
        URL = f'{self.domain}/uapi/domestic-stock/v1/quotations/chk-holiday'
        params = dict(
            BASS_DT=base_date,
            CTX_AREA_NK='', CTX_AREA_FK='')
        try:
            res = requests.get(URL, params=params,
                                    headers=self.get_headers('CTCA0903R'))
            if res.status_code != 200:
                print(res.json())
                err_msg = f'Request Error ({res.status_code}) : {res.content}'
                raise Exception(err_msg)
            data = res.json()
            df = pd.DataFrame(data.get('output'))
            return df.bzdy_yn.eq('N').iloc[0]
        except Exception as ex:
            print(type(ex), ex)
    
    def get_daily_all_orders(self,
            INQR_STRT_DT, INQR_END_DT,
            SLL_BUY_DVSN_CD='00', INQR_DVSN='01',
            PDNO='', CCLD_DVSN='01', simple=True):
        orders = []
        key1 = ''
        key2 = ''
        while True:
            order, cont, key1, key2 = self\
            .get_daily_order(
                INQR_STRT_DT, INQR_END_DT,
                SLL_BUY_DVSN_CD, INQR_DVSN,
                PDNO, CCLD_DVSN, key1, key2, simple)
            orders.append(order)
            if cont == 'D' or cont == 'E':
                return pd.concat(orders)
            
    def get_daily_order(self,
            INQR_STRT_DT, INQR_END_DT,
            SLL_BUY_DVSN_CD='00', INQR_DVSN='01',
            PDNO='', CCLD_DVSN='01',
            CTX_AREA_FK100='', CTX_AREA_NK100='', simple=True):
        '''
        #### 주식일별주문체결조회
        * INQR_STRT_DT : 조회시작일자 (YYYYMMDD)
        * INQR_END_DT : 조회종료일자 (YYYYMMDD)
        * SLL_BUY_DVSN_CD : 매도매수구분코드 (00: 전체, 01: 매도, 02: 매수)
        * INQR_DVSN : 조회구분 (00: 역순, 01: 정순)
        * PDNO : 종목번호(6자리)
        * CCLD_DVSN : 체결구분 (00: 전체, 01: 체결, 02: 미체결)
        '''
        URL = f'{self.domain}/uapi/domestic-stock/v1/trading/inquire-balance'
        params = dict(
            **self.default_params,
            INQR_STRT_DT=INQR_STRT_DT, INQR_END_DT=INQR_END_DT,
            SLL_BUY_DVSN_CD=SLL_BUY_DVSN_CD, INQR_DVSN=INQR_DVSN,
            PDNO=PDNO, CCLD_DVSN=CCLD_DVSN,
            ORD_GNO_BRNO='', ODNO='',
            INQR_DVSN_3='00', INQR_DVSN_1='', 
            CTX_AREA_FK100=CTX_AREA_FK100,
            CTX_AREA_NK100=CTX_AREA_NK100)
        try:
            res = requests.get(URL, params=params,
                headers=self.get_headers('TTTC8001R',
                'N' if CTX_AREA_FK100 else ''))
            if res.status_code != 200:
                print(res.json())
                err_msg = f'Request Error ({res.status_code}) : {res.text}'
                raise Exception(err_msg)
            data = res.json()
            ctx_area_fk100 = data.get('ctx_area_fk100')
            ctx_area_nk100 = data.get('ctx_area_nk100')
            df = pd.DataFrame(data.get('output1'),)
            df.columns = [
                '주문일자', '주문채번지점번호', '주문번호', '원주문번호', '주문구분명',
                '매도매수구분코드', '매도매수구분코드명', '상품번호', '상품명',
                '주문수량', '주문단가', '주문시각', '총체결수량', '평균가',
                '취소여부', '총체결금액', '대출일자', '주문담당자', '주문구분코드',
                '취소확인수량', '잔여수량', '거부수량', '체결조건명', '요청IP주소',
                'x1', 'x2', '통보시각', '연락전화번호', '상품유형코드', '거래소구분코드',
                'x3', 'x4', 'x5']
            df['유일주문코드'] = df.apply(
                lambda x: "-".join([x.주문일자, x.주문번호]), axis=1)
            df = df.astype({
                '총체결수량': int, '총체결금액': int, '주문수량': int, '주문단가': int,
                '취소확인수량': int, '잔여수량': int, '거부수량': int, '평균가': int})
            df['평균단가'] = df.총체결금액 / df.총체결수량
            df.set_index('유일주문코드', inplace=True)
            df.주문일자 = pd.to_datetime(df.주문일자, format='%Y%m%d')
            return df if not simple else\
                df.loc[:,
                 ['주문일자', '매도매수구분코드', '상품유형코드', '상품번호', '상품명',
                  '총체결수량', '총체결금액', '평균단가']
                ], res.headers.get('tr_cont'), ctx_area_fk100, ctx_area_nk100
        except Exception as ex:
            print(type(ex), ex)

class TradingHelper:
    periods = [2, 3, 5, 8, 13, 21]

    def __init__(self,
                 kis_client: KISTrading,
                 universe: pd.DataFrame=None,
                 n_clusters=10,
                 screen=4):
        
        self.kis_client : KISTrading = kis_client
        self.universe = universe
        print(f'UNIVERSE : {len(universe)}')
        self.current_stock : pd.DataFrame\
            = self.kis_client.get_stock_account()

        self.get_history()
        self.get_current_account_balance()
        self.get_current_budget()
        self.get_volitality()
        self.get_data_group(n_clusters)
        self.get_screen_table(screen)
    
    def get_current_account_balance(self):
        '''### 계좌 현황 조회'''
        account_balance = self.kis_client.get_account_balance()
        self.current_balance : pd.DataFrame\
            = account_balance.query('전체비중율 > 0')
    
    def get_current_budget(self):
        '''### 투자 예산 조회'''
        balance = self.current_balance
        self.current_budget : float = balance.loc[
            (balance.index != '채권') & (balance.index != '<합계>')]\
            ['평가금액'].sum()

    def get_price(self, symbol):
        return symbol, self.kis_client.get_daily_price(symbol)
    
    def get_history(self):
        '''### 멀티프로세싱으로 가격 데이터 조회'''
        start_time = time.time()
        with Pool(cpu_count() * 2) as p:
            prices = p.map(self.get_price, self.universe.index)
        p.join();
        print(f'Pool({cpu_count() * 2}) : {time.time() - start_time : .2f} seconds')
        self.history : pd.DataFrame = {k : v for k, v in prices}
    
    @classmethod
    def get_tr(cls, df: pd.DataFrame, close_col, high_col, low_col):
        '''### True Range 계산'''
        c = df[close_col].shift(1)
        h, l = df[high_col], df[low_col]
        concat = lambda x, y : pd.concat([x, y], axis=1)
        th = concat(c, h).max(axis=1)
        tl = concat(c, l).min(axis=1)
        return th - tl
    
    def get_volitality(self):
        '''### 변동성 계산'''
        tr_dict = {k: self.get_tr(p, '종가', '고가', '저가')
                    for k, p in self.history.items()}
        volitality = pd.concat(tr_dict.values(), axis=1)\
            .tail(max(self.periods))
        volitality.columns = tr_dict.keys()
        self.volitality : pd.DataFrame = volitality.copy()
        self.correlation : pd.DataFrame = volitality.corr()

    def draw_scatter(self,
                     text='종목명', color='카테고리',
                     size='시가총액', size_max=100):
        '''### 상관성 분석 시각화'''
        pca = PCA(2)
        components = pca.fit_transform(self.correlation)
        corr_pca = pd.DataFrame(components, index=self.correlation.index)
        corr_pca[text] = [
                self.universe.query(f"index == '{v}'").iloc[0][text]
                    for v in self.correlation.index]
        corr_pca[color] = [
                self.universe.query(f"index == '{v}'").iloc[0][color]
                    for v in self.correlation.index]
        corr_pca[size] = [
                self.universe.query(f"index == '{v}'").iloc[0][size]
                    for v in self.correlation.index]
        fig = px.scatter(corr_pca, x=0, y=1,
                        text=text, color=color, size=size,
                        size_max=size_max, height=525)
        return fig

    def get_data_group(self, n_clusters):
        '''
        ### 종목 그룹화
        '''
        cluster = AgglomerativeClustering(n_clusters=n_clusters)
        labels = cluster.fit_predict(self.correlation)
        self.data_group = [[(i, self.universe.loc[i].종목명)
                for i in self.correlation.index[labels == label]]
                for label in np.unique(labels)]

    @classmethod
    def get_score(cls, close_price):
        momentum = lambda x: x[-1] / x[0]
        scores = [close_price.rolling(p).apply(momentum).iloc[-1] for p in cls.periods]
        scores = [s for s in scores if pd.notnull(s)]
        if not scores: return 0
        return sum(scores) / len(scores)

    @classmethod
    def get_risk(cls, tr, close_price):
        c = close_price.iloc[-1]
        atr = tr.ewm(max(cls.periods)).mean().iloc[-1]
        return atr / c
    
    def get_screen_table(self, screen, limit=0.015):
        '''진입 테이블 작성'''
        scores = [[[v[0], v[1],
                    self.get_score(self.history[v[0]].종가),
                    self.get_risk(self.volitality[v[0]],
                                  self.history[v[0]].종가)]
                   for v in d] for d in self.data_group]
        df_scores = pd.DataFrame(
            [[i] + sorted(s, key=lambda x: x[2], reverse=True)[0] for i, s in enumerate(scores)])
        df_scores.columns = ['그룹', '종목코드', '종목명', '점수', '위험']
        df_scores.set_index('종목코드', inplace=True)
        df_scores.sort_values('점수', ascending=False, inplace=True)
        df_scores['버퍼'] = [(s > 1) and (s >= df_scores.점수.iloc[screen])
                            for s in df_scores.점수 ]
        df_scores['보유'] = [any([s in [d[0] for d in self.data_group[i]]
                            for s in self.current_stock.index]) for i in df_scores.그룹]
        df_scores['진입'] = df_scores.위험.apply(lambda x: min(limit / x, 1))\
            .apply(lambda x: x * self.current_budget / screen)\
            .apply(lambda x: int(x // 100000) * 100000)
        def enter(x):
            if x.버퍼 and x.보유:
                return x.진입
            if x.버퍼 and len(df_scores[df_scores.버퍼 & df_scores.보유]) != screen\
                and df_scores.iloc[screen - 1].점수 == x.점수:
                return x.진입
            return 0
        df_scores['진입'] = df_scores.apply(enter, axis=1)
        df_scores['보유'] = df_scores['보유'].apply(lambda x: '✅' if x else '🔘')
        df_scores.drop(columns=['그룹', '위험', '버퍼'], inplace=True)
        print(df_scores.진입.sum())
        self.screen_table = df_scores.copy()
