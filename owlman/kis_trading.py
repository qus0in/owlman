import requests
import pandas as pd

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