import requests
import pandas as pd

class KISTrading:
    domain = 'https://openapi.koreainvestment.com:9443'

    def __init__(self, appkey, appsecret,
                 CANO, ACNT_PRDT_CD):
        self.appkey = appkey
        self.appsecret = appsecret
        self.access_token = self.get_access_token()
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
    
    def get_headers(self, tr_id) -> dict:
        return {
            'content-type': 'application/json; charset=utf-8',
            'authorization': f'Bearer {self.access_token}',
            'appkey': self.appkey,
            'appsecret': self.appsecret,
            'tr_id': tr_id,
            'custtype': 'P',
        }
    
    @property
    def default_params(self): 
        return dict(CANO=self.CANO,
                    ACNT_PRDT_CD=self.ACNT_PRDT_CD)

    def get_account_balance(self, simple=True) -> pd.DataFrame:
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
            return df.loc[df.전체비중율 > 0]\
                .iloc[:, [0, 1, 2, 4, 5]] if simple else df
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
                err_msg = f'Request Error ({res.status_code}) : {res.text}'
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

if __name__ == '__main__':
    import os

    trading = KISTrading(
        os.getenv('appkey'),
        os.getenv('appsecret'),
        os.getenv('CANO'),
        os.getenv('ACNT_PRDT_CD'))
    
    account_balance = trading.get_account_balance()
    print(account_balance)
    stock_account = trading.get_stock_account()
    print(stock_account)