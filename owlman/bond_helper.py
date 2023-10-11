from datetime import datetime

import requests
import pandas as pd

from owlman.kis_trading import KISTrading

class BondHelper:
    PATH='https://www.shinhansec.com/siw/wealth-management/bond-rp'
    발행정보='590401P02'
    상세시세='590401P03V02'
    cache = {}

    def __init__(self, pd_no):
        if pd_no not in self.cache:
            # print(f'NEW : {pd_no}')
            self.pd_no = pd_no
            self.pd_no_json = dict(
                bondCode=self.pd_no, cls=self.pd_no)
            self.get_issue_info() # 발행정보
            self.get_price_info() # 시세정보
            self.cache[pd_no]\
                = (self.profit, self.expire_date, self.current_price)
        else:
            profit, expire_date, current_price = self.cache.get(pd_no)
            self.profit = profit
            self.expire_date = expire_date
            self.current_price = current_price

    def get_issue_info(self):
        '''### 발행정보'''
        URL = f'{self.PATH}/{self.발행정보}/data.do'
        response = requests.post(URL, json=self.pd_no_json)
        data = response.json()
        body = data.get('body')
        # print(body)
        # 이자율
        bondProfitInfo = body.get('bondProfitInfo')
        profit = pd.DataFrame(bondProfitInfo.get('반복데이타0'))
        profit.columns = ['지급일자', '지급이율', '세전지급금액']
        profit.지급일자 = pd.to_datetime(profit.지급일자)
        self.profit = profit.set_index('지급일자').iloc[:, [1]].astype(float)
        # 만기일자
        bondMaster = body.get('bondMaster')
        self.expire_date = datetime.strptime(
            bondMaster.get('만기일자'), '%Y%m%d').date()
    
    def get_price_info(self):
        '''### 시세정보'''
        URL = f'{self.PATH}/{self.상세시세}/data.do'
        response = requests.post(URL, json=self.pd_no_json)
        data = response.json()
        body = data.get('body')
        # 만기 시 시세정보 대응
        self.current_price = float(
            body.get('tr1').get('현재가')) if body else 1000

    @classmethod
    def cal_fee(cls, timedelta):
        '''### 채권수수료'''
        left_days = timedelta.days
        if left_days < 90: return 0.000052  # 90일 미만 : 0.0052%
        if left_days < 365: return 0.000152 # 90일 이상 ~ 365일 미만 : 0.0152%
        if left_days < 730: return 0.000252 # 365일 이상 ~ 730일 미만 : 0.0252%
        return 0.000352                     # 730일 이상 : 0.0352%

    def get_interest_between(self, start_dt, end_dt, tax=False):
        '''### 기간 이자 계산'''
        return self.profit.query(
            f'"{start_dt}" < index <= "{end_dt}"')\
            .세전지급금액.sum() * ((1 - 0.154) if tax else 1)

    @classmethod
    def get_trade_record(cls, buy, sell, tax=False):
        '''매매 기록 조회'''
        detail = cls(buy.iloc[0].상품번호)
        today = datetime.now().date()
        sell.reset_index(inplace=True)
        m = pd.merge(buy, sell, on='상품번호')\
            if len(sell) else pd.merge(buy, sell, on='상품번호', how='outer')
        m['매수수수료'] = m.평균단가_x * m.주문일자_x.apply(
            lambda x: cls.cal_fee((detail.expire_date - x.date())))
        if len(sell):
            m['매도수수료'] = m.평균단가_y * m.주문일자_y.apply(
                lambda x: cls.cal_fee((detail.expire_date - x.date())))
            m['예상이표수익'] = m.apply(
              lambda x: detail.get_interest_between(
                x.주문일자_x, x.주문일자_y, tax) / 10, axis=1)
            m['예상매매수익'] = m.평균단가_y + m.예상이표수익 - m.평균단가_x - m.매수수수료 - m.매도수수료
            m['예상매매수익률'] = m.예상매매수익 / m.평균단가_x * 365\
                / (m.주문일자_y - m.주문일자_x).apply(lambda x: x.days) * 100
        else:
            m['매도수수료'] = 0
            m['예상이표수익'] = m.apply(
              lambda x: detail.get_interest_between(
                x.주문일자_x, today, tax) / 10, axis=1)
            if detail.expire_date < today: # 만기상환 시
                m.평균단가_y = 1000
                m.주문일자_y = detail.expire_date
                m['예상매매수익'] = m.평균단가_y + m.예상이표수익 - m.평균단가_x - m.매수수수료
                m['예상매매수익률'] = m.apply(
                    lambda x: x.예상매매수익 / x.평균단가_x *365\
                    / (x.주문일자_y - x.주문일자_x.date()).days * 100, axis=1)
            else:
                m['예상매매수익'] = 0
                m['예상매매수익률'] = 0
        m['만기일'] = detail.expire_date

        # print(m.keys())
        result = m.sort_values('예상매매수익률', ascending=False).copy()\
                .iloc[[0]]\
                .loc[:, ['상품번호', '상품명_x', '만기일', '총체결수량_x',
                            '주문일자_x', '평균단가_x',
                            '유일주문코드', '주문일자_y', '평균단가_y',
                            '예상이표수익', '예상매매수익', '예상매매수익률',]]
        result.columns = ['상품번호', '상품명', '만기일', '보유수량',
                            '매수일자', '매수단가',
                            '매도주문코드', '매도일자', '매도단가',
                            '이표수익', '매매수익', '매매수익률',]
        try:
            result['매도일자'] = result['매도일자'].apply(lambda x: x.date())
        except:
            pass
        return result
    
    @classmethod
    def get_bond_trading_result(cls,
                                kis_client: KISTrading,
                                start_dt, end_dt):
        '''특정 기간 매수 및 매도 기록 받아오기'''
        orders = kis_client.get_daily_all_orders(start_dt, end_dt)
        query = lambda code : f'매도매수구분코드 == "{code}" & 상품유형코드 == "302"'
        buy_bond = orders.query(query('02')).copy()
        sell_bond = orders.query(query('01')).copy()
        return buy_bond, sell_bond
    
    @classmethod
    def get_merged_result(cls, buy_bond, sell_bond, tax=False):
        '''매수와 매도 기록 짝짓기'''
        records = []
        for i in range(len(buy_bond)):
            b = buy_bond.iloc[[i]]
            s = sell_bond.query(
                f'상품번호 == "{b.iloc[0].상품번호}" &'\
                f'주문일자 > "{b.iloc[0].주문일자.date()}" &'\
                f'총체결수량 > 0')
            r = cls.get_trade_record(b, s, tax)
            if len(s) > 0:
                sell_bond.loc[r.iloc[0].매도주문코드, '총체결수량'] -= r.iloc[0].보유수량
            records.append(r)
        return pd.concat(records)\
                .reset_index(drop=True)\
                .drop(columns=['매도주문코드'])

    @classmethod
    def cal_earn_predict(cls,
                         data : pd.DataFrame,
                         name: str = '',
                         price: float = None,
                         screen: float = 0,
                         sell_date: str = None,
                         hold = False,
                         tax = False):
        '''
        ### 매도 시 예상 수익률 계산
        `own = result.loc[result.매도일자.isnull()].copy()`
        '''
        df = data.copy()
        if name:
            df = df.loc[df.상품명 == name].copy()
        if hold: # 만기 보유 시
            df['매도일자'] = df.상품번호.apply(
                lambda x: datetime.fromordinal(
                    cls(x).expire_date.toordinal()))
            df['매도단가'] = 1000
        else:
            if sell_date: # 만기 보유 테스트를 위한 일자 지정
                df['매도일자'] = datetime.strptime(sell_date, '%Y%m%d')     
            else:
                df['매도일자'] = datetime.now()
            df['매도단가'] = df.상품번호.apply(
                lambda x: price or cls(x).current_price).div(10)
        df['매수수수료'] = df.apply(
            lambda x: x.매수단가 * cls.cal_fee(
                cls(x.상품번호).expire_date - x.매수일자.date()),
            axis=1)
        df['매도수수료'] = df.apply(
            lambda x: 0 if pd.Timestamp(x.만기일) <= pd.Timestamp(x.매도일자)\
                else x.매도단가 * cls.cal_fee(
                cls(x.상품번호).expire_date - x.매도일자.date()),
            axis=1)
        df['이표수익'] = df.apply(
              lambda x: cls(x.상품번호).get_interest_between(
                x.매수일자, x.매도일자, tax) / 10, axis=1)
        df['매매수익'] = df.매도단가 + df.이표수익 - df.매수단가 - df.매수수수료 - df.매도수수료
        df['매매수익률'] = df.매매수익 / df.매수단가 * 365\
            / (df.매도일자 - df.매수일자).apply(lambda x: x.days) * 100
        df['매도일자'] = df['매도일자'].apply(lambda x: x.date()) 
        df.drop(columns=['매수수수료', '매도수수료'], inplace=True)
        result = df.sort_values('매매수익률', ascending=False).set_index('상품번호')
        return result.query(f'매매수익률 > {screen}')