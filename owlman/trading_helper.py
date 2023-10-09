import time
from multiprocessing import Pool, cpu_count

import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.cluster import AgglomerativeClustering
import plotly.express as px

from owlman.kis_trading import KISTrading

class TradingHelper:
    periods = [2, 3, 5, 8, 13, 21]

    def __init__(self,
                 kis_client: KISTrading,
                 universe: pd.DataFrame=None,
                 n_clusters=10, screen=4, limit=0.015, buffer=1):
        
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
        self.get_screen_table(screen, limit, buffer)
    
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

    def draw_corr_scatter(self,
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
    
    def get_data_group_table(self):
        '''
        ### 종목 그룹 DF화
        '''
        group_df = [pd.DataFrame(group,
                                 columns=['종목코드', '종목명']).set_index('종목코드')
                    for group in self.data_group]
        for i, v in enumerate(group_df):
            v['그룹'] = i + 1
        return pd.concat(group_df).iloc[:, [1, 0]]

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
    
    def get_screen_table(self, screen, limit=0.015, buffer=1):
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
        df_scores['버퍼'] = [(s > 1) and (s >= df_scores.점수.iloc[screen - 1 + buffer])
                            for s in df_scores.점수 ]
        df_scores['보유'] = [any([s in [d[0] for d in self.data_group[i]]
                            for s in self.current_stock.index]) for i in df_scores.그룹]
        df_scores['진입'] = df_scores.위험.apply(lambda x: min(limit / x, 1))\
            .apply(lambda x: x * self.current_budget / screen)\
            .apply(lambda x: int(x // 100000) * 100000)
        new_candidate = screen - len(df_scores[df_scores.버퍼 & df_scores.보유])
        own = sum(df_scores['보유'])
        print(f'own: {own}, new_candidate : {new_candidate}')
        def enter(x):
            if x.버퍼 and x.보유:
                return x.진입
            if new_candidate and x.점수\
                > df_scores.iloc[own + new_candidate].점수:
                return x.진입
            return 0
        df_scores['진입'] = df_scores.apply(enter, axis=1)
        df_scores['보유'] = df_scores['보유'].apply(lambda x: '✅' if x else '🔘')
        df_scores['그룹'] += 1 # 0 시작 -> 1 시작
        df_scores['위험'] = df_scores['위험'].apply(lambda x: int(x * 10000) / 100)
        df_scores['점수'] = df_scores['점수'].apply(lambda x: int(x * 1000) / 1000)
        df_scores.drop(columns=['버퍼'], inplace=True)
        print(df_scores.진입.sum())
        self.screen_table = df_scores.copy()