# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

# 设置中文字体和负号正常显示
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False


class DealData:
    def __init__(self):
        date_parse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        self.df1 = pd.DataFrame()
        self.df = pd.read_csv('Demo_Data.csv', parse_dates=['timestamp'], date_parser=date_parse)

    def per_year_dollar(self):
        """
        统计每年dollar的和
        :return:
        """
        df1 = self.df.groupby(['year'])['dollar'].sum().reset_index()
        year = list(df1['year'])
        dollar = list(df1['dollar'])
        plt.figure(figsize=(9, 6))
        plt.xticks(range(len(year)), year)
        plt.bar(range(len(dollar)), dollar, color='green')
        plt.ylabel("dollar")
        plt.xlabel("年份")
        plt.title("每年dollar和")
        plt.savefig("./result/每年dollar和.png")
        plt.close()

    def every_year_data(self):
        """
        筛选某一年数据
        :return:
        """
        print("对某一年数据进行筛选。")
        year = int(input("请输入年份："))
        df1 = self.df[self.df.year == year]
        df1.to_csv("./result/%s年数据.csv" % year)
        print("%s 年数据已保存在 result 文件夹下。" % year)

    def every_name(self):
        """
        筛选某一个人的信息
        :return:
        """
        print("对某一 name 的信息进行筛选。")
        name = input("请输入名字：")
        df1 = self.df[self.df.name == name]
        df1.to_csv("./result/%s 信息数据.csv" % name)
        print("%s 信息数据已保存在 result 文件夹下。" % name)

    def year_and_close_price(self, date1, date2, num):
        """
        统计year和close_price在某个范围的数据
        :return:
        """
        df1 = self.df[('%s' % date1 < self.df.timestamp) & (self.df.timestamp < '%s' % date2) & (
                self.df['close-price'] > num)]
        df1.to_csv("./result/%s 到 %s , close-price 大于%s 的数据.csv" % (date1, date2, num))

    def deal_name(self, name, date_):
        """
        统计某一个人在某天之后的数据
        :param name: 人名
        :param date_: 日期
        :return:
        """
        df1 = self.df[(self.df.timestamp > '%s' % date_) & (self.df.name == '%s' % name)]
        df1.to_csv("./result/%s 在 %s 之后的数据.csv" % (name, date_))

    def much_col(self):
        """
        处理列不定
        :return:
        """
        col = input("请输入要处理的列：")
        col_list = col.split(",")
        # self.df['src'] = pd.to_datetime(self.df['src'], format='%Y/%m/%d %H:%M:%S')
        # self.df['src'] = pd.to_datetime(self.df['src'], unit='s')
        # self.df['dst'] = pd.to_datetime(self.df['dst'], unit='s')
        # print(self.df)
        for i in col_list:
            if i == 'name':
                if 'src' not in list(self.df1.columns):
                    self.df1 = self.df
                aa = input("请输入要筛选%s的数据：" % i)
                self.df1 = self.df1[self.df1['%s' % i] == aa]
            elif i in ['year', 'month', 'day']:
                if 'src' not in list(self.df1.columns):
                    self.df1 = self.df
                cc = eval(input("请输入要筛选%s的数据：" % i))
                self.df1 = self.df1[self.df1['%s' % i] == cc]
            elif i in ['btc', 'timestamp', 'close-price', 'dollar']:
                if 'src' not in list(self.df1.columns):
                    self.df1 = self.df
                bb = input("请输入%s的范围：" % i)
                if ',' in bb:
                    bb_list = bb.split(",")
                    self.df1 = self.df1[(self.df1['%s' % i] > '%s' % bb_list[0]) & (self.df1['%s' % i] < '%s' % bb_list[1])]
                elif '>' in bb:
                    print(bb.split('>'))
                    self.df1 = self.df1[self.df1['%s' % i] > '%s' % bb.split('>')[1]]
                elif '<' in bb:
                    self.df1 = self.df1[self.df1['%s' % i] > '%s' % bb.split('<')[1]]

        self.df1.to_csv("./result/%s列 筛选后的数据结果.csv" % col, index=None)
        print("%s列 的筛选数据结果已保存在result文件夹" % col )


if __name__ == '__main__':
    dd = DealData()
    #-- test opreation
    # dd.per_year_dollar()
    # dd.every_year_data()
    # dd.year_and_close_price('2011-1-1', '2011-6-1', 3)
    # dd.deal_name('GET HOLDINGS LIMITED', '2011-6-1')
    # dd.every_name()
    #---- test opreation
    
    dd.much_col()
