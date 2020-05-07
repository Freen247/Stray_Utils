#!/usr/bin/python
# -*- coding: utf-8 -*-
# __Author__ : Stray_Camel
# __Time__: 2020/04/23 11:01:41

import multiprocessing
import re
from concurrent.futures import (ALL_COMPLETED, ThreadPoolExecutor,
                                as_completed, wait)

import pandas as pd
import requests
from bs4 import BeautifulSoup as bf
from openpyxl import load_workbook

# 请求头
HEADS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36"
}
# 用来分隔汉字的所有标点符号
TAGS = "[＂＃＄％＆＇（）＊＋，－／：；: ＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､　、〃〈〉《》「」『』【】〔〕〖〗，。]"
# 文字中所需的关键词-解析门店数
REGREX = re.compile(r".*[{}].*".format('|'.join(['开设', '家', '门店',
                                                 '直营', '自营', '店铺', '专卖店', '专营店', '专柜', '终端', '达', '销售', '\d'])))


def _soup(res): return bf(res.text, "lxml")


def _save(data: "数据集，格式为字典",
          columns: "列名",
          sheet_name: "表单名",
          file: "文件名/路径" = "Data.xlsx",
          **others):
    # data = pd.DataFrame(data, columns=columns)
    # data.to_excel(file, sheet_name=sheet_name, index=None)
    try:
        data = pd.DataFrame(data, columns=columns)
        tmp = pd.ExcelWriter(file, engine='openpyxl')
        data.to_excel(tmp, index=None)
        tmp.book = load_workbook(tmp.path)
        data.to_excel(excel_writer=tmp, sheet_name=sheet_name, index=None)
        tmp.close()
    except BaseException as e:
        # print("保存失败", e)
        pass


def _res(url: "请求的链接",
         ) -> requests.models.Response:
    res = requests.get(url, headers=HEADS)
    res.encoding = res.encoding.lower().replace('gb2312', 'GBK')
    # print(url, res)
    return res


def _brand_store(bf_plist: "beautifulsoup找到的所有p标签的list",
                 ) -> list:
    p_res = []
    try:
        for p in bf_plist:
            # 按照中文的标点符号分隔每段p文字
            p_list = re.split(TAGS, p.get_text())
            # 遍历p文字中的所有小段，如果代又re_words中的任意个词，即返回
            def res_brand(x): return REGREX.findall(x)
            p_res += [res_brand(_) for _ in p_list if res_brand(_)]
    except AttributeError as e:
        """找不到p标签"""
        # print("通过文本查找门店信息失败")
    return p_res if p_res else ["找不到"]


class brand_ef360_com():
    def __init__(self):
        self.lable = "华衣网"
        self.origin_url = "http://brand.ef360.com/"
        # 固定字段和其他字段
        self.fields = []
        # 准备存入excel的数据
        self.data = []
        # 获取当前page的所有的子url链接
        self.sub_urls = lambda res: _soup(res).find_all(
            name='a', attrs={'class': 'logo', 'target': '_blank'})
        # 对所有子链接去重
        self.urls = []
        # 线程池
        self.all_task = []

    def get_data(self,
                 url: "访问品牌的界面",
                 ):
        """子程序，通过品牌的界面爬取需要的信息"""
        if url in self.urls:
            return
        else:
            self.urls.append(url)
        info = {}
        # 访问界面
        res = _res(url)
        # 此html部分可获得基本字段信息
        try:
            ori_infos = _soup(res).find(name='div', attrs={'class': 'd-detail-brandinfo'}).ul.find_all(name='li')
            # 解析基本字段信息
            title = ori_infos[0].get_text().split('：')
            info[title[0]] = title[1]
            if title[0] not in self.fields:
                self.fields.append(title[0])
            for _ in ori_infos[1:]:
                _info = _.get_text().split('：')
                _info[0] = _info[0].replace(title[1], '')
                info[_info[0]] = _info[1]
                if _info[0] not in self.fields:
                    self.fields.append(_info[0])
        except AttributeError as e:
            """如果品牌没有基本字段"""
            # print(url, "没有基本字段")
            pass

        if "不详" in info["门店总数"] and info["门店总数"]:
            info["门店总数"] = _brand_store(_soup(res).find_all(name='div', attrs={
                                        'class': 'd-detail-inner'})[1].find_all(name='p', text=REGREX))

        # "终端定位"等其他信息需从另一部分html界面调取
        try:
            other_infos = _soup(res).find(
                name='ul', attrs={'class': 'd-detail-infolist'}).find_all(name='li')
            for _ in other_infos:
                _info = _.get_text()[1:].split("：")
                info[_info[0]] = _info[1]
                if _info[0] not in self.fields:
                    self.fields.append(_info[0])
        except AttributeError as e:
            """有的品牌界面没有其他字段"""
            # print(url, "没有其他字段")
            pass
        # print(self.fields)
        self.data.append(info)

    def get_brands_url(self,
                       num: "list页面的page_num"
                       ) -> list:
        """根据访问每个list界面，获取所有的品牌的url"""
        res = _res(self.origin_url+"list/p-{}.html".format(num))
        with ThreadPoolExecutor(len(self.sub_urls(res))) as sub_executor:
            self.all_task += [sub_executor.submit(self.get_data, (_))
                              for _ in [*map(lambda x:x.get('href'), self.sub_urls(res))]]

    def crawler(self):
        res = _res(self.origin_url+'list')
        pattern = re.compile(r'(?<=list/p-)\d+')
        # 获取list中界面的总数，620
        page_num = int(pattern.findall(
            str(_soup(res).find_all(name='a', attrs={'class': 'last'})))[0])
        # 我们预定开设的最大线程为max_processes = multiprocessing.cpu_count()//(1-0.9))，因为每个list界面最多有10个子界面，并且都需要对其访问，所以我们需要开设max_processes/len(self.sub_lists)个线程
        with ThreadPoolExecutor(int((multiprocessing.cpu_count()//(1-0.9))//len(self.sub_urls(res)))) as executor:
            self.all_task += [executor.submit(self.get_brands_url, (_))
                              for _ in range(3)]
        wait(self.all_task, return_when=ALL_COMPLETED)
        _save(self.data, self.fields, self.lable)


class china_ef_com():
    def __init__(self):
        self.lable = "品牌服装网"
        self.origin_url = "http://www.china-ef.com/"
        # 固定字段
        self.fields = []
        # 准备存入excel的数据
        self.data = []
        # 获取当前page的所有的子url链接
        self.sub_urls = lambda res: _soup(res).find(
            name='ul', attrs={'class': 'brand-list'}).find_all(name='dl')
        # 对所有子链接去重
        self.urls = []
        # 线程池
        self.all_task = []

    def get_data(self,
                 url: "访问品牌的界面",
                 ):
        """子程序，通过品牌的界面爬取需要的信息"""
        if url in self.urls:
            return
        else:
            self.urls.append(url)
        info = {}
        # 访问界面
        res = _res(url)
        pattern = re.compile("[\n\r\[\]\u0020\u3000]")
        # 此html部分可获得基本字段信息
        try:
            ori_infos = _soup(res).find(name='div', attrs={
                'class': 'contact-left'}).table.tbody.find_all(name='tr')
            # 解析基本字段信息
            for _ in ori_infos:
                _info = pattern.sub('', _.get_text()).split('：')
                info[_info[0]] = _info[1]
                if _info[0] not in self.fields:
                    self.fields.append(_info[0])
        except AttributeError as e:
            """如果品牌没有基本字段"""
            # print(url, "没有基本字段")
            pass
        # 其他信息需从另一部分html界面调取
        try:
            other_infos = _soup(res).find(name='div', attrs={
                'class': 'company_main'}).table.find_all(name='td')
            for _ in other_infos:
                _info = pattern.sub('', _.get_text()).split('：')
                info[_info[0]] = _info[1]
                if _info[0] not in self.fields:
                    self.fields.append(_info[0])
        except AttributeError as e:
            """有的品牌界面没有其他字段"""
            # print(url, "没有其他字段")
            pass

        # 访问界面
        url_brand = url.replace('jiameng', 'brand')
        res_brand = _res(url_brand)
        # print(url_brand, res_brand)
        if "门店" not in self.fields:
            self.fields.append("门店")
        info["门店"] = _brand_store(_soup(res_brand).find(
            name='div', attrs={'class': 'company_main'}).find_all(name='p', text=REGREX))
        self.data.append(info)

    def get_brands_url(self,
                       num: "list页面的page_num"
                       ) -> list:
        """根据访问每个list界面，获取所有的品牌的url"""
        res = _res(self.origin_url+'brand/list-0-0-0-0-0-0-{}.html'.format(num))
        with ThreadPoolExecutor(len(self.sub_urls(res))) as sub_executor:
            self.all_task += [sub_executor.submit(self.get_data, (_)) for _ in [*map(lambda x:self.origin_url+x.find(name='div', attrs={
                'class': 'left-logo'}).find(name='div', attrs={'class': 'pic-logo'}).a.get('href').replace('show', 'jiameng'), self.sub_urls(res))]]

    def crawler(self):
        # 构造链接http://www.china-ef.com/brand/list-0-0-0-0-0-0-99999999.html 因为网站list没有最后一夜的按钮，构造链接访问第99999999个list界面，获取最大的page_num数
        res = _res(self.origin_url +
                   'brand/list-0-0-0-0-0-0-{}.html'.format(999999999))
        page_num = int(
            [*_soup(res).find(name='div', attrs={'class': 'page'}).children][-1].get_text())
        # 我们预定开设的最大线程为max_processes = multiprocessing.cpu_count()//(1-0.9))，因为每个list界面最多有url_num个子界面，并且都需要对其访问，所以我们需要开设max_processes/7个线程

        with ThreadPoolExecutor(int((multiprocessing.cpu_count()//(1-0.9))//len(self.sub_urls(res)))) as executor:
            self.all_task += [executor.submit(self.get_brands_url, (_))
                              for _ in range(page_num)]
        wait(self.all_task, return_when=ALL_COMPLETED)
        _save(self.data, self.fields, self.lable)


class chinasspp_com():
    def __init__(self):
        self.lable = "中国时尚品牌网"
        self.origin_url = "http://www.chinasspp.com/"
        # 固定字段
        self.fields = []
        # 准备存入excel的数据
        self.data = []
        # 获取当前page的所有的子url链接
        self.sub_urls = lambda res: _soup(res).find_all(
            name='a', attrs={'class': 'logo'})[1:-1]
        # 对所有子链接去重
        self.urls = []
        # 线程池
        self.all_task = []

    def get_data(self,
                 url: "访问品牌的界面",
                 ):
        """子程序，通过品牌的界面爬取需要的信息"""
        if url in self.urls:
            return
        else:
            self.urls.append(url)
        info = {}
        # 访问界面
        res = _res(url)
        pattern = re.compile("[\n\r\[\]\u0020\u3000\"]")
        # 此html部分可获得基本字段信息
        try:
            ori_infos = _soup(res).find(name='ul', attrs={
                'id': 'brand_info_ctl00_blink'}).find_all(name='li')
            # 解析基本字段信息，[:-1]去除联系客服的链接
            for _ in ori_infos:
                _info = pattern.sub('', _.get_text()).split('：')
                try:
                    # 公司信息的联系电话和传真时图片形式
                    tel = self.origin_url+_.img.get('src')
                    info[_info[0]] = tel
                except:
                    info[_info[0]] = _info[1]
                if _info[0] not in self.fields:
                    self.fields.append(_info[0])
        except AttributeError as e:
            """如果品牌没有基本字段"""
            # print(url, "没有基本字段")
            pass

        if "门店" not in self.fields:
            self.fields.append("门店")
        info["门店"] = _brand_store(_soup(res).find(
            name='div', attrs={'class': 'about'}).find_all(name='p', text=REGREX))
        self.data.append(info)

    def get_brands_url(self,
                       num: "list页面的page_num"
                       ) -> list:
        """根据访问每个list界面，获取所有的品牌的url"""
        res = _res(self.origin_url+'brand/brands-{}.html'.format(num))
        with ThreadPoolExecutor(len(self.sub_urls(res))) as sub_executor:
            self.all_task += [sub_executor.submit(self.get_data, (_))
                              for _ in [*map(lambda x:x.get('href'), self.sub_urls(res))]]

    def crawler(self):
        # 直接访问网站的list链接
        res = _res(self.origin_url+'brand/brands.html')
        page_num = int(
            [*_soup(res).find(name='p', attrs={'class': 'pagination'}).children][-2].get_text())
        # 我们预定开设的最大线程为max_processes = multiprocessing.cpu_count()//(1-0.9))，因为每个list界面最多有7个子界面，并且都需要对其访问，所以我们需要开设max_processes/url_num个线程

        with ThreadPoolExecutor(int((multiprocessing.cpu_count()//(1-0.9))//len(self.sub_urls(res)))) as executor:
            self.all_task += [executor.submit(self.get_brands_url, (_))
                              for _ in range(page_num)]
        wait(self.all_task, return_when=ALL_COMPLETED)
        _save(self.data, self.fields, self.lable)


class fuzhuang_1637_com():
    def __init__(self):
        self.lable = "一路商机"
        self.origin_url = "http://xiangmu.1637.com/"
        # 固定字段
        self.fields = []
        # 准备存入excel的数据
        self.data = []
        # 获取当前page的所有的子url链接
        self.sub_urls = lambda res: _soup(res).find_all(
            name='div', attrs={'class': 'xmlogo'})
        # 对所有子链接去重
        self.urls = []
        # 线程池
        self.all_task = []

    def get_data(self,
                 url: "访问品牌的界面",
                 ):
        """子程序，通过品牌的界面爬取需要的信息"""
        if url in self.urls:
            return
        else:
            self.urls.append(url)
        info = {}
        # 访问界面
        res = _res(url)
        # 此html部分可获得基本字段信息
        try:
            ori_infos = _soup(res).find(name='ul', attrs={
                'class': 'cf'}).find_all(name='li')
            # 解析基本字段信息
            for _ in ori_infos:
                info[[*_.contents][0].get_text()] = [*_.contents][-1].get_text()
                if [*_.contents][0].get_text() not in self.fields:
                    self.fields.append([*_.contents][0].get_text())
        except AttributeError as e:
            """如果品牌没有基本字段"""
            # print(url, "没有基本字段")
            pass
        self.data.append(info)

    def get_brands_url(self,
                       num: "list页面的page_num"
                       ) -> list:
        """根据访问每个list界面，获取所有的品牌的url"""
        res = _res(self.origin_url+'/fuzhuang/p{}.html'.format(num))
        with ThreadPoolExecutor(len(self.sub_urls(res))) as sub_executor:
            self.all_task += [sub_executor.submit(self.get_data, (_)) for _ in [
                *map(lambda x:x.a.get('href'), self.sub_urls(res))]]

    def crawler(self):
        # 获取http://xiangmu.1637.com/fuzhuang/，中显示的总共最大的界面
        res = _res(self.origin_url+'/fuzhuang/')
        page_num = int(
            [*_soup(res).find(name='div', attrs={'class': 'pager fr'}).span.children][-1][1:])
        # 我们预定开设的最大线程为max_processes = multiprocessing.cpu_count()//(1-0.9))，因为每个list界面最多有url_num个子界面，并且都需要对其访问，所以我们需要开设max_processes/url_num个线程

        with ThreadPoolExecutor(int((multiprocessing.cpu_count()//(1-0.9))//len(self.sub_urls(res)))) as executor:
            self.all_task += [executor.submit(self.get_brands_url, (_))
                              for _ in range(3)]
        wait(self.all_task, return_when=ALL_COMPLETED)
        _save(self.data, self.fields, self.lable)


class anxjm_com_fz_():
    def __init__(self):
        self.lable = "安心加盟网"
        self.origin_url = "https://www.anxjm.com/"
        # 字段
        self.fields = []
        # 准备存入excel的数据
        self.data = []
        # 获取当前page的所有的子url链接
        self.sub_urls = lambda res: _soup(res).find_all(
            name='div', attrs={'class': 't_Logo'})
        # 对所有子链接去重
        self.urls = []
        # 线程池
        self.all_task = []

    def get_data(self,
                 url: "访问品牌的界面",
                 ):
        """子程序，通过品牌的界面爬取需要的信息"""
        if url in self.urls:
            return
        else:
            self.urls.append(url)
        info = {}
        # 访问界面
        res = _res(url)
        pattern = re.compile("[\n]")
        try:
            base_info = _soup(res).find(name='div', attrs={
                'class': 'context_title'}).get_text()
            info["品牌名称"] = pattern.sub('', base_info)
            if "品牌名称" not in self.fields:
                self.fields.append("品牌名称")
        except AttributeError as e:
            """没有基本字段"""
            # print(url, "没有基本字段", e)

        try:
            other_infos = _soup(res).find(name='div', attrs={
                'class': 'c_info'}).ul.find_all(name='li')[:-1]
            for _ in other_infos:
                _info = pattern.sub('', _.get_text()).split('：')
                info[_info[0]] = _info[1]
                if _info[0] not in self.fields:
                    self.fields.append(_info[0])
        except AttributeError as e:
            """没有其他字段"""
            # print(url, "没有其他字段", e)

        self.data.append(info)

    def get_brands_url(self,
                       num: "list页面的page_num"
                       ) -> list:
        """根据访问每个list界面，获取所有的品牌的url"""
        res = _res(self.origin_url+"fz/{}".format(num))
        with ThreadPoolExecutor(len(self.sub_urls(res))) as sub_executor:
            self.all_task += [sub_executor.submit(self.get_data, (_)) for _ in [*map(
                lambda x:self.origin_url+x.a.get('href'), self.sub_urls(res))]]

    def crawler(self):
        res = _res(self.origin_url+'fz/')
        # 获取list中界面的总数
        page_num = int(_soup(res).find(name='ul', attrs={
                       'class': 'pagination'}).find_all(name='li')[-2].get_text())
        # 我们预定开设的最大线程为max_processes = multiprocessing.cpu_count()//(1-0.9))，因为每个list界面最多有10个子界面，并且都需要对其访问，所以我们需要开设max_processes/len(self.sub_lists)个线程
        with ThreadPoolExecutor(int((multiprocessing.cpu_count()//(1-0.9))//len(self.sub_urls(res)))) as executor:
            self.all_task += [executor.submit(self.get_brands_url, (_))
                              for _ in range(page_num)]
        wait(self.all_task, return_when=ALL_COMPLETED)
        _save(self.data, self.fields, self.lable)


class cy580_com_fuzhuang():
    def __init__(self):
        self.lable = "创业网"
        self.origin_url = "http://www.cy580.com/"
        # 字段
        self.fields = []
        # 准备存入excel的数据
        self.data = []
        # 获取当前page的所有的子url链接
        self.sub_urls = lambda res: _soup(res).find_all(
            name='div', attrs={'class': 'item-img fl'})
        # 对所有子链接去重
        self.urls = []
        # 线程池
        self.all_task = []

    def get_data(self,
                 url: "访问品牌的界面",
                 ):
        """子程序，通过品牌的界面爬取需要的信息"""
        if url in self.urls:
            return
        else:
            self.urls.append(url)
        info = {}
        # 访问界面
        res = _res(url)
        try:
            base_info = _soup(res).find(name='div', attrs={
                'class': 'title'}).get_text()
            # # print(base_info)
            info["品牌名称"] = base_info
            if "品牌名称" not in self.fields:
                self.fields.append("品牌名称")
        except AttributeError as e:
            """没有基本字段"""
            # print(url, "没有基本字段", e)

        try:
            other_infos = _soup(res).find(name='div', attrs={
                'class': 'detail-con-info'}).ul.find_all(name='li')
            for _ in other_infos:
                info[_.span.get_text()] = _.a.get_text() if _.a else [
                    *_.children][1]
                if _.span.get_text() not in self.fields:
                    self.fields.append(_.span.get_text())
        except AttributeError as e:
            """没有其他字段"""
            # print(url, "没有其他字段", e)
        self.data.append(info)

    def get_brands_url(self,
                       num: "list页面的page_num"
                       ) -> list:
        """根据访问每个list界面，获取所有的品牌的url"""
        res = _res(self.origin_url+"fuzhuang/page/{}".format(num))
        with ThreadPoolExecutor(len(self.sub_urls(res))) as sub_executor:
            self.all_task += [sub_executor.submit(self.get_data, (_)) for _ in [*map(
                lambda x:self.origin_url+x.a.get('href'), self.sub_urls(res))]]

    def crawler(self):
        res = _res(self.origin_url+'fuzhuang')
        # 获取list中界面的总数
        page_num = int(_soup(res).find(name='ul', attrs={
                       'class': 'pagination'}).find_all(name='li')[-2].get_text())
        # 我们预定开设的最大线程为max_processes = multiprocessing.cpu_count()//(1-0.9))，因为每个list界面最多有10个子界面，并且都需要对其访问，所以我们需要开设max_processes/len(self.sub_lists)个线程
        with ThreadPoolExecutor(int((multiprocessing.cpu_count()//(1-0.9))//len(self.sub_urls(res)))) as executor:
            self.all_task += [executor.submit(self.get_brands_url, (_))
                              for _ in range(page_num)]
        wait(self.all_task, return_when=ALL_COMPLETED)
        _save(self.data, self.fields, self.lable)


if __name__ == "__main__":
    test_brand_ef360_com = brand_ef360_com()
    test_china_ef_com = china_ef_com()
    test_chinasspp_com = chinasspp_com()
    test_fuzhuang_1637_com = fuzhuang_1637_com()
    test_anxjm_com_fz_ = anxjm_com_fz_()
    test_cy580_com_fuzhuang = cy580_com_fuzhuang()

    test_brand_ef360_com.crawler()
    # test_china_ef_com.crawler()
    # test_chinasspp_com.crawler()
    test_fuzhuang_1637_com.crawler()
    # test_anxjm_com_fz_.crawler()
    # test_cy580_com_fuzhuang.crawler()