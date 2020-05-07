#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ : stray_camel
# __date__: 2020/05/07 19:00:38
import zipfile, urllib.request
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Public import Config

def downloadZip(urllist: "下载zip包的urls") -> None:
    """默认将数据集解压到当前文件夹的上级目录"""
    namelist = [_.split('/')[-1] for _ in urllist]
    for index, item in enumerate(urllist):
        print('正在下载，请耐心等待，' + namelist[index])
        work_path = os.path.join(Config.DIR, namelist[index])
        urllib.request.urlretrieve(item, work_path)
        # 下载完了解压
        files = zipfile.ZipFile(work_path, 'r')
        for f in files.namelist():
            files.extract(f, Config.DIR)

if __name__ == "__main__":
    print(Config.DIR)