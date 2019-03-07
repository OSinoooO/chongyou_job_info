# -*- coding:utf-8 -*-
import time
import csv
import re
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from queue import Queue
import requests
from retrying import retry
from threading import Thread
from threading import Lock


class JobInfoSpider(object):
    """重庆邮电大学就业信息中心爬虫"""
    def __init__(self, key_word):
        self.key_word = key_word
        self.driver = webdriver.PhantomJS()
        self.wait = WebDriverWait(self.driver, 20)
        self.url = 'http://job.cqupt.edu.cn/portal/home/special-recruitment-list.html?menuId=80'
        try:
            self.driver.get(self.url)
        except Exception as e:
            raise e
        self.url_queue = Queue()
        self.resp_queue = Queue()
        self.item_queue = Queue()
        self.lock = Lock()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
        }

    def parse_url(self):  # 发送请求，获取详情页url
        try:
            print('开始提取url...')
            while True:
                self.wait.until((EC.presence_of_all_elements_located((By.XPATH, '//*[@id="articleList-body"]/li/a'))))
                ret_list = self.driver.find_elements_by_xpath('//*[@id="articleList-body"]/li/a')
                url_list = [ret.get_attribute('href') for ret in ret_list]
                for url in url_list:
                    self.url_queue.put(url)
                self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="page-ul"]/li[last()-1]')))
                next_page = self.driver.find_element_by_xpath('//*[@id="page-ul"]/li[last()-1]')
                # 判断是否到了最后一页
                if next_page.get_attribute('class') != 'disabled':
                    next_page.find_element_by_tag_name('a').click()
                    time.sleep(0.5)
                else:
                    # 退出浏览器
                    print('url提取完毕...')
                    self.driver.quit()
                    break
        except:
            print('请求重试。。。')
            return self.parse_url()

    @retry(stop_max_attempt_number=3)
    def parse_info_url(self):  # 发送详情页请求，获取响应
        while True:
            url = self.url_queue.get()
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
            except Exception as e:
                raise e
            else:
                print(response.url, response.status_code)
                self.resp_queue.put(response)
                self.url_queue.task_done()

    def parse_item(self):  # 数据提取
        while True:
            item = {}
            response = self.resp_queue.get()
            url = response.url
            response = response.content.decode()
            self.lock.acquire()
            re_compile = re.findall(r'<div class="article-right floatR">(.*?)<div class="footer">', response, re.S)[0]
            if re.findall(r'(?i){}'.format(self.key_word), re_compile, re.S):
                item['name'] = re.findall(r'雇主名称：</label>(.*?)</td>', response, re.S)[0].strip()
                item['time'] = re.findall(r'举办时间：</label>(.*?)</td>', response, re.S)[0].strip()
                item['addr'] = re.findall(r'地点：</label>(.*?)</td>', response, re.S)[0].strip()
                item['url'] = url
                self.item_queue.put(item)
                print('匹配成功！')
            else:
                print('匹配失败！')
            self.resp_queue.task_done()
            self.lock.release()

    def save_item(self):
        f = open('{}.csv'.format(self.key_word), 'a', encoding='gbk', newline='')
        f_csv = csv.writer(f)
        f_csv.writerow(['雇主名称', '举办时间', '举办地点', '网址'])
        f.close()
        while True:
            with open('{}.csv'.format(self.key_word), 'a', encoding='gbk', newline='') as f:
                f = csv.writer(f)
                item = self.item_queue.get()
                print('记录保存')
                print(item)
                f.writerow([item['name'], item['time'], item['addr'], item['url']])
                self.item_queue.task_done()

    def run(self):
        thread_list = []
        # 发送请求，获取响应
        self.parse_url()
        for i in range(2):
            t_info_url = Thread(target=self.parse_info_url)
            thread_list.append(t_info_url)
        # 提取数据
        for i in range(4):
            t_item = Thread(target=self.parse_item)
            thread_list.append(t_item)
        # 保存数据
        t_save = Thread(target=self.save_item)
        thread_list.append(t_save)

        for t in thread_list:
            t.setDaemon(True)
            t.start()

        for q in [self.url_queue, self.resp_queue, self.item_queue]:
                q.join()

        print('程序执行完成！')


if __name__ == '__main__':
    key_word = input('请输入关键字：')
    job = JobInfoSpider(key_word)
    job.run()
