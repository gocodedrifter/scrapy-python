from scrapy import Spider
from scrapy import Request
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from time import sleep
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os.path
import numpy as np
import re
import mysql.connector

class Bnicrawl (Spider):
    name = "bni-crawl"
    __url = "https://bnidirect.bni.co.id/"

    def start_requests(self):
        # chrome_options = Options()
        # chrome_options.add_argument("--headless")
        # self.__driver = webdriver.Chrome(executable_path="/usr/local/bin/chromedriver", options=chrome_options)
        self.__driver = webdriver.Chrome()
        self.login()
        self.go_to_mutasipage()
        self.logout()
        yield Request(url=self.__url, callback=self.parse)

    def go_to_mutasipage(self):
        accInformation = WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.XPATH, "/html/body/table/tbody/tr/td/div[1]/div[1]")))
        accInformation.click()
        mutasi = WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.XPATH, "//a[@href=\"/corp/front/transactioninquiry.do?action=transactionByDateRequest&menuCode=MNU_GCME_040200\"]")))
        mutasi.click()
        self.__driver.switch_to.window(self.__window_after)
        self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"mainFrame\"]"))
        img = WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.XPATH, "/html/body/form/table[3]/tbody/tr[5]/td[2]/a/img")))
        img.click()
        window_acc = self.__driver.window_handles[len(self.__driver.window_handles)-1]
        self.__driver.switch_to.window(window_acc)
        link_text = WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "8877889901")))
        link_text.click()
        self.__driver.switch_to.window(self.__window_after)
        self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"mainFrame\"]"))
        btn_show = WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.NAME, "show1")))
        btn_show.click()
        htmlview = self.__driver.window_handles[len(self.__driver.window_handles)-1]
        self.__driver.switch_to.window(htmlview)
        self.__driver.switch_to.frame(self.__driver.find_element_by_id("viewHtmlFrame"))
        self.data_mutasi= WebDriverWait(self.__driver, 35).until(EC.presence_of_element_located((By.XPATH, "//html//body//table//tbody//tr//td[2]//a//table")))
        self.__data = self.mutasi_parse(self.data_mutasi)

        self.__driver.switch_to.window(self.__window_after)
        self.__driver.switch_to.default_content()
        WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.ID, "frset")))
        self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"menuFrame\"]"))
        
    def login(self):
        self.__driver.get(self.__url)
        sleep(10)
        self.company_id = WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.NAME, "corpId")))
        self.company_id.send_keys("indond")
        sleep(5)
        self.username = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.NAME, "userName")))
        self.username.send_keys("yola0521")
        sleep(5)
        self.password = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.NAME, "passwordEncryption")))
        self.password.send_keys("Soma0594")
        WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.NAME, "language")))
        select = Select(self.__driver.find_element_by_name('language'))
        select.select_by_visible_text('Indonesia')
        self.btn_submit = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.NAME, "submit1")))
        self.btn_submit.click()
        sleep(15)

        try:
            self.__window_before = self.__driver.window_handles[0]
            self.__window_after = self.__driver.window_handles[1]
            self.__driver.switch_to.window(self.__window_after)
            self.__driver.switch_to.default_content()
            WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.ID, "frset")))
            self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"menuFrame\"]"))
        except:
            self.__driver.close()

    def mutasi_parse(self,table):
        table_mutasi = BeautifulSoup(table.get_attribute('innerHTML'),"html.parser")
		# print(table_mutasi.prettify())
        data = []
        table_body = table_mutasi.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows[9:] :
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
        return data
    
    def parse(self, response):
        self.data = {"tanggal_mutasi":[], "waktu_mutasi":[], "keterangan":[], "amount":[], "dbkr":[], "saldo":[]}
        current_date = datetime.now().strftime('%d-%m-%Y')
        self.file = "/tmp/data-%s.csv" % current_date
        isexist = os.path.isfile(self.file)

        if len(self.__data) > 0:
            for transaction in self.__data:
                if len(transaction) > 1 :
                    tanggal = re.search("(\d{1,4}([.\-/])\d{1,2}([.\-/])\d{1,4})", str(transaction[1]))
                    if len(transaction) > 5 and bool(tanggal):
                        date_trans = datetime.strptime(transaction[1], '%d/%m/%Y %H.%M.%S')
                        self.data["tanggal_mutasi"].append(date_trans.strftime('%Y-%m-%d'))
                        self.data["waktu_mutasi"].append(date_trans.strftime('%H:%M:%S'))
                        self.data["keterangan"].append(transaction[4])
                        self.data["amount"].append(transaction[5])
                        self.data["dbkr"].append('DB' if str(transaction[6]) == 'D' else 'CR')
                        self.data["saldo"].append(transaction[7])
        
        if isexist:
            try:
                self.df1 = pd.DataFrame(self.data)
                self.df2 = pd.read_csv(self.file)
                self.df = pd.merge(self.df1, self.df2, on=['keterangan', 'amount', 'dbkr', 'saldo'], how='left', indicator='Exist')
                self.df['Exist'] = np.where(self.df.Exist == 'both', True, False)
                self.indexNames = self.df[ self.df['Exist'] == True ].index
                self.df.drop(self.indexNames, inplace=True)
                self.df1.to_csv(self.file)
            except Exception as err:
                self.logger.info("unable to read %d" % err)
                self.df = pd.DataFrame(self.data)
                self.df.to_csv(self.file)
        else:
            self.df = pd.DataFrame(self.data)
            self.df.to_csv(self.file)
        
        if self.df.size > 0 :
            self.logger.info("size data frame : ", self.df.size)
            self.mydb = mysql.connector.connect(
                host="103.101.226.244",
                user="root",
                password="Somatech@M0nit-k4skU#12930!!!Pr0D",
                database="ibank"
            )

            self.cursor = self.mydb.cursor()

            for index, row in self.df.iterrows():
                query = "INSERT INTO bankmutasi (kodebank, tanggal, waktu, keterangan, nominal, dbkr, saldo, tglsys, waktusys, catatan1, catatan2, status) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                args = ("BNI", datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime('%H:%M:%S'), row["keterangan"], str(row["amount"]).replace(",", ""), row["dbkr"], str(row["saldo"]).replace(",", ""), datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime('%H:%M:%S'), "", "", "0")
                self.cursor.execute(query, args)

            self.mydb.commit()

            if self.mydb.is_connected() :
                self.mydb.close()
                self.cursor.close()
    
    def close(self):
        # for driver in self.__driver.window_handles:
        #     driver.close()
        pass

    def logout(self):
        self.__driver.switch_to.window(self.__window_after)
        self.__driver.switch_to.default_content()
        WebDriverWait(self.__driver, 15).until(EC.presence_of_element_located((By.ID, "frset")))
        self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"menuFrame\"]"))
        self.logout = WebDriverWait(self.__driver, 5).until(EC.presence_of_element_located((By.XPATH, "//a[@href=\"/corp/common/login.do?action=logout\"]")))
        self.logout.click()
        self.__driver.close()
        self.__driver.switch_to.window(self.__window_before)
        self.__driver.switch_to.default_content()
        self.__driver.close()
