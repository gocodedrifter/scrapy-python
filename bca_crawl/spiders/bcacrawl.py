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

class BcaCrawl (Spider):
    name = ""
    __url = ""

    def start_requests(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.__driver = webdriver.Chrome(executable_path="/usr/local/bin/chromedriver", options=chrome_options)
        self.login()
        sleep(5)
        self.go_to_mutasipage()
        sleep(5)
        yield Request(url=self.__url, callback=self.parse)

    def go_to_mutasipage(self) :
        self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"menu\"]"))
        menu_mutasi = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.XPATH, "//a[@href=\"account_information_menu.htm\"]")))
        menu_mutasi.click()
        sleep(5)
        cek_mutasi = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.XPATH, "//a[@onclick=\"javascript:goToPage('accountstmt.do?value(actions)=acct_stmt');return false;\"]")))
        cek_mutasi.click()
        sleep(5)
        self.__driver.switch_to.default_content()
        self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"atm\"]"))
        mutasi_button = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.NAME, "value(submit1)")))
        mutasi_button.click()

        try :
            self.mutasi = self.__driver.find_element_by_xpath("//table[3]//tbody//tr[2]//td//table")
            self.__mutasiData = self.mutasi_parse(self.mutasi)
            self.show_mutasi()
        except:
            self.logger.info("Tidak ada mutasi hari ini")
        
        sleep(5)
        self.__driver.switch_to.default_content()

    def login(self):
        self.__driver.get(self.__url)
        self.username = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.ID, "user_id"))) 
        self.password = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.ID, "pswd")))  
        self.login_button = WebDriverWait(self.__driver, 3).until(EC.presence_of_element_located((By.XPATH, '//input[@value="LOGIN"]')))
        sleep(5)
        
        try:
            self.__driver.switch_to.frame(self.__driver.find_element_by_xpath("//frame[@name=\"header\"]"))
            self.__driver.switch_to.default_content()
        except Exception as err :
            alert = self.__driver.switch_to.alert
            alert.accept()
            self.logger.info(err)
            self.__driver.close()
            

    def logout(self):
        try :
            self.__driver.switch_to.frame((self.__driver.find_element_by_xpath("//frame[@name=\"header\"]")))
            logout = WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.XPATH, "//a[@onclick=\"javascript:goToPage('authentication.do?value(actions)=logout');return false;\"]")))
            logout.click()
        except TimeoutException as te:
            self.logger.info("Timeout due to" + te)
        except Exception as e:
            self.logger.info("Error due to " + e)

    def show_mutasi(self) :
        if len(self.__mutasiData) > 0 :
            print("Tgl \tMutasi \tDebit/Kredit \tKet")
            for transaction in self.__mutasiData:
                print("%s \t%s \t%s \t%s"%(transaction[0], transaction[3], transaction[4], transaction[1]))
        else:
            print("Tidak ada Mutasi Hari ini")

    def mutasi_parse(self,table):
        table_mutasi = BeautifulSoup(table.get_attribute('innerHTML'),"html.parser")
        data = []
        table_body = table_mutasi.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows :
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
        return data[1:]

    def close(self) :
        self.logout()
        self.__driver.quit()

    def parse(self, response):
        self.data = {"tanggal_mutasi":[], "keterangan":[], "amount":[], "dbkr":[], "saldo":[]}
        current_date = datetime.now().strftime('%d-%m-%Y')
        year = datetime.now().strftime('%Y')
        self.file = ""
        isexist = os.path.isfile(self.file)
        if len(self.__mutasiData) > 0 :
            year = datetime.now().strftime('%Y')
            self.logger.info("using new method")
            for transaction in self.__mutasiData:
                keterangan = str(transaction[1]).split()
                tanggal_mts = ''
                for ktrng in keterangan :
                    if '/' in ktrng :
                        ktrng_list = ktrng.split('/')
                        if len(ktrng_list[0]) == 2 and len(ktrng_list[1]) == 2:
                            tanggal = ktrng_list[0] + '/' + ktrng_list[1] + '/' + year
                            try :
                                if datetime.strptime(str(tanggal), "%m/%d/%Y") == datetime.strptime(datetime.now().strftime("%m/%d/%Y"), "%m/%d/%Y") :
                                    tanggal_mts = transaction[0]
                                    break
                            except:
                                self.logger.info("goes wrong unable to convert : " + tanggal)
                        if len(ktrng_list[0]) == 4:
                            tanggal = ktrng_list[0] + year
                            try :
                                if datetime.strptime(str(tanggal), "%d%m%Y") == datetime.strptime(datetime.now().strftime("%d%m%Y"), "%d%m%Y") :
                                    tanggal_mts = transaction[0]
                                    break
                            except:
                                self.logger.info("goes wrong unable to convert : " + tanggal)
                                
                if len(tanggal_mts) > 0 :
                    self.data["tanggal_mutasi"].append(transaction[0])
                    self.data["keterangan"].append(transaction[1])
                    self.data["amount"].append(transaction[3])
                    self.data["dbkr"].append(transaction[4])
                    self.data["saldo"].append(transaction[5])
                
        
        if isexist :
            try :
                self.df1 = pd.DataFrame(self.data)
                self.df2 = pd.read_csv(self.file)
                self.df = pd.merge(self.df1, self.df2, on=['keterangan', 'amount', 'dbkr', 'saldo'], how='left', indicator='Exist')
                self.df['Exist'] = np.where(self.df.Exist == 'both', True, False)
                self.indexNames = self.df[ self.df['Exist'] == True ].index
                self.df.drop(self.indexNames, inplace=True)
                self.df1.to_csv(self.file)
            except Exception as err:
                self.df = pd.DataFrame(self.data)
                self.df.to_csv(self.file)
        else:
            self.df = pd.DataFrame(self.data)
            self.df.to_csv(self.file)

        self.logger.info(self.df)

        if self.df.size > 0 :
            self.logger.info("size data frame : ", self.df.size)
            self.mydb = mysql.connector.connect(
                host="",
                user="",
                password="",
                database=""
            )

            self.cursor = self.mydb.cursor()

            for index, row in self.df.iterrows():
                query = "INSERT INTO bankmutasi (kodebank, tanggal, waktu, keterangan, nominal, dbkr, saldo, tglsys, waktusys, catatan1, catatan2, status) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                args = ("BCA", datetime.now().strftime("%Y-%m-%d"), "00:00:00", row["keterangan"], str(row["amount"]).replace(",", ""), row["dbkr"], str(row["saldo"]).replace(",", ""), datetime.now().strftime("%Y-%m-%d"), "00:00:00", "", "", "0")
                self.cursor.execute(query, args)

            self.mydb.commit()

            if self.mydb.is_connected() :
                self.mydb.close()
                self.cursor.close()


