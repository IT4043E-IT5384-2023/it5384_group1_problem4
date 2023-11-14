exec("import os\nos.system('pip install pandas selenium webdriver-manager openxyl')")

import time
from threading import Thread

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from ExcelProcessing import SaveDataToExcel

def DriverSetup(profile = '',sleep = True): #profile ~ file_index
    chrome_options = Options()
    chrome_options.add_argument('--start-maximized')
    if sleep==True:
        chrome_options.add_experimental_option("detach", True)
    if profile:
        chrome_options.add_argument('--user-data-dir=C:/Users/ADMIN/AppData/Local/Google/Chrome/User Data')
        chrome_options.add_argument('--profile-directory=Profile '+profile)
    return webdriver.Chrome(options=chrome_options)

def DatetimeToEpoch(*args):
    import datetime
    import calendar
    s = list(map(int, args[0]))
    t = datetime.datetime(s[0], s[1], s[2], s[3], s[4], s[5])
    return (calendar.timegm(t.timetuple()))

def main():
    SaveDataToExcel(path='add5.xlsx',data=[],append=False)
    driver = DriverSetup()
    # gotit_button = '/html/body/div[4]/div/div/button'
    # WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, gotit_button))).click()
    page = 1
    gasprice = ''
    while True:
        driver.get('https://etherscan.io/txs?p='+str(page))
        page+=1
        i=1
        arr = []
        if not gasprice:
            gasprice = float(WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'/html/body/main/section[3]/div[1]/div[4]/a/div/div[2]/span[1]'))).get_attribute('data-bs-title').split(' ')[0])
        while i < 51:
            txnhash = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[2]/div/span/a'))).text
            type = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[3]/span'))).text
            blocknumber = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[4]/a'))).text
            timestamplast = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[6]/span'))).text.split(' ')[0]
            timestampnow = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[6]/span'))).get_attribute('data-bs-title').split(' ')
            timestamp = DatetimeToEpoch(timestampnow[0].split('-') + timestampnow[1].split(':'))+int(timestamplast)
            from_address = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[8]/div/a[1]'))).get_attribute('href')
            to_address = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[10]/div/a[1]'))).get_attribute('href')
            value = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[11]/span'))).text
            try:
                fee = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[12]'))).get_attribute('outerHTML')
                fee = fee.replace('<b>', '')
                fee = fee.replace('</b>', '')
                fee = fee[fee.find('<td style="" class="small text-muted showTxnFee ">') + len('<td style="" class="small text-muted showTxnFee ">'):fee.find('</td>')]
            except: fee = ''
            print([txnhash,type,blocknumber, timestamp,from_address,to_address,value,fee,float(fee)/gasprice,gasprice])
            arr.append([txnhash,type,blocknumber, timestamp,from_address,to_address,value,fee,float(fee)/gasprice,gasprice])
            i+=1
        SaveDataToExcel(path='add5.xlsx',data=arr,append=True)
if __name__ == '__main__':
    main()
