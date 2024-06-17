import time
import openpyxl
import pymssql
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from datetime import date



class TraxpayScraper:
    def __init__(self, username, password):
        self.driver = webdriver.Edge()
        self.username = username
        self.password = password

    def login(self):
        self.driver.get('your-dashboard')
        user = self.driver.find_element(By.ID, 'loginForm:inputUsername')
        password = self.driver.find_element(By.ID, 'loginForm:inputPassword')
        login_button = self.driver.find_element(By.ID, 'loginForm:loginButton')

        user.send_keys(self.username)
        password.send_keys(self.password)
        login_button.click()

    def go_to_table(self):
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.presence_of_element_located((By.ID, 'invoiceDtoLazyListModel:j_id_id')))
        wait.until(EC.element_to_be_clickable((By.ID, 'invoiceDtoLazyListModel:j_id_id')))
        order_by_element = self.driver.find_element(By.ID, 'invoiceDtoLazyListModel:j_id_id')
        self.driver.execute_script("arguments[0].click();", order_by_element)
        time.sleep(1)
        self.driver.execute_script("arguments[0].click();", order_by_element)
        time.sleep(1)
        dropdown_element = self.driver.find_element(By.ID, 'invoiceDtoLazyListModel:j_id__v_1')
        actions = ActionChains(self.driver)
        actions.move_to_element(dropdown_element).perform()
        select = Select(dropdown_element)
        select.select_by_visible_text("100")
        time.sleep(1)
        wait.until(EC.presence_of_element_located((By.ID, 'invoiceDtoLazyListModel_data')))

    def scrape_rglnrs(self):
        table = self.driver.find_element(By.ID, 'invoiceDtoLazyListModel_data')
        rows = table.find_elements(By.TAG_NAME, 'tr')
        invoice_data = []

        for row in rows:
            cells = row.find_elements(By.TAG_NAME, 'td')
            if len(cells) > 0 and len(cells) >= 5:
                rglnr_span_element = cells[5].find_element(By.TAG_NAME, 'span')
                rglnr_data = rglnr_span_element.text.strip() if rglnr_span_element else ''
                date_span_element = cells[7].find_element(By.TAG_NAME, 'span')
                date_data = date_span_element.text.strip() if date_span_element else ''
                invoice_data.append(tuple((rglnr_data, date_data)))

        return invoice_data

    def quit(self):
        self.driver.quit()


class MSQueryExecutor:
    def __init__(self, data):
        self.db = pymssql.connect(server='your-server', database='your-database')
        self.data = data
        self.rglnr_values = []
        self.grouped_list = []
        
    
    def execute_query(self):
        rglnr_values = [item[0] for item in self.data]
        cursor = self.db.cursor()
        placeholders = ', '.join(['%s'] * len(rglnr_values))
        query = f"SELECT AXRGNR, RGLNR, ABGDATUMZEIT FROM LOG_RGLNR WHERE RGLNR IN ({placeholders})"
        cursor.execute(query, rglnr_values)
        
        for row in cursor.fetchall():
           formatted_date = row[2].strftime('%d.%m.%Y')
           row2 = row[:2] + (formatted_date,) + row[3:]
           self.grouped_list.append(row2)
           
        return self.grouped_list
    
        cursor.close()
        self.db.close()


class ExcelExport:
        def __init__(self, data, excel_path, save_path):
            self.data = data
            self.excel_path = excel_path
            self.save_path = save_path
            
        def authenticate_sharepoint(self):
            sharepoint_site_url = 'your-sharepoint-url'
            username = 'your-sharepoint-user'
            password = 'your-sharepoint-password'
            
        def write_to_excel(self):
            filtered_data = [i for i in self.data if i[2] == date.today().strftime("%d.%m.%Y")]
            workbook = openpyxl.load_workbook(self.excel_path)
            sheet = workbook['your-sheet']
            
            last_row = sheet.max_row
            
            for i in filtered_data:
                last_row+=1
                for col, value in enumerate(i, start=1):
                    sheet.cell(row=last_row, column=col, value=value)
            workbook.save(self.save_path)
       

username = 'your-dashboard-user'
password = 'your-dashboard-password'

scraper = TraxpayScraper(username, password)
scraper.login()
scraper.go_to_table()

data = scraper.scrape_rglnrs()

scraper.quit()

executor = MSQueryExecutor(data)
grouped_data = executor.execute_query()

writer = ExcelExport(grouped_data, r"your-file-path", r"your-save-path")
        
writer.write_to_excel()