try:
  from app import app
  from selenium import webdriver
  from selenium.webdriver.chrome.options import Options
  from selenium.webdriver.common.action_chains import ActionChains
  from selenium.webdriver.support.ui import WebDriverWait
  from selenium.webdriver.support import expected_conditions as EC
  from selenium.common.exceptions import *
  from selenium.webdriver.common.by import By
  from selenium.webdriver.common.keys import Keys
  import time
  from decimal import *
  import logging
  import json
  import datetime
  import sys
  import zipfile

  print("All Modules are ok .......")

except Exception as e:
  print("Error in Imports ")
  print(str(e))

# local imports
import config
from utilities.helpers import publish_sns_message, get_random_user_agent
from models.Items import Items
from models.Merchants import Merchants
from models.PlatformCredentials import PlatformCredentials
from models.VirtualMerchants import VirtualMerchants


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


### webdriver class
class WebDriver(object):

  def __init__(self, userAgent=False, useProxy=False):
    self.options = Options()

    self.options.binary_location = '/opt/headless-chromium'
    self.options.add_argument('--headless')
    self.options.add_argument('--no-sandbox')
    self.options.add_argument('--start-maximized')
    self.options.add_argument('--start-fullscreen')
    self.options.add_argument('--single-process')
    self.options.add_argument('--disable-dev-shm-usage')
    self.options.add_argument("--disable-blink-features")
    self.options.add_argument("--disable-blink-features=AutomationControlled")
    self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
    self.options.add_experimental_option('useAutomationExtension', False)

    if userAgent:
      self.options.add_argument(f"user-agent={userAgent}")

    if useProxy:
      # proxy_plugin_file = self.create_selenium_proxy_plugin()
      # self.options.add_extension(proxy_plugin_file)
      PROXY = config.PROXY_URL
      self.options.add_argument(f'--proxy-server={PROXY}')

  def get(self):
    driver = webdriver.Chrome('/opt/chromedriver', options=self.options)
    # driver = webdriver.Chrome('C:/Users/Alwazan/Desktop/chromedriver.exe', options=self.options)
    return driver
  
  @staticmethod
  def create_selenium_proxy_plugin():
    PROXY_HOST = 'zproxy.lum-superproxy.io'
    PROXY_PORT = 22225
    PROXY_USER = 'lum-customer-c_d46d887f-zone-residential-country-us'
    PROXY_PASS = 't3079aaq6vx4'

    manifest_json = """
      {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
          "proxy",
          "tabs",
          "unlimitedStorage",
          "storage",
          "<all_urls>",
          "webRequest",
          "webRequestBlocking"
        ],
        "background": {
          "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
      }
    """

    background_js = """
      var config = {
        mode: "fixed_servers",
        rules: {
          singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
          },
          bypassList: ["localhost"]
        }
      };

      chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

      function callbackFn(details) {
        return {
          authCredentials: {
            username: "%s",
            password: "%s"
          }
        };
      }

      chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        {urls: ["<all_urls>"]},
        ['blocking']
      );
    """ % (PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS)

    # zip the plugin
    proxy_plugin_file = '/tmp/proxy_auth_plugin.zip'

    with zipfile.ZipFile(proxy_plugin_file, 'w') as zp:
      zp.writestr("manifest.json", manifest_json)
      zp.writestr("background.js", background_js)
    
    # return the zip file path
    return proxy_plugin_file



### HELPERS FUNCTIONS ###

# wait for element to be located
def waitForElementToBeLocated(driver, locator, locatorType=By.XPATH, timeout=10, pollFrequency=0.5):
  element = None
  try:
    print(f"Waiting for maximum :: {timeout} :: seconds for element to be visible")
    wait = WebDriverWait(driver, timeout, poll_frequency=pollFrequency,
                          ignored_exceptions=[NoSuchElementException,
                                              ElementNotVisibleException,
                                              ElementNotSelectableException])
    element = wait.until(EC.presence_of_element_located((locatorType, locator)))
  except:
    print("error: element not found!!!")
  return element

### END ###


### grubhub bot event
def grubhub_bot_event(event, context):
  with app.app_context():
    print("-------------------------- Grubhub Bot --------------------------")

    for record in event['Records']:

      subject, message = record.get("Sns").get("Subject"), eval(record.get("Sns").get("Message"))
      print(subject); print(message)

      ### check subject
      if subject != "merchant.status_change" and subject != "item.status_change" and subject != "item.update":
        continue

      ### unpack message details
      merchantId = message.get("body").get("merchantId")
      userId = message.get("body").get("userId")
      itemId = message.get("body").get("itemId")
      pauseTime = int(message.get("body").get("pauseTime")) if message.get("body").get("pauseTime") else 0
      oldItemStatus = int(message.get("body").get("oldItemStatus")) if message.get("body").get("oldItemStatus") is not None else None

      ### get merchant + all-virtual-merchant ids list
      vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
      merchants_list = [{
        "id": merchantId,
        "isVirtual": 0
      }]
      for vm in vms:
        merchants_list.append({
          "id": vm["id"],
          "isVirtual": 1,
          "virtualname": vm["virtualname"]
        })
      
      ### loop over virtual merchants
      for merchant in merchants_list:

        strike = 0
      
        # get platform credentials
        platform_creds_details = PlatformCredentials.get_platform_credentials(merchant["id"], platformType=5)#5=grubhub
        print(platform_creds_details)
        if not len(platform_creds_details):
          print("grubhub credentials not available. exiting...")
          continue
        platform_creds_details = platform_creds_details[0]
        accountUsername = platform_creds_details['email']
        accountPassword = platform_creds_details['password']

        # check if grubhub username and password are provided
        if not accountUsername or not accountPassword:
          print("username or password is empty. exiting...")
          continue
        
        # get merchant_details and item_details
        merchant_details = Merchants.get_merchant_by_id(merchantId)
        item_details = Items.get_item_by_id(itemId) if itemId is not None else None
        
        # check if subject is item.update and old item status is different from current item status
        if subject == "item.update" and oldItemStatus == item_details['itemStatus']:
          print("subject is item.update and status is not changed. exiting...")
          continue

        # form error message extension
        merchantName = merchant.get("virtualname") if merchant.get("virtualname") else merchant_details.get("merchantname")
        if subject == "merchant.status_change": 
          error_message_extension = f"<<<Please manually change the status of merchant <{merchantName}> to <{'Pause' if merchant_details['marketstatus'] == 0 else 'Resume'}> for <{pauseTime} minutes> on Grubhub>>>"
        else:
          error_message_extension = f"<<<Please manually change the status of item <{item_details.get('itemName')}> to <{'Pause' if item_details.get('itemStatus') == 0 else 'Resume'}> of merchant <{merchantName}> on Grubhub>>>"

        while True:
          try:
            
            ### init webdriver
            userAgent = get_random_user_agent()
            instance_ = WebDriver(userAgent=False, useProxy=True)
            driver = instance_.get()
            driver.delete_all_cookies()
            driver.set_window_size(1920, 1080)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # print some stuff
            print(driver.get_window_size())
            print(driver.execute_script("return navigator.userAgent;"))

            ### go to proxy url
            # driver.get("https://lumtest.com/myip.json")
            # time.sleep(2)
            # print(driver.page_source)
            
            ### go to base url
            base_url = "https://restaurant.grubhub.com/login"
            if subject == "item.status_change" or subject == "item.update":
              base_url = "https://restaurant.grubhub.com/menu"
            driver.get(base_url)
            time.sleep(4)
            print(driver.title)


            ### login to grubhub
            el_username = None
            el_password = None
            check_count = 0
            while True:
              check_count += 1
              el_username = waitForElementToBeLocated(driver, locator='//form[@class="login__form"]//div[@class="authentication-username"]//input')
              el_password = waitForElementToBeLocated(driver, locator='//form[@class="login__form"]//div[@class="authentication-password"]//input', timeout=2)
              
              if el_username and el_password:
                break
              else:
                if check_count == 1:
                  print("reloading page...")
                  driver.execute_script("location.reload()")
                  time.sleep(4)
                elif check_count == 2:
                  driver.get(base_url)
                  time.sleep(4)
                else:
                  raise Exception("error: email or password element not found!")

            el_username.send_keys(accountUsername)
            el_password.send_keys(accountPassword)

            btn_signin = driver.find_element(By.XPATH, '//form[@class="login__form"]//button')
            btn_signin.click()
            time.sleep(7)
            print(driver.title)

            ### check if login was unsuccessful
            if "/login" in driver.current_url:
              raise Exception("error: login failed, please check username and password")

            ### check for captcha
            if "captcha" in driver.title.lower():
              print("error: captcha!!!")

              # first strike -> call wait for 10 seconds and initialize the driver again. 2nd strike -> throw error in error_logs
              if strike == 0:
                driver.quit()
                strike = 1
                time.sleep(10)
                continue

              else:
                if subject == "merchant.status_change": 
                  raise Exception(f"captcha error!")
                else:
                  raise Exception(f"captcha error!")

            ##### Merchant Status Change
            if subject == "merchant.status_change":
              print("merchant.status_change event. Find pause resume button...")

              ### restaurant-status button
              btn_restaurantStatus = waitForElementToBeLocated(driver, locator='//div[@class="restaurant-status"]//button')
              if not btn_restaurantStatus:
                raise Exception("error: restautant status button not found!!!")

              print("Restaurant-Status Button is visible? " + str(btn_restaurantStatus.is_displayed()))
              print(btn_restaurantStatus.text)

              btn_restaurantStatusText = btn_restaurantStatus.text.lower()
              btn_restaurantStatus.click()
              time.sleep(3)


              ### check restaurant-status button text
              # if "stop" in button text then it means that restaurant is online
              # if "start" in button text then it means that restaurant is offline
              if "stop" in btn_restaurantStatusText and merchant_details['marketstatus'] == 0:
                print("restaruant status grubhub: online and restaurant status dashboard: paused. Pausing restaurant on grubhub...")

                # find radio btn
                # 30:30 minutes, 60:1 hour, 120:2 hours, 1440:Today, 0(infinite):Today
                if pauseTime == 30:
                  pauseTime = "30"
                elif pauseTime == 60:
                  pauseTime = "1 hour"
                elif pauseTime == 120:
                  pauseTime = "2 hour"
                else:
                  pauseTime = "Today"
                radio_xpath = f'//div[@class="gfr-modal gfr-mega-modal"]//div[@class="gfr-modal__content"]//div[@class="gfr-radio-button-group"]//span[starts-with(text(), "{pauseTime}")]/parent::node()/div[@class="gfr-radio"]'
                radio_opt = driver.find_element(By.XPATH, radio_xpath)
                radio_opt.click()
                time.sleep(3)

                btn_mdlStopOrders = driver.find_element(By.XPATH, '//div[@class="gfr-modal gfr-mega-modal"]//div[@class="gfr-modal__footer"]//button[contains(text(),"Stop")]')
                print(btn_mdlStopOrders)
                btn_mdlStopOrders.click()

              elif "start" in btn_restaurantStatusText and merchant_details['marketstatus'] == 1:
                print("restaruant status grubhub: paused and restaurant status dashboard: online. Resuming restaurant on grubhub...")
                btn_mdlStartOrders = driver.find_element(By.XPATH, '//div[@class="gfr-modal gfr-mega-modal"]//div[@class="gfr-modal__footer"]//button[contains(text(),"Start")]')
                print(btn_mdlStartOrders)
                btn_mdlStartOrders.click()
              
              elif "stop" in btn_restaurantStatusText and merchant_details['marketstatus'] == 1:
                print("merchant is already online on both dashboard and grubhub")
              elif "start" in btn_restaurantStatusText and merchant_details['marketstatus'] == 0:
                print("merchant is already paused on both dashboard and grubhub")
            

            ##### Item Status Change or Item Update
            elif (subject == "item.status_change" or subject == "item.update") and item_details is not None:
              print("item.status_change event. Navigate to menu tab...")

              ### extract item details
              itemName = item_details['itemName'].lower()
              itemStatus = item_details['itemStatus']
              
              # ### navigate to menus tab
              # print("navigate to menu tab...")
              # btn_menu_xpath = '//div[contains(text(), "Menu")]'
              # btn_menu = waitForElementToBeLocated(driver, locator=btn_menu_xpath)
              # if not btn_menu:
              #   raise Exception(f"error: menu button with xpath <{btn_menu_xpath}> not found on grubhub dashboard!")
              # btn_menu.click()
              # time.sleep(5)

              ### find search bar
              print("find search bar on menu tab...")
              # searchInput_xpath = '//input[@class="gfr-textfield__input gfr-textfield-text__input" and contains(@placeholder, "Search")]'
              searchInput_xpath = '//input[contains(@placeholder, "Search")]'
              el_searchInput = waitForElementToBeLocated(driver, locator=searchInput_xpath)
              if not el_searchInput:
                raise Exception(f"error: search field with xpath <{searchInput_xpath}> not found on grubhub menu screeen!")
              el_searchInput.click()

              el_searchInput = waitForElementToBeLocated(driver, locator=searchInput_xpath)
              el_searchInput.send_keys(itemName)
              el_searchInput.send_keys(Keys.RETURN)
              time.sleep(5)

              ### find the exact itemName's element
              print("find exact itemname and go to item details page...")
              itemName_xpath = f'//h4[@color="blue" and translate(normalize-space(text()), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz") = "{itemName}"]'
              el_itemName = driver.find_elements(By.XPATH, itemName_xpath)
              if not len(el_itemName):
                raise Exception(f"error: itemName <{item_details['itemName']}> not found on grubhub!")

              el_itemName[0].click()
              time.sleep(5)

              ### click on update availability button
              print("click on item availability button...")
              btn_updateAvailability = waitForElementToBeLocated(driver, locator='//button[text() = "Update availability"]')
              btn_updateAvailability.click()
              time.sleep(2)

              ### select radio btn and click done_button of modal to update availability of item
              print("select specific radio button and click on done_button...")
              if itemStatus == 1: # active
                print("item is active on apptopus dashboard")
                radio_xpath = '//div[@class="gfr-modal__dialog"]//span[@class="gfr-radio-button__label" and text() = "Available"]/parent::node()/div'
              elif itemStatus == 0: # inactive
                print("item is inactive on apptopus dashboard")
                radio_xpath = '//div[@class="gfr-modal__dialog"]//span[@class="gfr-radio-button__label" and contains(text(),"Archive")]/parent::node()/div'

              btn_radio = driver.find_element(By.XPATH, radio_xpath)
              btn_radio.click()
              time.sleep(1)

              btn_done = driver.find_element(By.XPATH, '//div[@class="gfr-modal__dialog"]//div[@class="gfr-modal__footer"]/button[contains(text(), "Done")]')
              btn_done.click()
              print("item status changed on grubhub")
            
            
            time.sleep(10)
            driver.quit()

            # Triggering activity_logs SNS
            print("Triggering activity logs sns ...")
            sns_msg = {
              "event": "grubhub_bot",
              "body": {
                "merchantId": merchant["id"],
                "userId": userId,
                "botOperationEvent": subject,
                "itemId": itemId,
                "pauseTime": pauseTime
              }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="grubhub_bot")

            print("--END--")
            break
          except Exception as e:
            print(f"Error {str(e)} {error_message_extension}")
            driver.quit()

            # Triggering SNS -> error_logs.entry
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": userId,
                    "merchantId": merchant["id"],
                    "errorName": "Grubhub Bot Error",
                    "errorSource": "grubhub_bot",
                    "errorStatus": 500,
                    "errorDetails": f"{str(e)} {error_message_extension}"
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")

            break
