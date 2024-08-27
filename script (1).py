import smtplib
import time, os
import pandas as pd
import cloudscraper
from collections import defaultdict
from email.mime.text import MIMEText
from requests.utils import requote_uri
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart


runner_prices = defaultdict(list)


def save_to_excel(filename, data):
    df = pd.DataFrame(data)
    if os.path.exists(filename):
        df1 = pd.read_excel(filename)
        df2 = pd.concat([df1, df], ignore_index=True)
        df2.to_excel(filename, index=False)
    else:
        df.to_excel(filename, index=False)


def read_dict_excel(filename):
    df = pd.read_excel(filename)
    df = df.fillna("")
    data = df.to_dict('records')

    return data


def get_response(url):
    while True:
        try:
            header = {}
            header['User-Agent'] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            scraper = cloudscraper.create_scraper()
            r = scraper.get(requote_uri(url), headers=header, timeout=60)
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)
            print("Trying again...")
            time.sleep(5)
            continue
    
    scraper.close()
    return r


def get_main_url_today():
    today = datetime.utcnow().date()
    tomorrow = today

    formatted_date_start = tomorrow.strftime("%Y-%m-%dT00:00:00.000Z")
    formatted_date_end = tomorrow.strftime("%Y-%m-%dT23:59:59.999Z")

    main_url = f"https://apieds.betfair.com/api/eds/meeting-races/v4?_ak=nzIFcwyWhrlwYMrh&countriesGroup=%5B%5B%22GB%22,%22IE%22%5D%5D&eventTypeId=7&marketStartingAfter={formatted_date_start}&marketStartingBefore={formatted_date_end}"
    return main_url


def update_runner_prices(all_race_data):
    for race in all_race_data:
        market_name = race["Market Name"]
        race_time = race["Race Time"]
        runner_name = race["Runner Name"]
        back_price = race["Back Price"]
        lay_price = race["Lay Price"]

        print(race_time)

        runner_prices[(market_name, race_time, runner_name)].append((back_price, lay_price))


def get_main_url_tomorrow():
    today = datetime.utcnow().date()
    tomorrow = today + timedelta(days=1)

    formatted_date_start = tomorrow.strftime("%Y-%m-%dT00:00:00.000Z")
    formatted_date_end = tomorrow.strftime("%Y-%m-%dT23:59:59.999Z")

    main_url = f"https://apieds.betfair.com/api/eds/meeting-races/v4?_ak=nzIFcwyWhrlwYMrh&countriesGroup=%5B%5B%22GB%22,%22IE%22%5D%5D&eventTypeId=7&marketStartingAfter={formatted_date_start}&marketStartingBefore={formatted_date_end}"
    return main_url


def extract_time(datetime_str):
    datetime_obj = datetime.fromisoformat(datetime_str[:-1])  # Removing 'Z' at the end
    time_str = datetime_obj.strftime('%H:%M')
    
    return time_str


def scrape_market(market_id, market_name, race_time):
    r = get_response(f"https://ero.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak=nzIFcwyWhrlwYMrh&alt=json&currencyCode=GBP&locale=en_GB&marketIds={market_id}&rollupLimit=10&rollupModel=STAKE&types=MARKET_STATE,MARKET_RATES,MARKET_DESCRIPTION,EVENT,RUNNER_DESCRIPTION,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_METADATA,MARKET_LICENCE,MARKET_LINE_RANGE_INFO")
    data = r.json()["eventTypes"][0]["eventNodes"][0]["marketNodes"][0]["runners"]

    temp_list = []
    for horse in data:
        temp_dict = {}
        horse_name = horse["description"]["runnerName"]

        try:
            back_all_price_elem = horse["exchange"]["availableToBack"][0]
            back_all_price = back_all_price_elem["price"]
        except:
            back_all_price = "NA"

        try:
            lay_all_price_elem = horse["exchange"]["availableToLay"][0]
            lay_all_price = lay_all_price_elem["price"]
        except:
            lay_all_price = "NA"

        print(horse_name, back_all_price, lay_all_price)

        temp_dict["Market Name"] = market_name
        temp_dict["Market ID"] = market_id
        temp_dict["Race Time"] = race_time
        temp_dict["Runner Name"] = horse_name
        temp_dict["Back Price"] = back_all_price
        temp_dict["Lay Price"] = lay_all_price

        temp_list.append(temp_dict)

    return temp_list


def process_races(url_function):
    r = get_response(url_function())
    data = r.json()

    all_race_data = []
    first_race_time = None

    all_meetings = data[0]["meetings"]
    for meeting in all_meetings:
        name = meeting["name"].split(" ")[0]
        meeting_name = name

        all_meeting_races = meeting["races"]    
        for race in all_meeting_races:
            race_market_id = race["marketId"]
            race_market_name = race["marketName"]
            race_time = extract_time(race["startTime"])

            if first_race_time is None or race_time < first_race_time:
                first_race_time = race_time

            temp_dict = {
                "Meeting": meeting_name,
                "Race Name": race_market_name,
                "Race Market ID": race_market_id,
                "Race Time": race_time  # Convert back to string for output
            }

            all_race_data.append(temp_dict)

    print("Total Races Found: ", len(all_race_data))

    all_races_final_data = []

    for index, race_data in enumerate(all_race_data, start=1):
        market_id = race_data["Race Market ID"]
        market_name = race_data["Meeting"]
        race_time = race_data["Race Time"]
        
        print(f"Scraping Race #{index}: {market_id} - {market_name} - {race_time}")
        all_races_final_data.extend(scrape_market(market_id, market_name, race_time))
        print()

    return all_races_final_data, first_race_time


def send_email(subject, body):
    sender_email = "farhan.sherry159@gmail.com"  # Change this
    receiver_email = "farhanjavedjatt@gmail.com"  # Change this
    password = "thzg vchp fcsq qzbh"  # Change this

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:  # Change this
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())


def main():
    while True:
        now = datetime.now()

        if 22 <= now.hour or now.hour < 0:  # Between 10pm and 12am
            print("Scraping Tomrrow Prices")
            races, first_race_time = process_races(get_main_url_tomorrow)
            print(first_race_time)
            time_to_sleep = 900  # Sleep for 15 minutes

        elif 0 <= now.hour < first_race_time.hour:  # Between 12am and first race time
            print("Scraping Today Prices")
            races, first_race_time = process_races(get_main_url_today)
            time_to_sleep = 900  # Sleep for 15 minutes

        else:  # Between first race time and 10pm
            time_to_sleep = (datetime(now.year, now.month, now.day, 22, 0, 0) - now).seconds
            runner_prices.clear()

        update_runner_prices(races)
        subject = f"Runner Prices"
        body = ""
        for market_time, runner_data in runner_prices.items():
            market_name, race_time, runner_name = market_time
            if f"Market Name: {market_name}, Race Time: {race_time}" in body:
                body += f"{runner_name} "
            else:
                body += f"\n\nMarket Name: {market_name}, Race Time: {race_time}\n\n{runner_name} "
            for runner_info in runner_data:
                back_price, lay_price = runner_info
                body += f"({lay_price}, {back_price}) "

            body += "\n"

        send_email(subject, body.strip())
        
        print(f"Next run will start in {time_to_sleep} seconds.")
        time.sleep(time_to_sleep)


if __name__ == "__main__":
    print("Program Started!")
    main()
    print("Program Ended!")