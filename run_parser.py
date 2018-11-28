import pandas as pd
import requests
import json
import xlrd
import openpyxl
import threading
from multiprocessing.dummy import Pool as ThreadPool
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from time import sleep

pd.options.mode.chained_assignment = None
CURRENT_SHEET = None
EXCEL_FILENAME = "test.xlsx"
OUTPUT_FOLDER = "output/"
EXCEL_OUTPUT = "_out.xlsx"
MAX_THREADS = 100
SAVE_EVERY_ROWS = 2000
START_FROM = 0
AUTH_KEY = os.environ.get('AUTH_KEY')

s = requests.Session()
headers = {
    'content-type': 'application/json',
    'authorization': AUTH_KEY
}
retries = Retry(total=4, backoff_factor=1)
s.mount('http://', HTTPAdapter(max_retries=retries))
s.headers.update(headers)


def parse_row(row_number):
    print("Parse row: " + str(row_number))
    global CURRENT_SHEET
    ''' function update data in row,
    currently it add "aaa" text to distance, later should be request method here to get value '''
    row = read_row(CURRENT_SHEET, row_number)
    rest_data = get_rest_data(row)  # here we should assign value received from api request

    if not rest_data:
        print("Failed to get row: " + str(row_number))
        return {}

    row[4] = rest_data['distance']
    row[5] = rest_data['amount']
    #update_row(CURRENT_SHEET, row_number, row)
    return {row_number: row}


def get_rest_data(row):
    post_from = str(row[0]).zfill(5)
    city_from = row[1]
    post_to = str(row[2]).zfill(5)
    city_to = row[3]

    coordinates_from = get_x_y(post_from, city_from)
    coordinates_to = get_x_y(post_to, city_to)

    if not coordinates_from or not coordinates_to:
        return False

    x_from = coordinates_from["x"]
    y_from = coordinates_from["y"]
    x_to = coordinates_to["x"]
    y_to = coordinates_to["y"]
    #print("X1 {} Y1 {} X2 {} Y2 {}".format(x_from, y_from, x_to, y_to))
    distance = get_distance(x_from, y_from, x_to, y_to)
    #print(distance)
    return distance


XY_CACHE = {}
def get_x_y(post_code, city):
    global XY_CACHE
    url = "https://xserver2-europe-eu-test.cloud.ptvgroup.com/services/rs/XLocate/searchLocations"

    data = {
        "$type": "SearchByAddressRequest",
        "scope": "globalscope",
        "storedProfile": "default",
        "coordinateFormat": "EPSG:4326",
        "address": {
            "postalCode": post_code,
            "city": city
        }
    }

    cache_key = post_code + city
    if cache_key in XY_CACHE:
        #print("CACHE: " + cache_key)
        return XY_CACHE[cache_key]
    else:
        #print("REQUEST: " + cache_key)
        try:
            for i in range(0, 3):
                r = s.post(url, data=json.dumps(data))
                r_json = json.loads(r.content)
                x = r_json['results'][0]['location']["referenceCoordinate"]["x"]
                y = r_json['results'][0]['location']["referenceCoordinate"]["y"]

                if not x or not y:
                    sleep(1)
                    continue
                XY_CACHE[cache_key] = {"x": x, "y": y}
                return {"x": x, "y": y}
        except requests.exceptions.Timeout as e:
            # Maybe set up for a retry, or continue in a retry loop
            print e
            return False
        except requests.exceptions.TooManyRedirects as e:
            # Tell the user their URL was bad and try a different one
            print e
            return False
        except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
            print e
            return False
        except Exception as e:
            print e
            return False


DISTANCE_CACHE = {}
def get_distance(x1, y1, x2, y2):
    url = "https://xserver2-europe-eu-test.cloud.ptvgroup.com/services/rs/XRoute/calculateRoute"

    data = {
        "waypoints": [
            {
                "$type": "OffRoadWaypoint",
                "location": {
                    "offRoadCoordinate": {
                        "x": x1,
                        "y": y1
                    }
                }
            },
            {
                "$type": "OffRoadWaypoint",
                "location": {
                    "offRoadCoordinate": {
                        "x": x2,
                        "y": y2
                    }
                }
            }
        ],
        "requestProfile": {
            "routingProfile": {
                "course": {
                    "distanceTimeWeighting": 50
                }
            }
        },

        "storedProfile": "truck40t.xml",
        "resultFields": {
            "toll": {
                "enabled": True
            }
        }
    }

    try:
        for i in range(0, 3):
            r = s.post(url, data=json.dumps(data))
            r_json = json.loads(r.content)
            distance = r_json["distance"]
            amount = r_json["toll"]["summary"]["costs"][0]["amount"]
            if not distance or not amount:
                print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                sleep(1)
                continue
            return { "distance": distance, "amount": amount }
    except requests.exceptions.Timeout as e:
        # Maybe set up for a retry, or continue in a retry loop
        print e
        return False
    except requests.exceptions.TooManyRedirects as e:
        # Tell the user their URL was bad and try a different one
        print e
        return False
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        print e
        return False
    except Exception as e:
        print e
        return False


def read_row(sheet, row_number):
    # df2.loc[startrow:endrow, startcolumn:endcolumn], from documentation
    row = sheet.loc[row_number, :]
    return row


def update_row(sheet, row_number, new_row):
    sheet.loc[row_number, :] = new_row


def save_to_excel(sheet, sheet_name):
    global EXCEL_OUTPUT
    writer = pd.ExcelWriter(OUTPUT_FOLDER + sheet_name + EXCEL_OUTPUT)
    sheet.to_excel(writer, sheet_name, index_label=False, index=False, header=True)
    writer.save()

def processSheet(sheet, sheet_name):
    global CURRENT_SHEET
    global MAX_THREADS
    CURRENT_SHEET = sheet
    sheet_rows, sheet_cols = sheet.shape
    pool = ThreadPool(MAX_THREADS)

    from_id = START_FROM
    to_id = START_FROM + SAVE_EVERY_ROWS -1

    break_next_loop = False
    while True:
        print("Loop from: " + str(from_id) + " to: " + str(to_id))
        data = pool.map(parse_row, range(from_id, to_id+1))

        for item in data:
            for id, val in item.items():
                update_row(CURRENT_SHEET, id, val)

        save_to_excel(CURRENT_SHEET, sheet_name + "_0_" + str(to_id) + "_")

        if break_next_loop:
            break

        to_id += SAVE_EVERY_ROWS
        from_id += SAVE_EVERY_ROWS

        if to_id > sheet_rows-1:
            to_id = sheet_rows-1
            break_next_loop = True

# apckages: pandas, xlrd, openpyxl
print("Opening {}".format(EXCEL_FILENAME))
xls = pd.ExcelFile(EXCEL_FILENAME)
SHEET_NAMES = xls.sheet_names
print("Sheets: {}".format(SHEET_NAMES))

sheets = []

for sheet_name in SHEET_NAMES:
    print("Reading... " + sheet_name)
    sheets.append(pd.read_excel(EXCEL_FILENAME, sheet_name=sheet_name))

counter = 0
for sheet in sheets:
    print("Processing sheet: " + SHEET_NAMES[counter])
    processSheet(sheet, SHEET_NAMES[counter])
    counter += 1

exit
'''

print(sheets[1].head())
# print rows/cols
print(sheets[1].shape)
'''