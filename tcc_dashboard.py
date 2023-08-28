import json
import requests
import login
import pandas as pd
import numpy as np
from geopy import distance
from geopy.distance import geodesic
import datetime as dt
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from openpyxl import load_workbook
import os

pd.set_option('display.max_columns', None)

# ------------------ #
# Customer DB Params #
# ------------------ #

# Quilmes (439)
companyId = 439

# userName = "rodrigo.silva@wdtl.com"
# companyDf = pd.read_excel(r"C:/Users/Rodri/OneDrive/Documents/AoFrio/company/company_Rodrigo.xlsx") #Change directory
userName = "nicolas.lopez@wdtl.com"
companyDf = pd.read_excel(r"C:/Users/Servidor 1/Documents/GitHub/WDTL-Reports/company.xlsx") #Change directory

company = companyDf[companyDf["companyId"] == companyId]

db_url = company["endPoint"].values[0].replace('/parse/', '')
appId = company["appId"].values[0]

sessionToken = login.login(userName, db_url, appId)

header = {
    "Content-Type": "application/json",
    "X-Parse-Application-Id": appId,
    'X-Parse-Session-Token': sessionToken
}

# ------------ #
# Cooler Query #
# ------------ #

print("Extracting cooler data")
table = "/parse/classes/Cooler"

# Get total count of coolers for limiting number of iterations
urlParams = {
    "limit": "0",
    "count": "1",
    "where": json.dumps(
        {"usageStatus": {
                "$in": ["In service","Lost"]
                }
    })
}
response = requests.get(db_url + table, headers=header, params=urlParams)
row_count = response.json()["count"]
print("Initial total count:", row_count)

field_list = [
        "coolerId",
        "outletName",
        "customerId",
        "installAddress",
        "modelObj.modelId",
        "modelObj.capacity",
        "lastTracked",
        "lastStatTime",
        "usageStatus",
        "installLocationLatitude",
        "installLocationLongitude",
        "installLocationVerified",
        "lastLocation",
        "lastAddress",
        "lastTrackedBy.username",
        "distFromInstall",       
        "productionDate",
        "codeRev",
        "salesRoute",
        "oemId",
        "oemSerial",
        "productId",
        "customSalesOwner", # Asset ID
        "customTotalHeladeras", # Total Heladeras
        "customRepaga", # Repaga
        "customNoRepaga", # No Repaga
        "customCanal", # Canal
        "customSubCanal", # Subcanal
        "customDinner", # Marca
        "customNegocio", # Negocio
        "customJefedeVentas", # Jefe de Ventas
        "customNewfield", # Supervisor
        "customVendedor", # Vendedor
        "customDevice", # Device
        "customActivatedby", # Field/OEM
        "customRegion", # Region

]

last_updatedAt = 0
all_records_received = False
loop_num = 0
query_limit = 50000
max_loop = int(row_count / query_limit) + 1
cooler_df = pd.DataFrame()

while not all_records_received:

    print("From timestamp:", last_updatedAt)
    
    urlParams = {
        "keys": ",".join(field_list),
        "order": "updatedAt",
        "where": json.dumps({
            "updatedAt": {
                "$gt": {
                    "__type": "Date",
                    "iso": last_updatedAt
                }
            },
            "usageStatus": {
                "$in": ["In service","Lost"]
            },
            "customRegion": {
                "$in": ["Charcas","CHARCAS","charcas"]
            }
        }),
        "limit": str(query_limit)
    }
    response = requests.get(db_url + table, headers=header, params=urlParams)
    result_json = response.json()["results"]
    
    if len(result_json) > 0:
        result_df = pd.DataFrame.from_records(result_json)
        cooler_df = pd.concat([cooler_df, result_df], ignore_index=True, sort=False)
        last_updatedAt = result_df["updatedAt"].max()
        
        # In case more cooler records get updated after each query iteration
        if (loop_num == max_loop):
            all_records_received = True
        
    else:
        all_records_received = True
    
    loop_num += 1

print("Total records extracted:", len(cooler_df))

# Drop duplicates
coolerlist_df = cooler_df.drop_duplicates(subset=["objectId"], keep="last", ignore_index=True).copy()
#cooler_deduped_df.to_excel("test.xlsx", index=False)
print("Total unique records:", len(coolerlist_df))

# data transformation
coolerlist_df["capacity"] = coolerlist_df["modelObj"].apply(lambda x: x["capacity"] if pd.notna(x) else None)
coolerlist_df["modelId"] = coolerlist_df["modelObj"].apply(lambda x: x["modelId"] if pd.notna(x) else None)
coolerlist_df["lastTracked"] = coolerlist_df["lastTracked"].apply(lambda x: x["iso"] if pd.notna(x) else None)
coolerlist_df["lastTracked"] = coolerlist_df["lastTracked"].apply(lambda x: pd.to_datetime(x).to_datetime64() if pd.notna(x) else None )
coolerlist_df["productionDate"] = coolerlist_df["productionDate"].apply(lambda x: x["iso"] if pd.notna(x) else None)
coolerlist_df["productionDate"] = coolerlist_df["productionDate"].apply(lambda x: pd.to_datetime(x).to_datetime64() if pd.notna(x) else None )
coolerlist_df["lastStatTime"] = coolerlist_df["lastStatTime"].apply(lambda x: x["iso"] if pd.notna(x) else None)
coolerlist_df["lastStatTime"] = coolerlist_df["lastStatTime"].apply(lambda x: pd.to_datetime(x).to_datetime64() if pd.notna(x) else None )
coolerlist_df["lastLatitude"] = coolerlist_df["lastLocation"].apply(lambda x: x["latitude"] if pd.notna(x) else None)
coolerlist_df["lastLongitude"] = coolerlist_df["lastLocation"].apply(lambda x: x["longitude"] if pd.notna(x) else None)
coolerlist_df["distFromInstall"] = coolerlist_df["distFromInstall"].apply(lambda x: None if (pd.isna(x) or x in [-1, 999999999]) else int(x * 1000))
coolerlist_df["lastTrackedBy"] = coolerlist_df["lastTrackedBy"].apply(lambda x: x["username"] if pd.notna(x) else None)
coolerlist_df.drop(columns=["modelObj", "lastLocation"], inplace=True)

def controllerType(productId): 
        if productId == 1:
                return "SCS Gen 1"
        elif productId == 2:
                return "Ice Bank"
        elif productId == 3:
                return "Click"
        elif productId == 4:
                return "SCS Controller (Basic No Display)"
        elif productId == 6:
                return "SCS Black"
        elif productId == 7:
                return "Connect Network"
        elif productId == 16:
                return "Monitor"
        elif productId == 17:
                return "Unconnected Asset"
        else:
                return ""

coolerlist_df["productId"] = coolerlist_df["productId"].apply(lambda x: controllerType(x))

def ManufactNames(oemId):
        if oemId == "88":
                return "Imbera"
        elif oemId == "1":
                return "Metalfrio" 
        elif oemId == "2":
                return "Criotec"
        elif oemId == "439":
                return "Quilmes"
        elif oemId == "291":
                return "Inelro"
        elif oemId == "300":
                return "Briket"
        elif oemId == "81":
                return "Mimet"
        elif oemId == "79":
                return "Metalfrio Brazil"
        else:
                return "Unkown"
      
coolerlist_df["oemName"] = coolerlist_df["oemId"].apply(lambda x: ManufactNames(x) if pd.notna(x) else None)
coolerlist_df.reset_index(drop=True, inplace=True)
# print("Number of coolers in the dataframe: " + str(coolerlist_df.shape[0]))

# Calculating days since last visit and days missing data
coolerlist_df["Report Date"] = pd.to_datetime(datetime.today())
coolerlist_df["daysSinceReport"] = (coolerlist_df["Report Date"] - coolerlist_df["lastTracked"]).dt.days
coolerlist_df["daysMissingData"] = (coolerlist_df["Report Date"] - coolerlist_df["lastStatTime"]).dt.days

# Creating categorical conditions:

def track(daysreport):
  if  daysreport <= 30:
    return "<30"
  elif daysreport <= 60:
    return "<60"
  else:
    return ">60"

def data(daysmissingdata):
  if  daysmissingdata <= 30:
    return "<30"
  elif daysmissingdata <= 60:
    return "<60"
  else:
    return ">60"

coolerlist_df["Track"] = coolerlist_df[["daysSinceReport"]].apply(lambda x: track(*x), axis =1)
coolerlist_df["Data"] = coolerlist_df[["daysMissingData"]].apply(lambda x: data(*x), axis =1)
data_dummies = pd.get_dummies(coolerlist_df[["Track","Data"]])  
aud = pd.concat([coolerlist_df, data_dummies], axis = 1)
aud.dropna(subset=["customSalesOwner"], inplace=True) # Drop coolers without Asset ID
drop_list = ["Check OS","Pending","Test"] # List of Asset IDs to drop
aud = aud[~aud.customSalesOwner.isin(drop_list)]

print("Number of devices in the dataframe after dropping AssetID == Null: " + str(aud.shape[0]))

# ------------- #
# Get user role #
# ------------- #
print("Extracting user data")
table = "/parse/classes/_Role"

urlParams = {
    "keys": "objectId,title,name",
    "limit": "100000000",
    "where": json.dumps({
        "name": {"$nin": ["wdtl usermanager", "wdtl staff", "super user", "wdtl disabled", "disabled"]}
    })
}

response = requests.get(db_url + table, headers=header, params=urlParams)
role_data_json = response.json()["results"]
user_df_all = pd.DataFrame() # Will hold user role info

resourceUser = "/parse/classes/_User"
for role in role_data_json:

    userQueryParams = {
        "where": json.dumps({
            "$relatedTo": {
                "object": {
                    "__type": "Pointer",
                    "className": "_Role",
                    "objectId": role["objectId"]
                },
                "key": "users"
            }
        }),
        "limit": "1000000000", 
        "keys": "objectId,appAuthCode,fullname,username"
    }

    # Get users per role
    response = requests.get(db_url + resourceUser, headers=header, params=userQueryParams)
    user_data_json = response.json()["results"]

    if len(user_data_json) > 0:
        temp_df = pd.DataFrame.from_records(user_data_json, exclude=["objectId", "createdAt", "updatedAt"])
        temp_df["role"] = role["title"] if "title" in role.keys() else role["name"]
        user_df_all = pd.concat([user_df_all, temp_df], ignore_index=True, sort=False)

# merge user data with cooler data
aud = pd.merge(aud, user_df_all, how="left", left_on="lastTrackedBy", right_on="username")

print("Completed!")

# --------------- #
# asset stitching #
# --------------- #

asset_stitching_df = aud.loc[:,["coolerId", "customSalesOwner", "lastTracked", "lastStatTime"]].copy()
asset_stitching_df['PosPuerta'] = np.where(asset_stitching_df.coolerId.str.contains("I"), "Izquierdo",
                    np.where(asset_stitching_df.coolerId.str.contains("M"), "Medio",
                    np.where(asset_stitching_df.coolerId.str.contains("D"), "Derecho", "Derecho")))

data_dummies = pd.get_dummies(asset_stitching_df["PosPuerta"])  
asset_stitching_df = pd.concat([asset_stitching_df, data_dummies], axis=1)

df_I = asset_stitching_df[asset_stitching_df["Izquierdo"]>=1].copy()
df_M = asset_stitching_df[asset_stitching_df["Medio"]>=1].copy()
df_D = asset_stitching_df[asset_stitching_df["Derecho"]>=1].copy()

df_D["coolerIdD"]= df_D["coolerId"]
df_I["coolerIdI"]= df_I["coolerId"]
df_M["coolerIdM"]= df_M["coolerId"]
df_D.drop(columns=["coolerId","PosPuerta","Derecho","Izquierdo","Medio","lastTracked", "lastStatTime"], inplace=True)
df_I.drop(columns=["coolerId","PosPuerta","Derecho","Izquierdo","Medio","lastTracked", "lastStatTime"], inplace=True)
df_M.drop(columns=["coolerId","PosPuerta","Derecho","Izquierdo","Medio","lastTracked", "lastStatTime"], inplace=True)

asset_stitching_df.drop(columns=["coolerId","PosPuerta","Derecho","Izquierdo","Medio"], inplace=True)
asset_stitching_df_lastTracked = asset_stitching_df.groupby("customSalesOwner")["lastTracked"].max().reset_index()
asset_stitching_df_lastStatTime = asset_stitching_df.groupby("customSalesOwner")["lastStatTime"].max().reset_index()

asset_stitching_df_lastTracked_lastStatTime = asset_stitching_df_lastTracked.merge(asset_stitching_df_lastStatTime, how="inner", right_on="customSalesOwner", left_on="customSalesOwner")
asset_stitching_df.drop(columns=["lastTracked","lastStatTime"], inplace=True)
asset_stitching_df_lastTracked_lastStatTime = asset_stitching_df_lastTracked_lastStatTime.merge(df_I, how="left", right_on="customSalesOwner", left_on="customSalesOwner")
asset_stitching_df_lastTracked_lastStatTime = asset_stitching_df_lastTracked_lastStatTime.merge(df_M, how="left", right_on="customSalesOwner", left_on="customSalesOwner")
asset_stitching_df_lastTracked_lastStatTime = asset_stitching_df_lastTracked_lastStatTime.merge(df_D, how="left", right_on="customSalesOwner", left_on="customSalesOwner")

asset_stitching_df_lastTracked_lastStatTime.sort_values(by=["customSalesOwner", "lastTracked"], axis= 0, ascending=[False,True], inplace=True)

asset_stitching_df_lastTracked_lastStatTime.drop_duplicates(subset = "customSalesOwner",keep= "last", inplace=True, ignore_index=True)

asset_stitching_df = asset_stitching_df_lastTracked_lastStatTime

# Calculating days since last visit and days missing data
asset_stitching_df["Report Date"] = pd.to_datetime(datetime.today())
asset_stitching_df["daysSinceReport"] = (asset_stitching_df["Report Date"] - asset_stitching_df["lastTracked"]).dt.days
asset_stitching_df["daysMissingData"] = (asset_stitching_df["Report Date"] - asset_stitching_df["lastStatTime"]).dt.days

# Creating categorical conditions:

def track(daysreport):
  if  daysreport <= 30:
    return "<30"
  elif daysreport <= 60:
    return "<60"
  else:
    return ">60"

def data(daysmissingdata):
  if  daysmissingdata <= 30:
    return "<30"
  elif daysmissingdata <= 60:
    return "<60"
  else:
    return ">60"

asset_stitching_df["Track"] = asset_stitching_df[["daysSinceReport"]].apply(lambda x: track(*x), axis =1)
asset_stitching_df["Data"] = asset_stitching_df[["daysMissingData"]].apply(lambda x: data(*x), axis =1)
data_dummies = pd.get_dummies(coolerlist_df[["Track","Data"]])  
asset_stitching_df_dummies = pd.concat([asset_stitching_df, data_dummies], axis = 1)
asset_stitching_df_dummies.dropna(subset=["customSalesOwner"], inplace=True) # Drop coolers without Asset ID
drop_list = ["Check OS","Pending","Test"] # List of Asset IDs to drop
asset_stitching_df_dummies = asset_stitching_df_dummies[~asset_stitching_df_dummies.customSalesOwner.isin(drop_list)]

# ----------------- #
# CoolerVisit Query # 
# ----------------- #
table = "/parse/classes/CoolerVisit"

field_list = [
    "coolerId",
    # "location",
    "startLocalTimestamp",
    "startTimestampUtc",
    "endTimestampUtc",
]

urlParams = {
    "keys": (",".join(field_list)),
    "limit": "1000000",
}

current_date = datetime.today() 
previous_date = datetime.today() - relativedelta(months=3)

print("Querying data from: " + previous_date.strftime("%Y-%m-%d") + " to: " + current_date.strftime("%Y-%m-%d"))

urlParams["where"] = json.dumps({
    "startLocalTimestamp": {
        "$gte": {
            "__type": "Date",
            "iso": previous_date.strftime("%Y-%m-%d") + "T00:00:00.000Z"
        },
        "$lte": {
            "__type": "Date",
            "iso": current_date.strftime("%Y-%m-%d") + "T23:59:59.999Z"
        }
    }
})

response = requests.get(db_url + table, headers=header, params=urlParams)
visit_data_json = response.json()["results"]

# Exit program if no visit records found
if len(visit_data_json) == 0:
    exit()

visit_df = pd.DataFrame.from_records(visit_data_json)

# visit_df["lastLocationLatitude"] = visit_df["location"].apply(lambda x: (x["latitude"]) if pd.notna(x) else None)
# visit_df["lastLocationLongitude"] = visit_df["location"].apply(lambda x: (x["longitude"]) if pd.notna(x) else None)
visit_df["startLocalTimestamp"] = visit_df["startLocalTimestamp"].apply(lambda x: x["iso"])
visit_df["startDate"] = pd.to_datetime(visit_df["startLocalTimestamp"].apply(lambda x: x[:10]))
visit_df["year"] = visit_df["startDate"].dt.isocalendar().year
visit_df["week"] = visit_df["startDate"].dt.isocalendar().week
visit_df["day"] = visit_df["startDate"].dt.isocalendar().day
visit_df["weeklength"] = visit_df.week.astype(str).apply(len)

def yearweek(weeklength, year, week):
    if weeklength == 1:
        return str(year) + '0' + str(week)
    else: 
        return str(year) + str(week)

visit_df["yearweek"] = visit_df[["weeklength","year", "week"]].apply(lambda x: yearweek(*x), axis = 1)
visit_df.sort_values(by=["coolerId", "year", "week"], ascending=[True, False, False], inplace=True)
visit_df.drop_duplicates(subset=["coolerId", "year", "week"], keep="first", inplace=True)
visit_df.dropna(subset="coolerId", inplace=True)
visit_df.drop(columns=["objectId","createdAt","updatedAt","startTimestampUtc","endTimestampUtc","startLocalTimestamp","weeklength"], inplace=True)

#Listing all the yearweeks in visit_df
yearweek = visit_df[~visit_df["yearweek"].duplicated(keep="first")]
yearweek = yearweek[["yearweek"]].sort_values(by = "yearweek").reset_index(drop=True).copy()

#This loop creates a dataframe with all the yearweekvalues in visit_df
num = yearweek.yearweek.tolist()
c = coolerlist_df.coolerId.tolist()
df = pd.DataFrame()

for x in num:
    for y in c:
        a = {"col1":x, "col2": y}
        # df = df.append(a, ignore_index = True)
        df = pd.concat([df, pd.DataFrame(a, index=[0])], ignore_index=True)

df.rename(columns = {"col1":"yearweek", "col2": "coolerId"}, inplace= True)
df["yearweek_coolerId"] = df["yearweek"].astype(str) + "_" + df["coolerId"].astype(str) 
df["year"] = df.yearweek.str[:4]
df["week"] = df.yearweek.str[4:6]
df["day"] = "01"
df["startDate"] = pd.to_datetime(df.week.astype(str)+ df.year.astype(str).add('-1'), format= '%V%G-%u')

# setting visit_df.visit as True for all the visits 
visit_df["yearweek_coolerId"] = visit_df["yearweek"].astype(str) + "_" + visit_df["coolerId"].astype(str) 
visit_df["visit"] = True

# similarly, setting False for the coolers that weren't visited during that yearweek
novisit_df= df[~df['yearweek_coolerId'].isin(visit_df['yearweek_coolerId'])].copy()
novisit_df["visit"] = False

# concatenating both lists so that final list includes visits and nonvisits
coolerVisits = pd.concat([visit_df, novisit_df], axis = 0, ignore_index=True)

# merging with aud to get the customSalesOwner
coolerVisits = pd.merge(aud.loc[:,["coolerId", "customSalesOwner"]],coolerVisits, how="right", on="coolerId")

# creating a new column for yearmonth
coolerVisits["month"] = coolerVisits["startDate"].dt.month
coolerVisits["monthlength"] = coolerVisits.month.astype(str).apply(len)

def yearmonth(monthlength, year, month):
    if monthlength == 1:
        return str(year) + '0' + str(month)
    else: 
        return str(year) + str(month)

coolerVisits["yearmonth"] = coolerVisits[["monthlength","year", "month"]].apply(lambda x: yearmonth(*x), axis = 1)
# deleting nulls in customSalesOwner
coolerVisits.dropna(subset=["customSalesOwner"], inplace=True)
# creating a new column for yearmonth_customSalesOwner
coolerVisits["yearmonth_customSalesOwner"] = coolerVisits["yearmonth"].astype(str) + "_" + coolerVisits["customSalesOwner"].astype(str) 

# removing duplicates based on yearmonth_customSalesOwner
coolerVisits.sort_values(by=["yearmonth_customSalesOwner"], ascending=[False], inplace=True)
coolerVisits.drop_duplicates(subset=["yearmonth_customSalesOwner"], keep="first", inplace=True)

# ---------------- #
# Cooler Daily Ops #
# ---------------- #

def getDbParams():
    
    df =  pd.read_excel(r"C:/Users/Servidor 1/Documents/GitHub/WDTL-Reports/company.xlsx")  
    db_df = df[df["companyId"] == companyId]
    db_url = db_df["endPoint"].values[0].replace('/parse/', '')
    appId = db_df["appId"].values[0]
    sessionToken = login.login(userName, db_url, appId)

    header = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": appId,
        "X-Parse-Session-Token": sessionToken
    }

    return db_url, header


# ------------ #
# Cooler Query #
# ------------ #

def getCoolerInfo(db_url, header):

    table = "/parse/classes/Cooler"
    field_list = [
        "coolerId",
        "usageStatus",
        "productionDate",
        "customActivatedby"
    
    ]

    urlParams = {
    "keys": (",".join(field_list)),
    "limit": "1000000",
    "count": 1,
    "where": json.dumps(
        {
            "usageStatus": {
                "$in": ["In service","Lost"]
            },
            "customRegion": {
                "$in": ["Charcas","CHARCAS","charcas"]
            }
        })
    }

    response = requests.get(db_url + table, headers=header, params=urlParams)
    result_json = response.json()["results"]
    cooler_df = pd.DataFrame.from_records(result_json, exclude=["objectId", "createdAt", "updatedAt"])
    cooler_df["productionDate"] = cooler_df["productionDate"].apply(lambda x: x["iso"][:10] if pd.notna(x) else None)

    return cooler_df


# ---------------- #
# DailyStats Query #
# ---------------- #

def getCoolerDailyStat(db_url, header, period):

    table = "/parse/classes/CoolerDailyStat"
    dailystat_df = pd.DataFrame()

    today = datetime.today()
    start_date = (today - timedelta(days=period[0])).strftime("%Y-%m-%d") + "T00:00:00.000Z"
    end_date = (today - timedelta(days=period[1])).strftime("%Y-%m-%d") + "T23:59:59.999Z"

    urlParams = {
        "keys": "coolerId,statDate,do,t1a,t1ap,ws,wsr,wsp,subDayStat",
        "include": "subDayStat",
        "limit": "10000000000",
        "where": json.dumps({
            "statDate": {
                "$gte": {
                    "__type": "Date",
                    "iso": start_date 
                },
                "$lte": {
                    "__type": "Date",
                    "iso": end_date 
                }
            }
        })
    }
    response = requests.get(db_url + table, headers=header, params=urlParams)
    result_json = response.json()["results"]
    dailystat_df = pd.DataFrame.from_records(result_json, exclude=["createdAt", "updatedAt"])
    
    dailystat_df["stats"] = dailystat_df["subDayStat"].apply(lambda x: x["stats"] if "stats" in x.keys() else None)
    dailystat_df["statDate"] = dailystat_df["statDate"].apply(lambda x: x["iso"][:10])
    dailystat_df["onHours"] = dailystat_df["t1ap"].apply(lambda x: 24 if x >= 48 else round(x/2, 2))
    dailystat_df["wsTotal"] = dailystat_df.apply(lambda x: x["ws"] + x["wsr"] + x["wsp"], axis=1)
    dailystat_df["t1a"] = round(dailystat_df["t1a"], 2)

    return dailystat_df

def createReport(data_df, warm_sale_temp):

    # For calculating other stats
    stat_df = data_df[["coolerId", "statDate", "stats"]].explode("stats")
    stat_df = stat_df[(stat_df.stats.notnull()) & (stat_df["stats"].str.len() > 0)]

    # Get daily sum of motion counts
    stat_df["as"] = stat_df["stats"].apply(lambda x: x["as"] if "as" in x.keys() else 0)
    motion_count_df = stat_df[["coolerId", "statDate", "as"]].groupby(["coolerId", "statDate"], as_index=False).agg("sum").rename(columns={"as": "mc"})

    # Get daily avg temperature when opening door
    temp_do_df = stat_df.copy()
    temp_do_df["do"] = temp_do_df["stats"].apply(lambda x: x["do"] if "do" in x.keys() else 0)
    temp_do_df = temp_do_df[temp_do_df["do"] > 0]
    temp_do_df["t1a"] = temp_do_df["stats"].apply(lambda x: x["t1a"] if "t1a" in x.keys() else None)
    avg_temp_do_df = temp_do_df[["coolerId", "statDate", "t1a"]].groupby(["coolerId", "statDate"], as_index=False).agg("mean").rename(columns={"t1a": "t1a_do"})

    # Get daily avg temperature when out-of-spec
    temp_ws_df = stat_df.copy()
    temp_ws_df["ws"] = temp_ws_df["stats"].apply(lambda x: x["ws"] if "ws" in x.keys() else 0)
    temp_ws_df = temp_ws_df[temp_ws_df["ws"] > 0]
    temp_ws_df["t1a"] = temp_ws_df["stats"].apply(lambda x: x["t1a"] if "t1a" in x.keys() else None)
    temp_ws_df = temp_ws_df[temp_ws_df["t1a"] > warm_sale_temp]
    avg_temp_ws_df = pd.DataFrame()
    if len(temp_ws_df) > 0:
        avg_temp_ws_df = temp_ws_df[["coolerId", "statDate", "t1a"]].groupby(["coolerId", "statDate"], as_index=False).agg("mean").round(2).rename(columns={"t1a": "t1a_ws"})

    # Get last data upload event per cooler
    data_df["events"] = data_df["subDayStat"].apply(lambda x: x["events"] if "events" in x.keys() else None)
    event_df = data_df[["coolerId", "statDate", "events"]].copy()
    event_df = event_df[event_df.events.notnull()]
    event_df = event_df.explode("events")
    event_df["et"] = event_df["events"].apply(lambda x: str(x["et"]) if pd.notna(x) and "et" in x.keys() else None)
    upload_event_df = event_df[(event_df["et"] == "-16") | (event_df["et"] == "-21")].copy()
    upload_event_df["lt"] = upload_event_df["events"].apply(lambda x: x["lt"])

    # Create report
    report_df = pd.merge(data_df, avg_temp_do_df, how="left", on=["coolerId", "statDate"])
    report_df = report_df.merge(motion_count_df, how="left", on=["coolerId", "statDate"])
    if not avg_temp_ws_df.empty:
        report_df = report_df.merge(avg_temp_ws_df, how="left", on=["coolerId", "statDate"])
    else:
        report_df["t1a_ws"] = None

    return report_df, upload_event_df

def getWarmSaleTemp(db_url, header):

    table = "/parse/classes/ReportSettings"
    urlParams = {
        "keys": "warmSaleTemperature"
    }
    response = requests.get(db_url + table, headers=header, params=urlParams)
    warmSaleTemperature = response.json()["results"][0]["warmSaleTemperature"]

    return  warmSaleTemperature

if __name__ == "__main__":

    (db_url, header) = getDbParams()
    warm_sale_temp = getWarmSaleTemp(db_url, header)

    # Generate report data per week then merge into one at the end to avoid out of memory issue
    final_report_df = pd.DataFrame() # Will hold final data for reporting
    last_upload_df = pd.DataFrame() # Will hold indicator if last upload completed successfully

    # Last 90 days but divided into 3 days
    period_list = [
        (90, 88),
        (87, 85),
        (84, 82),
        (81, 79),
        (78, 76),
        (75, 73),
        (72, 70),
        (69, 67),
        (66, 64),
        (63, 61),
        (60, 56),
        (57, 55),
        (54, 52),
        (51, 49),
        (48, 46),
        (45, 41),
        (42, 40),
        (39, 38),
        (36, 34),
        (33, 31),
        (30, 28),
        (27, 25),
        (24, 22),
        (21, 19),
        (18, 16),
        (15, 13),
        (12, 10),
        (9, 7),
        (6, 4),
        (3, 1)
    ]

    print("Creating report for a weekly period...")
    for index, period in enumerate(period_list):

        print(f"Period #{index + 1}")
        dailystat_df = getCoolerDailyStat(db_url, header, period)

        (report_df, upload_event_df) = createReport(dailystat_df, warm_sale_temp)
        final_report_df = pd.concat([final_report_df, report_df], axis=0, ignore_index=True)
        last_upload_df = pd.concat([last_upload_df, upload_event_df], axis=0, ignore_index=True, sort=False)

print("Completed!")

# Delete duplicated coolers by concatenating coolerId and statDate
final_report_df = final_report_df.drop_duplicates(subset=["objectId"], keep="last", ignore_index=True).copy()

# merge with cooler info
final_report_df = final_report_df.merge(getCoolerInfo(db_url, header), on="coolerId")

# Filter out stats recorded prior to production date
final_report_df["priorProd"] = final_report_df[["statDate", "productionDate"]].apply(lambda x: True if x["statDate"] < x["productionDate"] else False, axis=1)
final_report_df = final_report_df[final_report_df["priorProd"] == False]

# reduce temperature in 2 degrees when customActivatedby == Field
final_report_df["t1a_do"] = final_report_df[["t1a_do", "customActivatedby"]].apply(lambda x: x["t1a_do"] - 2 if x["customActivatedby"] == "Field" else x["t1a_do"], axis=1)

# add isocalendar values of statDate
final_report_df["statDate"] = pd.to_datetime(final_report_df["statDate"].apply(lambda x: x[:10]))
final_report_df["year"] = final_report_df["statDate"].dt.isocalendar().year
final_report_df["week"] = final_report_df["statDate"].dt.isocalendar().week
final_report_df["day"] = final_report_df["statDate"].dt.isocalendar().day
final_report_df["weeklength"] = final_report_df.week.astype(str).apply(len)

def yearweek(weeklength, year, week):
    if weeklength == 1:
        return str(year) + '0' + str(week)
    else: 
        return str(year) + str(week)

final_report_df["yearweek"] = final_report_df[["weeklength","year", "week"]].apply(lambda x: yearweek(*x), axis = 1)

# create a column that combines yearweek and coolerId
final_report_df["yearweek_cooler"] = final_report_df["yearweek"].astype(str) + "_" + final_report_df["coolerId"].astype(str)


# --------------- #
# Complete report #
# --------------- #

#Ordering columns
FieldOrder = [
    "coolerId",
    "statDate",
    "year",
    "week",
    "day",
    "yearweek",
    "yearweek_cooler",
    "do",
    "wsTotal",
    "mc",
    "t1a",
    "t1a_do",
    "t1a_ws",
    "onHours"
]
final_report_df = final_report_df.reindex(columns=FieldOrder)

# delete all the rows with null values in t1a_do
final_report_df.dropna(subset=["t1a_do"], inplace=True)

#-------------------#
# Data Aggregations #
#-------------------#

# extracting from the aud dataframe
asset_df = aud.loc[:,["coolerId", "customSalesOwner", "customerId", "customNegocio","customTotalHeladeras","customRepaga","customNoRepaga","capacity","modelId",]].copy()

# creating a new column with the position of the device. Will be useful to aggregate data based on position
asset_df['PosPuerta'] = np.where(asset_df.coolerId.str.contains("I"), "Izquierdo",
                    np.where(asset_df.coolerId.str.contains("M"), "Medio",
                    np.where(asset_df.coolerId.str.contains("D"), "Derecho", "Derecho")))

# Capacity has some null values. The Average Capacity is 680, so we're going to replace all NaN values to their average
# To identify replaced capacity values, see modelId == None
asset_df["capacity"] = asset_df.capacity.fillna(680)

# merging asset_df with final_report_df
asset_stats_df = final_report_df.merge(asset_df, on="coolerId", how="left")

asset_stats_df["yearweek_assetId"] = asset_stats_df["yearweek"].astype(str) + "_" + asset_stats_df["customSalesOwner"].astype(str) 

# Define the list of values to replace
values_to_replace = ['Izquierdo', 'Medio']

# Replace the values in the 'A' column of the DataFrame with None
mask = asset_stats_df['PosPuerta'].isin(values_to_replace)
asset_stats_df.loc[mask, 't1a_do'] = None

# Calculate aggregate data
pivot_df = asset_stats_df.pivot_table(index=["customSalesOwner","yearweek"], values=["t1a_do","do","onHours","t1a"], aggfunc={"t1a_do":"mean","do":"sum","onHours":"mean","t1a":"count"}).reset_index()
pivot_df = pivot_df.rename(columns={"t1a_do":"avg_t1a_do","do":"sum_do","onHours":"avg_onHours","t1a":"daysDownloaded"})
pivot_df["yearweek_assetId"] = pivot_df["yearweek"].astype(str) + "_" + pivot_df["customSalesOwner"].astype(str) 

# merge with asset_df
pivot_df = pivot_df.merge(asset_df, on="customSalesOwner", how="left")

# merge with asset_stitching_df
pivot_df = pivot_df.merge(asset_stitching_df, on="customSalesOwner", how="left")

# dropping columns to avoid confusion
pivot_df.drop(columns=["coolerId","PosPuerta"], inplace=True)

# by the end of this script, we will have the data aggregated by yearweek and assetId,
# the t1a_do column will have the average temperature when the door was opened, where Monitors installed in the field have been reduced by 2 degrees
# whenever there was no capacity specified, the capacity has been replaced by the average capacity of all the devices
# door openings are expressed as the sum of all door opens on every device inside an asset for a given week
# enoughData is a boolean column that indicates if the asset has enough data to be considered for the analysis, it is calculated based on the number of days downloaded
# it shall take 6 days to consider a full week. If the asset has less than 6 days downloaded, previous week will be considered as as the last week with enough data
# the doorOpening limit shall be specified as 2.40 due to the early implementation days where this was below the average door opening for the whole fleet   

pivot_df["enoughData"] = np.where(pivot_df["daysDownloaded"] >= 6, True, False)
pivot_df["doorOpeningLimit"] = 2.40
pivot_df["doorOpeningsPerDay"] = pivot_df["sum_do"] / pivot_df["daysDownloaded"]
pivot_df["doorOpeningsPer100LitersPerDay"] = pivot_df["doorOpeningsPerDay"]*100/pivot_df["capacity"]

# formatting stat columns
pivot_df["avg_t1a_do"] = pivot_df["avg_t1a_do"].round(2)
pivot_df["sum_do"] = pivot_df["sum_do"].round(2)
pivot_df["avg_onHours"] = pivot_df["avg_onHours"].round(2)
pivot_df["doorOpeningsPerDay"] = pivot_df["doorOpeningsPerDay"].round(2)
pivot_df["doorOpeningsPer100LitersPerDay"] = pivot_df["doorOpeningsPer100LitersPerDay"].round(2)

# Followed by the classification of the temperature

def temperatura_Cat(negocio, t):
  if negocio == '1-CERVEZAS CMQ' and t <= 4:
    return 'Óptima (<4)'
  elif negocio == '1-CERVEZAS CMQ' and 4 < t <= 8:
    return 'Medianamente Alta (4<8)'
  elif negocio == '1-CERVEZAS CMQ' and t > 8:
    return 'Alta (>8)'

  elif negocio == '3-UNG' and t <= 0:
    return ' Congela Heladera (<0)'
  elif negocio == '3-UNG' and 0 < t <= 6:
    return 'Óptima (0<6)'
  elif negocio == '3-UNG' and 6 < t <= 10:
    return 'Medianamente Alta (6<10)'
  elif negocio == '3-UNG' and  t > 10:
    return 'Alta (>10)'  
  else:
    return 

pivot_df['TempClassifier'] = pivot_df[['customNegocio', 'avg_t1a_do']].apply(lambda x: temperatura_Cat(*x), axis =1)

# and the classification of Repaga/No Repaga/ Mixto

pivot_df["customRepaga"] = pivot_df.customRepaga.astype(float)
pivot_df["customNoRepaga"] = pivot_df.customNoRepaga.astype(float)
pivot_df["customTotalHeladeras"] = pivot_df.customNoRepaga.astype(float)

pivot_df['Repaga'] = np.where(pivot_df.customRepaga/pivot_df.customTotalHeladeras == 1, 1, 0)
pivot_df['NoRepaga'] = np.where(pivot_df.customNoRepaga/pivot_df.customTotalHeladeras == 1, 1, 0)

def repaga_Cat(Repaga, NoRepaga):
    if Repaga ==  0 and NoRepaga == 0:
        return 'Sin Datos'
    elif Repaga == 1 and NoRepaga == 0:
        return 'No Repaga'
    if Repaga ==  1:
        return 'Repaga'
    elif NoRepaga ==  1:
        return 'No Repaga'
    elif Repaga ==  0 and NoRepaga == 0:
        return 'Sin Datos'

pivot_df['RepClassifier'] = pivot_df[['Repaga', 'NoRepaga']].apply(lambda x: repaga_Cat(*x), axis =1)
# pivot_df

# Classifying assets based on statistics
# There shall be 9 classes of assets: Ganador, Posible Invasión (Comercial), No vende Heladera, Perdedor, Ventas, Cooler Chico, Posible Invasión (Técnico), Ticket Soporte Alto, Ticket Soporte Bajo
# The classification is based on the following criteria:

# 1. Heladera Chica: Assets with TempClassifier != 'Óptima (<4)' OR 'Óptima (0<6)', avg_onHours >= 19 and RepClassifier = 'Repaga'  
# 2. No vende Heladera: Assets with less or equal than 2.40 door openings per 100 liters per day, TempClassifier = 'Óptima (<4)' OR 'Óptima (0<6)', and RepClassifier = 'Repaga' 
# 3. Apagado de Heladera: Assets with TempClassifier != 'Óptima (<4)' OR 'Óptima (0<6)', and avg_onHours <= 19
# 4. Ganadora: Assets with more than 2.40 door openings per 100 liters per day, TempClassifier = 'Óptima (<4)' OR 'Óptima (0<6)', and RepClassifier = 'Repaga' 
# 5. Perdedora: Assets with less or equal than 2.40 door openings per 100 liters per day, TempClassifier = 'Óptima (<4)' OR 'Óptima (0<6)', and RepClassifier = 'No Repaga' 
# 6. Posible Invasión (Comercial): Assets with more than 2.40 door openings per 100 liters per day, TempClassifier = 'Óptima (<4)' OR 'Óptima (0<6)', and RepClassifier = 'No Repaga' 
# 7. Posible Invasión (Técnico): Assets with TempClassifier != 'Óptima (<4)' OR 'Óptima (0<6)', avg_onHours >= 19, and RepClassifier = 'No Repaga' 
# 8. Ticket Soporte Alto: Assets with TempClassifier != 'Alta (>10)' OR 'Alta (>8)', avg_onHours >= 19
# 9. Ticket Soporte Bajo: Assets with TempClassifier != 'Medianamente Alta (4<8)' OR 'Medianamente Alta (6<10)', avg_onHours >= 19
# 10. Sin Datos Repago: Assets with RepClassifier = 'Sin Datos'


def classify_asset(TempClassifier, avg_onHours, RepClassifier, doorOpeningsPer100LitersPerDay):
    if doorOpeningsPer100LitersPerDay > 2.40 and (TempClassifier == "Óptima (<4)" or TempClassifier == "Óptima (0<6)") and RepClassifier == "Repaga":
        return "Ganadora"
    elif doorOpeningsPer100LitersPerDay > 2.40 and (TempClassifier == "Óptima (<4)" or TempClassifier == "Óptima (0<6)") and RepClassifier == "No Repaga":
        return "Posible Invasión"   
    elif doorOpeningsPer100LitersPerDay <= 2.40 and (TempClassifier == "Óptima (<4)" or TempClassifier == "Óptima (0<6)") and RepClassifier == "Repaga":
        return "No vende Heladera"
    elif doorOpeningsPer100LitersPerDay <= 2.40 and (TempClassifier == "Óptima (<4)" or TempClassifier == "Óptima (0<6)") and RepClassifier == "No Repaga":
        return "Perdedora"
    elif (TempClassifier != "Óptima (<4)" or TempClassifier != "Óptima (0<6)") and avg_onHours <= 19:
        return "Apagado de Heladera"
    elif (TempClassifier != "Óptima (<4)" or TempClassifier != "Óptima (0<6)") and avg_onHours >= 19 and RepClassifier == "Repaga":
        return "Heladera Chica"
    elif (TempClassifier != "Óptima (<4)" and TempClassifier != "Óptima (0<6)") and avg_onHours >= 19 and RepClassifier == "No Repaga":
        return "Posible Invasión"
    elif (TempClassifier == "Alta (>10)" or TempClassifier == "Alta (>8)") and avg_onHours >= 19:
        return "Ticket Soporte Alto"
    elif (TempClassifier == "Medianamente Alta (4<8)" or TempClassifier == "Medianamente Alta (6<10)") and avg_onHours >= 19:
        return "Ticket Soporte Bajo"
    elif RepClassifier == "Sin Datos":
        return "Sin Datos Repago"
    else:
        return "Sin Clasificar"

pivot_df['ClassHipotesis'] = pivot_df[["TempClassifier", "avg_onHours", "RepClassifier", "doorOpeningsPer100LitersPerDay"]].apply(lambda x: classify_asset(*x), axis =1)

# pivot_df

# calculate previous weeks for each week in the dataset

# Create empty DataFrame
dateDict = pd.DataFrame(columns=['year', 'week', 'yearweek', 'prev_yearweek', 'prev_2_yearweek', 'prev_3_yearweek'])

# Set start and end dates
start_date = datetime.now().date()
end_date = start_date - timedelta(days=120)

# Loop through dates and add rows to DataFrame
while start_date >= end_date:
    year = start_date.isocalendar()[0]
    week = start_date.isocalendar()[1]
    week_1 = (start_date - timedelta(days=7)).isocalendar()[1]
    week_2 = (start_date - timedelta(days=14)).isocalendar()[1]
    week_3 = (start_date - timedelta(days=21)).isocalendar()[1]
    yearweek = f"{year}{week:02}"
    prev_yearweek = f"{year}{week_1:02}"
    prev_2_yearweek = f"{year}{week_2:02}"
    prev_3_yearweek = f"{year}{week_3:02}"
    
    dateDict = dateDict.append({'year': year, 'week': week, 'yearweek': yearweek, 'prev_yearweek': prev_yearweek, 'prev_2_yearweek': prev_2_yearweek, 'prev_3_yearweek': prev_3_yearweek}, ignore_index=True)
    
    start_date -= timedelta(days=7)

# maximizing the yearweek based on whether enoughData is True
pivot_df2 = pivot_df[pivot_df["enoughData"] == True].pivot_table(index=['customSalesOwner'], values=['yearweek'], aggfunc='max').reset_index()

# merging the yearweek with the dateDict to grab previous weeks
pivot_df2 = pivot_df2.merge(dateDict, on='yearweek', how='left')
pivot_df2.drop(columns=['year','week'], inplace=True)

# concatenating yearweek + "_" + customAssetId
pivot_df2['yearweek_customAssetId'] = pivot_df2['yearweek'] + "_" + pivot_df2['customSalesOwner']
pivot_df2['yearweek-1_customAssetId'] = pivot_df2['prev_yearweek'] + "_" + pivot_df2['customSalesOwner']
pivot_df2['yearweek-2_customAssetId'] = pivot_df2['prev_2_yearweek'] + "_" + pivot_df2['customSalesOwner']
pivot_df2['yearweek-3_customAssetId'] = pivot_df2['prev_3_yearweek'] + "_" + pivot_df2['customSalesOwner']

# Creating separate dataframes with previous hypotheses
yearweek_0 = pivot_df2.loc[:,["yearweek_customAssetId"]].merge(pivot_df.loc[:,["yearweek_assetId", "ClassHipotesis"]], left_on='yearweek_customAssetId', right_on="yearweek_assetId", how='left')
yearweek_0.rename(columns={'ClassHipotesis': 'ClassHipotesis_0'}, inplace=True)
yearweek_0.drop(columns=['yearweek_assetId'], inplace=True)

yearweek_1 = pivot_df2.loc[:,["yearweek-1_customAssetId"]].merge(pivot_df.loc[:,["yearweek_assetId", "ClassHipotesis"]], left_on='yearweek-1_customAssetId', right_on="yearweek_assetId", how='left')
yearweek_1.rename(columns={'ClassHipotesis': 'ClassHipotesis_1'}, inplace=True)
yearweek_1.drop(columns=['yearweek_assetId'], inplace=True)

yearweek_2 = pivot_df2.loc[:,["yearweek-2_customAssetId"]].merge(pivot_df.loc[:,["yearweek_assetId", "ClassHipotesis"]], left_on='yearweek-2_customAssetId', right_on="yearweek_assetId", how='left')
yearweek_2.rename(columns={'ClassHipotesis': 'ClassHipotesis_2'}, inplace=True)
yearweek_2.drop(columns=['yearweek_assetId'], inplace=True)

yearweek_3 = pivot_df2.loc[:,["yearweek-3_customAssetId"]].merge(pivot_df.loc[:,["yearweek_assetId", "ClassHipotesis"]], left_on='yearweek-3_customAssetId', right_on="yearweek_assetId", how='left')
yearweek_3.rename(columns={'ClassHipotesis': 'ClassHipotesis_3'}, inplace=True)
yearweek_3.drop(columns=['yearweek_assetId'], inplace=True)

pivot_df2 = pivot_df2.merge(yearweek_0, on='yearweek_customAssetId', how = "left").merge(yearweek_1, on='yearweek-1_customAssetId', how = "left").merge(yearweek_2, on='yearweek-2_customAssetId', how = "left").merge(yearweek_3, on='yearweek-3_customAssetId', how = "left")

# add the full subset of coolers 

pivot_df2.drop_duplicates(subset=['yearweek_customAssetId'], inplace=True, ignore_index=True)
notinpivot = asset_df.loc[:,["customSalesOwner"]].copy()
notinpivot['ClassHipotesis_0'] = "Datos Incompletos"

# merge the two dataframes based on the 'id' column
merged_df = pd.merge(pivot_df2, notinpivot, on='customSalesOwner', how='outer', suffixes=('_pivot', '_notinpivot'))

# filter the merged dataframe to include only the records that don't exist in the first dataframe
filtered_df = merged_df[merged_df['customSalesOwner'].isin(notinpivot['customSalesOwner']) & ~merged_df['customSalesOwner'].isin(pivot_df2['customSalesOwner'])]
filtered_df.rename(columns={'ClassHipotesis_0_notinpivot': 'ClassHipotesis_0'}, inplace=True)

# append the filtered records to the first dataframe
pivot_df2 = pivot_df2.append(filtered_df[['customSalesOwner', 'ClassHipotesis_0']], ignore_index=True)
pivot_df2 = pivot_df2.merge(pivot_df.loc[:,["yearweek_assetId", "doorOpeningsPer100LitersPerDay" ,"avg_onHours", "avg_t1a_do"]], left_on='yearweek_customAssetId', right_on="yearweek_assetId", how='left')

# Write to file
output_file = "quilmes - sources.xlsx"
# output_path = os.path.join("C:/Users/Rodri/OneDrive/Documents/AoFrio", output_file)
output_path = os.path.join("C:/Users/Servidor 1/Documents/GitHub/WDTL-Reports/dashboardQUILMES/Output", output_file) #Change output folder

with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    aud.to_excel(writer, sheet_name="Devices", index=False)
    asset_stitching_df_dummies.to_excel(writer, sheet_name="Assets", index=False)
    coolerVisits.to_excel(writer, sheet_name="Visits", index=False)
    pivot_df.to_excel(writer, sheet_name="HistoricalWeeklyOps", index=False)
    pivot_df2.to_excel(writer, sheet_name="Hypotheses", index=False)
    # current_df.to_excel(writer, sheet_name="Visitados Mês Atual",columns = output_col_dict_6.values(),index=False)