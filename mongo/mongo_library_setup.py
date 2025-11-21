import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client.sf_library


patrons_col = db.patrons
libraries_col = db.libraries


patrons_col.create_index("library.name")
patrons_col.create_index("patron_type_code")
patrons_col.create_index("total_checkouts")
patrons_col.create_index("total_renewals")


csv_file = "SFPL_DataSF_library-usage_Jan_2023.csv"
df = pd.read_csv(csv_file, skiprows=12, header=0)


df.columns = [
    'patron_type_code',
    'patron_type_definition',
    'checkout_total',
    'renewal_total',
    'age_range',
    'home_library_code',
    'home_library_definition',
    'circulation_active_month',
    'circulation_active_year',
    'notification_medium_code',
    'notification_code_definition',
    'provided_email_address',
    'within_san_francisco_county',
    'year_patron_registered'
]



def determine_group(patron_type_code):
    if patron_type_code in [0]:
        return "adult"
    elif patron_type_code in [15, 5]:
        return "teacher/staff"
    elif patron_type_code in [2, 1]:
        return "youth"
    elif patron_type_code in [3]:
        return "senior"
    else:
        return "other"


for _, row in df.iterrows():
    email_bool = str(row.get('provided_email_address', '')).strip().lower() in ('true', 'yes', '1')

    patron_doc = {
        "patron_type_code": int(row['patron_type_code']) if pd.notnull(row['patron_type_code']) else None,
        "group": determine_group(row['patron_type_code']),
        "age_range": row.get("age_range"),
        "total_checkouts": int(row['checkout_total']) if pd.notnull(row['checkout_total']) else 0,
        "total_renewals": int(row['renewal_total']) if pd.notnull(row['renewal_total']) else 0,
        "library": {
            "code": row.get("home_library_code"),
            "name": row.get("home_library_definition"),
            "within_sf": str(row.get("within_san_francisco_county", False)).strip().lower() == "true"
        },
        "notification": {
            "code": row.get("notification_medium_code"),
            "definition": row.get("notification_code_definition")
        },
        "provided_email_address": email_bool,
        "circulation": {
            "month": row.get("circulation_active_month") if pd.notnull(row.get("circulation_active_month")) and str(row.get("circulation_active_month")).strip() != "" else None,
            "year": int(row['circulation_active_year']) if pd.notnull(row['circulation_active_year']) else None
            },
        "year_registered": int(row['year_patron_registered']) if pd.notnull(row['year_patron_registered']) else None
    }

    patrons_col.insert_one(patron_doc)



unique_libraries = df[['home_library_code', 'home_library_definition', 'within_san_francisco_county']].drop_duplicates()

for _, lib in unique_libraries.iterrows():
    lib_doc = {
        "_id": lib['home_library_code'],
        "name": lib['home_library_definition'],
        "within_sf": str(lib['within_san_francisco_county']).strip().lower() == "true"
    }
    libraries_col.update_one({"_id": lib['home_library_code']}, {"$set": lib_doc}, upsert=True)





db.command({
    "create": "adult_view",
    "viewOn": "patrons",
    "pipeline": [
        {"$match": {"group": "adult"}}
    ]
})


db.command({
    "create": "teacher_staff_view",
    "viewOn": "patrons",
    "pipeline": [
        {"$match": {"group": "teacher/staff"}}
    ]
})


db.command({
    "create": "youth_view",
    "viewOn": "patrons",
    "pipeline": [
        {"$match": {"group": "youth"}}
    ]
})

db.command({
    "create": "senior_view",
    "viewOn": "patrons",
    "pipeline": [
        {"$match": {"group": "senior"}}
    ]
})

