import pandas as pd
import requests
import zipfile
import re
import json
from bs4 import BeautifulSoup
from util import bulk_insert, get_default_headers


def get_submissions():
    print("downloading submissions.zip")
    url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    req = requests.get(url, headers=get_default_headers())
    with open("submissions.zip", 'wb') as output_file:
        output_file.write(req.content)
    print("finished downloading submissions.zip")


def parse_all(allowed_ciks):
    df = pd.DataFrame()
    with zipfile.ZipFile("submissions.zip", "r") as f:
        for filename in f.namelist():
            if any(cik in filename for cik in allowed_ciks):
                data = json.loads(f.read(filename))
                df = df.append(parse_submission(filename, data, ['10-K', '10-Q']))
    pre_sql = '''TRUNCATE archive.submissions'''
    bulk_insert(df, "submissions", pre_sql)


def parse_submission(filename, data, allowed_forms):
    if is_main_file(filename):
        df = pd.DataFrame.from_dict(data["filings"]["recent"])
    else:
        df = pd.DataFrame.from_dict(data)
    df = df[df["form"].isin(allowed_forms)].reset_index()
    df["cik"] = filename[3:13]
    df['xbrlDocument'] = df.apply(lambda row: get_xbrl_location(row['cik'], row['accessionNumber']), axis=1)
    cols = {
        "accessionNumber": "Adsh",
        "cik": "Cik",
        "filingDate": "FilingDate",
        "reportDate": "ReportDate",
        "form": "Form",
        "isXBRL": "IsXbrl",
        "primaryDocument": "PrimaryDocument",
        "xbrlDocument": "XbrlDocument"
    }
    df.rename(columns=cols, inplace=True)
    df = df[cols.values()]
    return df


def is_main_file(filename):
    return re.fullmatch(r"CIK\d{10}.json", filename)


def get_xbrl_location(cik, accessionNumber):
    accessionNumberRaw = accessionNumber.replace("-", "")
    url = f"https://sec.gov/Archives/edgar/data/{cik}/{accessionNumberRaw}/{accessionNumber}-index.html"

    r = requests.get(url, headers=get_default_headers())
    soup = BeautifulSoup(r.content, features="html.parser")
    next = False
    for row in soup.findAll("td"):
        if (next):
            return row.text
        if "XBRL INSTANCE DOCUMENT" in str(row):
            next = True
    return ""


if __name__ == "__main__":
    get_submissions()
    parse_all(["0000320193", "0000789019"])
