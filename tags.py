from util import execute_sql, get_default_headers, bulk_insert
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
from bs4 import BeautifulSoup


def get_submissions():
    query = '''
                SELECT * FROM archive.submissions
                WHERE isxbrl
            '''
    df = execute_sql(query)
    return df


def parse_all(df):
    tag_df = pd.DataFrame(columns=["adsh", "taxonomy", "tagname", "contextref", "unitref", "value"])
    unit_df = pd.DataFrame(columns=["adsh", "unitid", "measure", "numerator", "denominator"])
    context_df = pd.DataFrame(columns=["adsh", "contextid", "startdate", "enddate", "instantdate", "dimension", "tag"])
    for i, row in df.iterrows():
        print(i)
        report = get_xbrl_report(row['cik'], row['adsh'], row['xbrldocument'])
        tag_df_report, unit_df_report, context_df_report = parse_report(report, row['adsh'])
        tag_df = tag_df.append(tag_df_report)
        unit_df = unit_df.append(unit_df_report)
        context_df = context_df.append(context_df_report)
    pre_sql = '''TRUNCATE archive.tags'''
    bulk_insert(tag_df, "tags", pre_sql)
    pre_sql = '''TRUNCATE archive.units'''
    bulk_insert(unit_df, "units", pre_sql)
    pre_sql = '''TRUNCATE archive.contexts'''
    bulk_insert(context_df, "contexts", pre_sql)


def get_xbrl_report(cik, adsh, xbrl_document):
    adsh_raw = adsh.replace("-", "")
    url = f"https://sec.gov/Archives/edgar/data/{cik}/{adsh_raw}/{xbrl_document}"
    r = requests.get(url, headers=get_default_headers())
    return r.content


def parse_report(report, adsh):
    root = ET.fromstring(report)
    tag_df = pd.DataFrame(columns=["adsh", "taxonomy", "tagname", "contextref", "unitref", "value"])
    unit_df = pd.DataFrame(columns=["adsh", "unitid", "measure", "numerator", "denominator"])
    context_df = pd.DataFrame(columns=["adsh", "contextid", "startdate", "enddate", "instantdate", "dimension", "tag"])
    for child in root:
        res = re.search("{(.+)}(.+)", child.tag)
        taxonomy, name = res.groups()
        if name == "context":
            row = {"adsh": adsh, "contextid": child.attrib.get("id")}
            dimensions = []
            for sub_context in child:
                res = re.search("{(.+)}(.+)", sub_context.tag)
                taxonomy, name = res.groups()
                if name == "period":
                    for date in sub_context:
                        res = re.search("{(.+)}(.+)", date.tag)
                        taxonomy, name = res.groups()
                        if name == "startDate":
                            row["startdate"] = date.text
                        elif name == "endDate":
                            row["enddate"] = date.text
                        elif name == "instant":
                            row["instantdate"] = date.text
                elif name == "entity":
                    for sub_entity in sub_context:
                        res = re.search("{(.+)}(.+)", sub_entity.tag)
                        taxonomy, name = res.groups()
                        if name == "segment":
                            for segment in sub_entity:
                                dimensions.append((segment.attrib.get("dimension"), segment.text))
            if len(dimensions) == 0:
                context_df = context_df.append(row, ignore_index=True)
            else:
                for dimension, tag in dimensions:
                    row["dimension"] = dimension
                    row["tag"] = tag
                    context_df = context_df.append(row, ignore_index=True)
        elif name == "unit":
            for sub_unit in child:
                res = re.search("{(.+)}(.+)", sub_unit.tag)
                taxonomy, name = res.groups()
                if name == "measure":
                    row = {"adsh": adsh, "unitid": child.attrib.get("id"), "measure": sub_unit.text}
                elif name == "divide":
                    row = {"adsh": adsh, "unitid": child.attrib.get("id")}
                    for sub_divide in sub_unit:
                        res = re.search("{(.+)}(.+)", sub_divide.tag)
                        taxonomy, name = res.groups()
                        if name == "unitNumerator":
                            for measure in sub_divide:
                                row["numerator"] = measure.text
                        elif name == "unitDenominator":
                            for measure in sub_divide:
                                row["denominator"] = measure.text
            unit_df = unit_df.append(row, ignore_index=True)
        elif child.text is None:
            pass
        else:
            text = child.text.strip()
            try:
                value = float(text)
                row = {"adsh": adsh, "taxonomy": taxonomy, "tagname": name,
                       "contextref": child.attrib.get("contextRef", ""),
                       "unitref": child.attrib.get("unitRef", ""), "value": value,
                       "isnumeric": 'true'
                       }
                tag_df = tag_df.append(row, ignore_index=True)
            except ValueError:
                if text != '' and not is_html(text):
                    row = {"adsh": adsh, "taxonomy": taxonomy, "tagname": name,
                           "contextref": child.attrib.get("contextRef", ""),
                           "unitref": child.attrib.get("unitRef", ""), "value": text,
                           "isnumeric": 'false'
                           }
                    tag_df = tag_df.append(row, ignore_index=True)
                else:
                    row = {"adsh": adsh, "taxonomy": taxonomy, "tagname": name,
                           "contextref": child.attrib.get("contextRef", ""),
                           "unitref": child.attrib.get("unitRef", ""), "value": "html",
                           "isnumeric": 'false'
                           }
                    tag_df = tag_df.append(row, ignore_index=True)
    return tag_df, unit_df, context_df

def is_html(text):
    return bool(BeautifulSoup(text, "html.parser").find())


if __name__ == "__main__":
    submissions_df = get_submissions()
    parse_all(submissions_df)
