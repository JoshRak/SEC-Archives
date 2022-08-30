from util import execute_sql


def common_tags(ciks, years, numeric=False):
    if type(ciks) != list:
        ciks = [ciks]
    if type(years) != list:
        years = [years]

    all_tags = set()
    first = True
    for cik in ciks:
        for year in years:
            query = f'''
                SELECT 
                    submissions.cik,
                    EXTRACT(YEAR from submissions.reportdate) as reportyear,
                    tags.tagname,
                    MAX(tags.tagvalue) as tagvalue,
                    tags.isnumeric
                FROM secarchives.archive.tags
                INNER JOIN secarchives.archive.submissions ON submissions.adsh = tags.adsh
                WHERE
                    EXTRACT(YEAR from reportdate) = {year} AND
                    submissions.cik = '{cik}'
                GROUP BY cik, tagname, isnumeric, EXTRACT(YEAR from reportdate)
                '''
            res = execute_sql(query)
            tags = set(res["tagname"])
            if first:
                all_tags = tags
                first = False
            else:
                all_tags = all_tags.intersection(tags)
    return len(all_tags)


if __name__ == "__main__":
    ciks = {
        "AAPL": "0000320193",
        "MSFT": "0000789019",
        "AMZN": "0001018724",
        "NFLX": "0001065280",
        "TSLA": "0001318605"
    }
    print(common_tags([ciks["MSFT"], ciks["AAPL"]], [2017, 2018]))
