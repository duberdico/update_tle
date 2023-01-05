import os
import re
import requests

import pandas as pd
from contextlib import closing

import sqlite3

GROUPS = ['weather','geo','gnss','satnogs','science','cubesat','radar','military','stations']
URL = "https://celestrak.org/NORAD/elements/gp.php"
CREATE_TABLE_SQL = 'CREATE TABLE IF NOT EXISTS "tle" ( "name" TEXT PRIMARY KEY, "other_name" TEXT, "status" TEXT, "l1" TEXT, "l2" TEXT, "station_group" TEXT, "updated_utc" TEXT);'
DBFILE = os.getenv('TLE_DATABASE')

def parse_TLE(data):
    lines = data.splitlines()

    out = []
    i = 0
    while i < len(lines):
        tmp = {}
        name = lines[i]
        other_name = re.findall(r"\(.*?\)", lines[i])
        if len(other_name) > 0:
            tmp["other_name"] = other_name[0][1:-1]
            name = name.replace(other_name[0], "")
        else:
            tmp["other_name"] = ""

        brackets = re.findall(r"\[.*?\]", lines[i])
        if len(brackets) > 0:
            tmp["status"] = brackets[0][1:-1]
            name = name.replace(brackets[0], "")
        else:
            tmp["status"] = ""

        tmp["name"] = name.strip().replace("'","_")

        tmp["l1"] = lines[i + 1]
        tmp["l2"] = lines[i + 2]
        out.extend([tmp])

        i += 3

    return out


def main():

    with closing(sqlite3.connect(DBFILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            conn.commit()

        for group in GROUPS:
            print(f"downloading TLE data for group {group}")
            query_dict = {"GROUP": group.upper(), "FORMAT": "TLE"}
            ret = requests.get(URL, params=query_dict)
            if ret.status_code == 200:
                try:
                    data = parse_TLE(ret.text)
                except:
                    print("... cannot parse return data - aborting!")
                    continue
                df = pd.DataFrame.from_dict(data).set_index("name")
                df["station_group"] = group
                df["updated_utc"] = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%S")
                print(f"...updating database")
                with closing(conn.cursor()) as cursor:
                    for i, r in df.reset_index().iterrows():
                        rdict = {k: f"'{v}'" for k, v in r.to_dict().items()}
                        cs = [f"{x}" for x in rdict.keys()]
                        sql = (
                            f"INSERT OR REPLACE INTO tle ({','.join(cs)})\n"
                            + f"VALUES ({','.join(rdict.values())});"
                        )
                        cursor.execute(sql)
                conn.commit()
            else:
                print(f"... unable to retrieve data (status code {data.status_code})")


if __name__ == "__main__":
    main()
