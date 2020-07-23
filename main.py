from sqlalchemy import create_engine
from sqlalchemy.sql import text
import urllib
import pandas as pd

server='tcp:ysm-sync-prod1.ysmdevs.com'
driver='{ODBC Driver 17 for SQL Server}'
username='tridion'
password='Tr1d10n2013'
odbc_string=f'Server={server};Driver={driver};UID={username};PWD={password};Database=ccm_dev;'
odbc_params=urllib.parse.quote_plus(odbc_string)

engine = create_engine(f'mssql+pyodbc:///?odbc_connect={odbc_params}')

factsheets_adjust_statement = text("""select e.ConditionTitle,a.objectid,d.nameFull,a.matchid,b.score as baselinescore,
a.[0] as adjustedbaselinescore0,a.[1] as adjustedbaselinescore1,a.[2] as adjustedbaselinescore2,a.[3] as adjustedbaselinescore3,
a.[4] as adjustedbaselinescore4,a.[5] as adjustedbaselinescore5,a.[6] as adjustedbaselinescore6,a.[7] as adjustedbaselinescore7,
a.[8] as adjustedbaselinescore8,c.score as finalscore from( select objectid,matchid,adjustment,adjustType from factsheets_adjust) as
piv PIVOT(max(adjustment) for adjustType in([0],[1],[2],[3],[4],[5],[6],[7],[8])) as a inner join factsheets_baseline as b on a.objectid=b.objectid and
a.matchid=b.matchid left join ccm_index as c on c.objectid=b.objectid and c.matchid=b.matchid and
c.indextype=1000 inner join specialists as d on d.objectid=a.matchid inner join factSheets as e on e.objectid=a.objectid order by e.objectid""")
news_adjust_statement = text("""select e.ConditionTitle,a.objectid,d.nameFull,a.matchid,b.score as baselinescore,
a.[0] as adjustedbaselinescore0,a.[1] as adjustedbaselinescore1,a.[2] as adjustedbaselinescore2,a.[3] as adjustedbaselinescore3,
a.[4] as adjustedbaselinescore4,a.[5] as adjustedbaselinescore5,a.[6] as adjustedbaselinescore6,a.[7] as adjustedbaselinescore7,
a.[8] as adjustedbaselinescore8,c.score as finalscore from(select objectid,matchid,adjustment,adjustType from news_adjust) as
piv PIVOT(max(adjustment) for adjustType in([0],[1],[2],[3],[4],[5],[6],[7],[8])) as a inner join news_baseline as b on a.objectid=b.objectid and
a.matchid=b.matchid left join ccm_index as c on c.objectid=b.objectid and c.matchid=b.matchid and
c.indextype=1000 inner join specialists as d on d.objectid=a.matchid inner join news as e on e.objectid=a.objectid order by e.objectid""")


def create_dataframe_from_sql(statement):
    with engine.connect() as con:
        res = con.execute(statement)

        rows = []

        for row in res:
            rows.append(row)

        df = pd.DataFrame(rows)

        return df

print(create_dataframe_from_sql(factsheets_adjust_statement))
print(create_dataframe_from_sql(news_adjust_statement))

