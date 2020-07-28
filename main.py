from sqlalchemy import create_engine
from sqlalchemy.sql import text
import urllib
import pandas as pd
from upload import upload_blob
import plotly.express as px

from pdf_report import PDFReport

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
complicated_query_template = """DECLARE @colquery AS NVARCHAR(MAX),
@cols AS NVARCHAR(MAX),
 @query  AS NVARCHAR(MAX),
 @tbl as varchar(50),
 @objectid as varchar(50),
 @matchVariant as varchar(50);
 set @objectid={}
 set @matchVariant=300
 set @tbl='specialists_match'
SET @colquery = 'select @cols= STUFF((select distinct ''],['' + cast(a.matchtype as varchar(50))+''--''+case when b.label is null then ''No Match'' else b.label end as label from '+@tbl+' as a left join matchtype as b on b.matchtype=a.matchType where  matchVariant = @matchVariant FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)'') ,1,2,'''')+'']''' --objectid = @objectid and
EXEC sp_executesql @colquery, N'@matchVariant varchar(50),@objectid varchar(50),@cols VARCHAR(MAX) OUTPUT',
@matchVariant=@matchVariant,@objectid = @objectid,@cols = @cols OUTPUT
select @cols
set @query = 'select * from(select cast(a.matchtype as varchar(50))+''--''+case when b.label is null then ''No Match'' else b.label end as label, matchid,objectid,score from '+@tbl+' as a left join matchtype as b on b.matchtype=a.matchType where  matchVariant = @matchVariant) as piv pivot(max(score) for label in('+@cols+')) as pivotq'
--objectid = @objectid and
EXEC sp_executesql @query, N'@matchVariant varchar(50),@objectid varchar(50)',
@matchVariant=@matchVariant,@objectid = @objectid"""


def create_dataframe_from_sql(statement):
    with engine.connect() as con:
        res = con.execute(statement)

        rows = []

        for row in res:
            rows.append(row)

        df = pd.DataFrame(rows).rename(columns={i: v for i, v in enumerate(res.keys())})

        return df

factsheets_df = create_dataframe_from_sql(factsheets_adjust_statement)
news_df = create_dataframe_from_sql(news_adjust_statement)

factsheets_report = PDFReport()
news_report = PDFReport()

factsheets_report.add_css('bootstrap.min.css')
news_report.add_css('bootstrap.min.css')

styles = '''<style>
.table {
    font-size: 10px;
}
.table td, .table th, .table tr {
    text-align: center;
    padding: 5px 0 0 0;
}
</style>
'''

factsheets_report.add_html(styles)
news_report.add_html(styles)

for df, report in [(factsheets_df, factsheets_report), (news_df, news_report)]:
    for condition_title in df['ConditionTitle'].unique():
        condition_filtered_df = df.loc[df.ConditionTitle == str(condition_title)]

        top_baseline = condition_filtered_df.nlargest(10, 'baselinescore')
        top_final = condition_filtered_df.nlargest(10, 'finalscore')

        fig_baseline = px.histogram(condition_filtered_df, x="baselinescore")
        fig_final = px.histogram(condition_filtered_df, x="finalscore")

        report.add_html(
            f'<h1 style="text-align: center">{condition_title}</h1><br>'
        )
        report.add_figure(fig_baseline)
        report.add_html('<br>')
        report.add_figure(fig_final)
#     report.add_html(
#         '<p>Top baseline score doctors</p>'
#         '<ol>' + ''.join([f"<li>{each['nameFull']} - {each['baselinescore']}</li>" for index, each in top_baseline.iterrows()]) + '</ol>'
#     )
#     report.add_html(
#         '<p>Top final score doctors</p>'
#         '<ol>' + ''.join([f"<li>{each['nameFull']} - {each['finalscore']}</li>" for index, each in top_final.iterrows()]) + '</ol>'
#     )
        top_baseline_table = pd.DataFrame({'full name': top_baseline['nameFull'], 'score': top_baseline['baselinescore']})\
                .reset_index(drop=True)
        top_baseline_table.index = top_baseline_table.index + 1
        report.add_html(
            '<p>Top baseline score doctors</p>' + 
            top_baseline_table.to_html(header=False, classes=['table', 'table-condensed'])
        )

        top_final_table = pd.DataFrame({'full name': top_final['nameFull'], 'score': top_final['finalscore']})\
                .reset_index(drop=True)
        top_final_table.index = top_final_table.index + 1
        report.add_html(
            '<p>Top final score doctors</p>' + 
            top_final_table.to_html(header=False, classes=['table'])
        )

        insights_df = top_final\
                .drop(columns=['objectid', 'ConditionTitle', 'matchid'])\
                .rename(columns={f'adjustedbaselinescore{i}':f'adj{i}' for i in range(9)})\
                .rename(columns={
                'ConditionTitle': 'condition',
                'nameFull': 'name',
                'baselinescore': 'base',
                'finalscore': 'final'
            })
        report.add_html(
            insights_df.head(5).transpose().to_html(header=False, classes=['table'])
        )
        report.add_html(
            insights_df.tail(5).transpose().to_html(header=False, classes=['table'])
        )
        report.add_html('<br>')


factsheets_report.export_report_to_pdf('factsheets_report.pdf')
news_report.export_report_to_pdf('news_report.pdf')

factsheets_df.to_csv('factsheets.csv')
news_df.to_csv('news.csv')

upload_blob('factsheets_report.pdf', overwrite=True)
upload_blob('news_report.pdf', overwrite=True)

