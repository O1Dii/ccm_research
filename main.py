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

adjust_statement_template = """select e.ConditionTitle,a.objectid,d.nameFull,a.matchid,b.score as baselinescore,
a.[0] as adjustedbaselinescore0,a.[1] as adjustedbaselinescore1,a.[2] as adjustedbaselinescore2,a.[3] as adjustedbaselinescore3,
a.[4] as adjustedbaselinescore4,a.[5] as adjustedbaselinescore5,a.[6] as adjustedbaselinescore6,a.[7] as adjustedbaselinescore7,
a.[8] as adjustedbaselinescore8,c.score as finalscore from(select objectid,matchid,adjustment,adjustType from {}) as
piv PIVOT(max(adjustment) for adjustType in([0],[1],[2],[3],[4],[5],[6],[7],[8])) as a inner join {} as b on a.objectid=b.objectid and
a.matchid=b.matchid left join ccm_index as c on c.objectid=b.objectid and c.matchid=b.matchid and
c.indextype=(select max(indextype) from ccm_index) inner join specialists as d on d.objectid=a.matchid inner join {} as e on e.objectid=a.objectid order by e.objectid"""

# factsheets_adjust  factsheets_baseline  factSheets
# news_adjust  news_baseline  news

match_300_query_template = """DECLARE @colquery AS NVARCHAR(MAX),
@cols AS NVARCHAR(MAX),
 @query  AS NVARCHAR(MAX),
 @tbl as varchar(50),
 @objectid as varchar(50),
 @matchVariant as varchar(50);
 set @objectid={objectid}
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

match_500_template = """select b.objectid,a.objectid as matchid, sum(b.score) as score, 500 as matchvariant 
from specialists as a 
inner join news_match b on b.matchid = a.objectid and b.matchvariant = {matchvariant}
group by b.objectid,a.objectid  
order by b.objectid,sum(b.score) desc"""

# 500 551 552 553 554

icd10_cpt_query_template = """select c.objectid, b.objectid as matchid,
{operation} as qty from specialists_exactmatch as a 
inner join specialists as b on a.objectid = b.objectid 
inner join news_match as c on c.matchid = a.matchid 
where a.matchvariant = 100 and c.matchType = {matchtype} and c.score >= 
(select top 1 score from (select distinct top 3 score 
from news_match where objectid = c.objectid and matchtype = {matchtype} order by score desc) as a order by score) 
group by c.objectid,b.objectid 
order by c.objectid,qty desc"""

# count(1) sum(a.perc) 
# 153 253

news_text_query_template = """select b.text, sum(a.qty) as qty  
from news_text as a 
inner join text as b on a.textid = b.textid 
where objectid = {objectid} and b.type = {type}
group by b.text 
order by qty desc"""

# 1 2

news_proc_query_template = """select c.text, d.text, sum(a.qty) as qty 
from news_proc as a 
inner join text_pair as b on a.tpid = b.tpid 
inner join text c on c.textid = b.textid1 
inner join text d on d.textid = b.textid2 
where a.objectid = {objectid} and c.text!=d.text 
group by c.text,d.text 
order by qty desc"""

news_text_classified_query_template = """select b.text, sum(a.qty) as qty 
from news_text_classified as a 
inner join text as b on a.textid = b.textid 
where a.objectid = {objectid} and b.type = 1 and a.classid = {classid} and a.qty > 1
group by b.text 
order by qty desc"""

# 6 3 1--

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

def create_dataframe_from_sql(statement, as_string=False):
    with engine.connect() as con:
        res = con.execute(statement)

        rows = []

        for row in res:
            rows.append(row)

        print(rows[:10])

        if as_string:
            return '\n'.join(map(lambda v: ', '.join(map(lambda x: str(x[1]), v.items())), rows))

        df = pd.DataFrame(rows).rename(columns={i: v for i, v in enumerate(res.keys())})

        return df

def assemble_report(condition_filtered_df, report, title):
    top_baseline = condition_filtered_df.nlargest(10, 'baselinescore')
    top_final = condition_filtered_df.nlargest(10, 'finalscore')

    fig_baseline = px.histogram(condition_filtered_df, x="baselinescore")
    fig_final = px.histogram(condition_filtered_df, x="finalscore")

    report.add_html(
        f'<h1 style="text-align: center">{title}</h1><br>'
    )
    report.add_figure(fig_baseline)
    report.add_html('<br>')
    report.add_figure(fig_final)

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

    report.add_html(
        '<p>' + str((10 - len(set(top_baseline['nameFull']) & set(top_final['nameFull']))) * 10) + '% of doctors from final top were not mentioned in baseline top</p>'
    )

factsheets_adjust_statement = text(adjust_statement_template.format('factsheets_adjust', 'factsheets_baseline', 'factSheets'))
news_adjust_statement = text(adjust_statement_template.format('news_adjust', 'news_baseline', 'news'))

factsheets_df = create_dataframe_from_sql(factsheets_adjust_statement)
news_df = create_dataframe_from_sql(news_adjust_statement)

factsheets_report = PDFReport()
total_factsheets_report = PDFReport()
news_report = PDFReport()
total_news_report = PDFReport()

factsheets_report.add_css('bootstrap.min.css')
total_factsheets_report.add_css('bootstrap.min.css')
news_report.add_css('bootstrap.min.css')
total_news_report.add_css('bootstrap.min.css')

factsheets_report.add_html(styles)
total_factsheets_report.add_html(styles)
news_report.add_html(styles)
total_news_report.add_html(styles)

for df, report, total_report in [(factsheets_df, factsheets_report, total_factsheets_report), (news_df, news_report, total_news_report)]:
    assemble_report(df, total_report, 'Total results')

    for variant in [500, 551, 552, 553, 554]:
        total_report.add_html(
            f'<p>Match {variant}</p>'
            '<p>' + str(create_dataframe_from_sql(text(match_500_template.format(matchvariant=variant)), as_string=True)) + '</p>'
        )

    for operation in ['count(1)', 'sum(a.perc)']:
        for matchtype in [153, 253]:
            total_report.add_html(
                f'<p>Match operation {operation} and matchtype {matchtype}</p>'
                '<p>' + str(create_dataframe_from_sql(text(icd10_cpt_query_template.format(operation=operation, matchtype=matchtype)), as_string=True)) + '</p>'
            )

    for condition_title in df.head(1000)['ConditionTitle'].unique():
        condition_filtered_df = df.loc[df.ConditionTitle == str(condition_title)]

        assemble_report(condition_filtered_df, report, condition_title)

        objectid = condition_filtered_df['objectid'].iloc[0]

        report.add_html(
            '<p>Match 300</p>'
            '<p>' + str(create_dataframe_from_sql(text(match_300_query_template.format(objectid=objectid)), as_string=True)) + '</p>'
        )

        for text_type in [1, 2]:
            report.add_html(
                f'<p>Match text of type {text_type}</p>'
                '<p>' + str(create_dataframe_from_sql(text(news_text_query_template.format(objectid=objectid, type=text_type)), as_string=True)) + '</p>'
            )
        
        report.add_html(
            '<p>Match proc</p>'
            '<p>' + str(create_dataframe_from_sql(text(news_proc_query_template.format(objectid=objectid)), as_string=True)) + '</p>'
        )

        for classid in ['6', '3', '1--']:
            report.add_html(
                f'<p>Match text classified of class {classid}</p>'
                '<p>' + str(create_dataframe_from_sql(text(news_text_classified_query_template.format(objectid=objectid, classid=classid)), as_string=True)) + '</p>'
            )

        report.add_html('<br>')

factsheets_report.export_report_to_pdf('factsheets_report.pdf')
total_factsheets_report.export_report_to_pdf('total_factsheets_report.pdf')
news_report.export_report_to_pdf('news_report.pdf')
total_news_report.export_report_to_pdf('total_news_report.pdf')

factsheets_df.to_csv('factsheets.csv')
news_df.to_csv('news.csv')

upload_blob('factsheets_report.pdf', overwrite=True)
upload_blob('total_factsheets_report.pdf', overwrite=True)
upload_blob('news_report.pdf', overwrite=True)
upload_blob('total_news_report.pdf', overwrite=True)

