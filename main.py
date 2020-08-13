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
a.[8] as adjustedbaselinescore8,c.score as finalscore from(select objectid,matchid,adjustment,adjustType from {0}_adjust) as
piv PIVOT(max(adjustment) for adjustType in([0],[1],[2],[3],[4],[5],[6],[7],[8])) as a inner join {0}_baseline as b on a.objectid=b.objectid and
a.matchid=b.matchid left join ccm_index as c on c.objectid=b.objectid and c.matchid=b.matchid and
c.indextype=(select max(indextype) from ccm_index) inner join specialists as d on d.objectid=a.matchid inner join {0} as e on e.objectid=a.objectid order by e.objectid"""

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
inner join {0}_match b on b.matchid = a.objectid and b.matchvariant = {matchvariant}
group by b.objectid,a.objectid  
order by b.objectid,sum(b.score) desc"""

# 500 551 552 553 554

icd10_cpt_query_template = """select c.objectid, b.objectid as matchid,
{operation} as qty from specialists_exactmatch as a 
inner join specialists as b on a.objectid = b.objectid 
inner join {0}_match as c on c.matchid = a.matchid 
where a.matchvariant = 100 and c.matchType = {matchtype} and c.score >= 
(select top 1 score from (select distinct top 3 score 
from {0}_match where objectid = c.objectid and matchtype = {matchtype} order by score desc) as a order by score) 
group by c.objectid,b.objectid 
order by c.objectid,qty desc"""

# count(1) sum(a.perc) 
# 153 253

text_query_template = """select b.text, sum(a.qty) as qty  
from {0}_text as a 
inner join text as b on a.textid = b.textid 
where objectid = {objectid} and b.type = {type}
group by b.text 
order by qty desc"""

# 1 2

proc_query_template = """select c.text, d.text, sum(a.qty) as qty 
from {0}_proc as a 
inner join text_pair as b on a.tpid = b.tpid 
inner join text c on c.textid = b.textid1 
inner join text d on d.textid = b.textid2 
where a.objectid = {objectid} and c.text!=d.text 
group by c.text,d.text 
order by qty desc"""

text_classified_query_template = """select b.text, sum(a.qty) as qty 
from {0}_text_classified as a 
inner join text as b on a.textid = b.textid 
where a.objectid = {objectid} and b.type = 1 and a.classid = {classid} and a.qty > 1
group by b.text 
order by qty desc"""

# 6 3 1--

# select top 10 a.objectid,a.namefull, 
# (select contents from _am_ym_content where type = ‘specialist’ and id = a.objectid and key1=‘ymurl’) as profileurl
# from specialists a

specialists_url_query = """select a.id, a.contents, b.objectid, b.namefull
from _am_ym_content as a
inner join specialists as b on a.id=b.objectid
where a.type='specialist' and a.key1='ymurl'
"""

url_query_template = """select a.id,a.contents,b.objectid,b.ConditionTitle,b.ymsid 
from _am_ym_content as a 
inner join {0} as b on a.id=b.tcmid 
where a.type='{contenttype}' and key1 in('ymurl')"""

# story(news) concept(factsheets)

styles = '''<style>
.table {
    font-size: 10px;
}
.table td, .table tr {
    text-align: center;
    padding: 5px 0 0 0;
}
.table thead th {
    text-align: center;
}
.table tbody tr th {
    text-align: center;
}
</style>
'''

def create_dataframe_from_sql(statement, as_string=False):
    with engine.connect() as con:
        res = con.execute(statement)

        rows = []

        for row in res:
            rows.append(row)

        if as_string:
            return '\n'.join(map(lambda v: ', '.join(map(lambda x: str(x[1]), v.items())), rows))

        df = pd.DataFrame(rows).rename(columns={i: v for i, v in enumerate(res.keys())})

        return df

def to_html_or_empty_string(df, **kwargs):
    if df.empty:
        return ''
    else:
        return df.to_html(**kwargs)

def get_difference_percent(df):
    top_baseline = df.nlargest(10, 'baselinescore')
    top_final = df.nlargest(10, 'finalscore')

    return (10 - len(set(top_baseline['nameFull']) & set(top_final['nameFull']))) * 10

def add_zoomed_histograms(report, df, measurement, difference_value=5):
    latest_series = df[measurement]

    while True:
        latest_max = latest_series.max()
        latest_mean = latest_series.mean()

        if latest_max / latest_mean > difference_value:
            current_df = df[df[measurement] < latest_mean]
            latest_series = current_df[measurement]

            report.add_figure(px.histogram(current_df, x=measurement))
            report.add_html(f'<br><p>{measurement} with {latest_mean} as max</p><br>')
        else:
            break

def assemble_report_base(condition_filtered_df, report, title, url='', specialists_url_df=None):
    if url:
        top_baseline = condition_filtered_df.nlargest(10, 'baselinescore')
        top_final = condition_filtered_df.nlargest(10, 'finalscore')
        top_title_template = 'Doctors {} top by score'
    else:
        top_baseline = condition_filtered_df.nlargest(200, 'baselinescore').groupby('nameFull').count().reset_index().sort_values('baselinescore', ascending=False)
        top_final = condition_filtered_df.nlargest(200, 'finalscore').groupby('nameFull').count().reset_index().sort_values('finalscore', ascending=False)
        top_title_template = 'Number of times each doctor appeared in {} top 200'

    top_baseline_table = pd.DataFrame({'full name': top_baseline['nameFull'], 'score': top_baseline['baselinescore']}).reset_index(drop=True)
    top_final_table = pd.DataFrame({'full name': top_final['nameFull'], 'score': top_final['finalscore']}).reset_index(drop=True)

    if specialists_url_df is not None:
        top_baseline = top_baseline.merge(
            specialists_url_df[['namefull', 'contents']], how='left', left_on='nameFull', right_on='namefull', suffixes=['', '_specialists']
        ).drop(columns=['namefull'])
        top_final = top_final.merge(
            specialists_url_df[['namefull', 'contents']], how='left', left_on='nameFull', right_on='namefull', suffixes=['', '_specialists']
        ).drop(columns=['namefull'])

        top_baseline_table['url'] = top_baseline['contents']
        top_final_table['url'] = top_final['contents']

    top_baseline_table.index = top_baseline_table.index + 1
    top_final_table.index = top_final_table.index + 1

    fig_baseline = px.histogram(condition_filtered_df, x="baselinescore")
    fig_final = px.histogram(condition_filtered_df, x="finalscore")

    baseline = condition_filtered_df['baselinescore']
    final = condition_filtered_df['finalscore']

    report.add_html(
        f'<h1 style="text-align: center">{title}</h1><br>'
    )
    report.add_html('<a style="text-align: center">' + url + '</a>')
    report.add_figure(fig_baseline)
    report.add_html(f'<p>Average value of baseline score is {baseline.mean()}<br>Median is {baseline.median()}<br>Minimum is {baseline.min()}<br>Maximum is {baseline.max()}</p><br>')
    add_zoomed_histograms(report, condition_filtered_df, 'baselinescore')

    report.add_figure(fig_final)
    report.add_html(f'<p>Average value of final score is {final.mean()}<br>Median is {final.median()}<br>Minimum is {final.min()}<br>Maximum is {final.max()}</p><br>')
    add_zoomed_histograms(report, condition_filtered_df, 'finalscore')

    report.add_html(
        '<p>' + top_title_template.format('baseline') + '</p>' +
        top_baseline_table.to_html(header=False, classes=['table', 'table-condensed'])
    )

    report.add_html(
        '<p>' + top_title_template.format('final') + '</p>' +
        top_final_table.to_html(header=False, classes=['table', 'table-condensed'])
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
        '<p>' + str(100 - (len(set(top_baseline['nameFull']) & set(top_final['nameFull'])) / len(top_baseline)) * 100) + '% of doctors from final top were not mentioned in baseline top</p>'
    )

def create_report_for_theme(name):
    df = create_dataframe_from_sql(text(adjust_statement_template.format(name)))
    report = PDFReport()
    total_report = PDFReport()
    icd10_cpt_report = PDFReport()

    report.add_css('bootstrap.min.css')
    total_report.add_css('bootstrap.min.css')
    icd10_cpt_report.add_css('bootstrap.min.css')
    report.add_html(styles)
    total_report.add_html(styles)
    icd10_cpt_report.add_html(styles)

    url_df = create_dataframe_from_sql(text(url_query_template.format(name, contenttype=('story' if name == 'news' else 'concept'))))
    specialists_url_df = create_dataframe_from_sql(text(specialists_url_query))

    assemble_report_base(df, total_report, f'Total {name} results', specialists_url_df=specialists_url_df)

    difference_percents = []
    for condition_title in df['ConditionTitle'].unique():
        condition_filtered_df = df.loc[df.ConditionTitle == str(condition_title)]

        difference_percents.append(get_difference_percent(condition_filtered_df))

    total_report.add_figure(px.histogram(pd.DataFrame({'difference percents': difference_percents}), x='difference percents'))

    # for variant in [500, 551, 552, 553, 554]:
    #     local_df = create_dataframe_from_sql(text(match_500_template.format(name, matchvariant=variant)))

    #     total_report.add_html(
    #         f'<p>Match {variant}</p>' +
    #         to_html_or_empty_string(local_df, index=False, classes=['table', 'table-condensed'])
    #     )

    for operation in ['count(1)', 'sum(a.perc)']:
        for matchtype in [153, 253]:
            local_df = create_dataframe_from_sql(text(icd10_cpt_query_template.format(name, operation=operation, matchtype=matchtype)))

            icd10_cpt_report.add_html(
                f'<p>Match operation {operation} and matchtype {matchtype}</p>' +
                to_html_or_empty_string(local_df.head(20), index=False, classes=['table', 'table-condensed'])
            )

    print(name, 'total report done')

    # return df, report, total_report, icd10_cpt_report

    amt = len(df['ConditionTitle'].unique())
    counter = 1

    for condition_title in df['ConditionTitle'].unique():
        condition_filtered_df = df.loc[df.ConditionTitle == str(condition_title)]

        objectid = condition_filtered_df['objectid'].iloc[0]

        assemble_report_base(condition_filtered_df, report, condition_title, str(url_df[url_df['objectid'] == objectid]['contents'].iloc[0]), specialists_url_df=specialists_url_df)

        report.add_html(
            '<p>Match 300</p>'
            '<p>' + str(create_dataframe_from_sql(text(match_300_query_template.format(name, objectid=objectid)), as_string=True)) + '</p>'
        )

        for text_type in [1, 2]:
            report.add_html(
                f'<p>Match text of type {text_type}</p>' +
                to_html_or_empty_string(create_dataframe_from_sql(text(text_query_template.format(name, objectid=objectid, type=text_type))), index=False, classes=['table', 'table-condensed'])
            )
        
        report.add_html(
            '<p>Match proc</p>' +
            to_html_or_empty_string(create_dataframe_from_sql(text(proc_query_template.format(name, objectid=objectid))), index=False, classes=['table', 'table-condensed'])
        )

        for classid in ['6', '3', '1--']:
            report.add_html(
                f'<p>Match text classified of class {classid}</p>' +
                to_html_or_empty_string(create_dataframe_from_sql(text(text_classified_query_template.format(name, objectid=objectid, classid=classid))), index=False, classes=['table', 'table-condensed'])
            )

        report.add_html('<br>')

        print(f'{counter} of {amt} from {name} done')
        counter += 1

    print(name, 'done')

    return df, report, total_report, icd10_cpt_report

factsheets_df, factsheets_report, factsheets_total_report, factsheets_icd10_cpt_report = create_report_for_theme('factsheets')
news_df, news_report, news_total_report, news_icd10_cpt_report = create_report_for_theme('news')

# with open('temp.txt', 'w') as f:
#     f.write(factsheets_total_report.generate_report_html())

print('both done')

factsheets_report.export_report_to_pdf('factsheets_report.pdf')
print('factsheets report done')
upload_blob('factsheets_report.pdf', overwrite=True)
print('upload factsheets done')

# factsheets_icd10_cpt_report.export_report_to_pdf('factsheets_icd10_cpt_report.pdf')
# print('factsheets icd10_cpt report done')
# upload_blob('factsheets_icd10_cpt_report.pdf', overwrite=True)
# print('upload factsheets icd10_cpt report done')

# news_icd10_cpt_report.export_report_to_pdf('news_icd10_cpt_report.pdf')
# print('news icd10_cpt report done')
# upload_blob('news_icd10_cpt_report.pdf', overwrite=True)
# print('upload news icd10_cpt report done')

factsheets_total_report.export_report_to_pdf('total_factsheets_report.pdf')
print('total factsheets report done')
upload_blob('total_factsheets_report.pdf', overwrite=True)
print('upload total factsheets done')

news_report.export_report_to_pdf('news_report.pdf')
print('news report done')
upload_blob('news_report.pdf', overwrite=True)
print('upload news done')

news_total_report.export_report_to_pdf('total_news_report.pdf')
print('total news report done')
upload_blob('total_news_report.pdf', overwrite=True)
print('upload total news done')
