from lodstorage.sparql import SPARQL
from lodstorage.csv import CSV
import math
import ssl
import pandas as pd
from matplotlib import pyplot as plt
import datetime
from numpy import cumsum
from io import BytesIO
import base64

def recordsHvA():
    ###################### NUMBER OF RECORDS ADDED PER MONTH ######################

    # omzeilen certificaten (indien nodig)
    ssl._create_default_https_context = ssl._create_unverified_context

    # HVA - QUERY
    # count number of versions
    sparqlQuery = """
    PREFIX purl: <http://purl.org/dc/terms/>

    SELECT COUNT(?priref)
    WHERE{
    SELECT DISTINCT ?priref
    WHERE {
        SELECT ?versie ?priref FROM <http://stad.gent/ldes/hva>
        WHERE { 
        ?versie purl:isVersionOf ?priref.
        }
    } 
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal_hva = qlod[0]['callret-0']
    offsetrange = aantal_hva / 1000

    # determine number of pages to query
    pages = math.ceil(offsetrange)

    # determine offset range to query
    offsetrange = list(range(0, 1000 * pages, 1000))

    # only one page of results
    if pages < 1:
        ###################################################################################################
        # 1. SPARQL Query
        sparqlQuery = """
        PREFIX purl: <http://purl.org/dc/terms/>

        SELECT ?priref MIN(?versie)
        WHERE {
            SELECT ?versie ?priref FROM <http://stad.gent/ldes/hva>
            WHERE { 

            ?versie purl:isVersionOf ?priref.
            } ORDER BY ASC (?versie)

        } GROUP BY (?priref)"""
        sparql = SPARQL("https://stad.gent/sparql")
        qlod = sparql.queryAsListOfDicts(sparqlQuery)
        csv = CSV.toCSV(qlod)
        df_sparql = pd.DataFrame([x.split(',') for x in csv.split('\n')])

    # multiple pages to query
    else:
        ###################################################################################################
        # 1. SPARQL Query
        querylist = []
        for offset in offsetrange:
            querylist.append("""PREFIX purl: <http://purl.org/dc/terms/>

            SELECT ?priref MIN(?versie)
            WHERE {
                SELECT ?versie ?priref FROM <http://stad.gent/ldes/hva>
                WHERE { 

                ?versie purl:isVersionOf ?priref.
                } ORDER BY ASC (?versie)

            } GROUP BY (?priref) LIMIT 1000 OFFSET """ + str(offset))

        df_sparql = pd.DataFrame()
        for query in querylist:
            sparqlQuery = query
            sparql = SPARQL("https://stad.gent/sparql")
            qlod = sparql.queryAsListOfDicts(sparqlQuery)
            csv = CSV.toCSV(qlod)
            df_result = pd.DataFrame([x.split(',') for x in csv.split('\n')])
            df_sparql = df_sparql.append(df_result, ignore_index=True)

    df_sparql.rename(columns={0: 'priref', 1: 'timestamp'}, inplace=True, errors='raise')

    today = datetime.date.today()
    first = today.replace(day=1)
    interval = first - datetime.timedelta(days=186)
    datarange = pd.date_range(interval, today, freq='MS').strftime("%Y-%m").tolist()

    values = []
    for date in datarange:
        values.append(df_sparql['timestamp'].str.count(date).sum())
    
    values = [round(values) for values in values]
    values_sum = sum(values)
    values[0] = values[0] + (aantal_hva - values_sum)
    values = cumsum(values)
    values = values.tolist()

    plt.plot(datarange, values)
    plt.title("number of records / month")
    plt.xlabel("month")
    plt.ylabel("number")

    bufferRecordsHvA = BytesIO()
    plt.savefig(bufferRecordsHvA, format='png')
    plt.clf()
    bufferRecordsHvA.seek(0)
    imageRecordsHvA_png = bufferRecordsHvA.getvalue()
    bufferRecordsHvA.close()

    graphRecordsHvA = base64.b64encode(imageRecordsHvA_png)
    graphRecordsHvA = graphRecordsHvA.decode('utf-8')

    return(graphRecordsHvA)
