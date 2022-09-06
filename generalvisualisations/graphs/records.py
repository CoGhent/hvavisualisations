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

def records():
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

    # DMG - QUERY
    # count number of versions
    sparqlQuery = """
    PREFIX purl: <http://purl.org/dc/terms/>

    SELECT COUNT(?priref)
    WHERE{
    SELECT DISTINCT ?priref
    WHERE {
        SELECT ?versie ?priref FROM <http://stad.gent/ldes/dmg>
        WHERE { 
        ?versie purl:isVersionOf ?priref.
        }
    } 
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal_dmg = qlod[0]['callret-0']
    offsetrange = aantal_dmg / 1000

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
            SELECT ?versie ?priref FROM <http://stad.gent/ldes/dmg>
            WHERE { 

            ?versie purl:isVersionOf ?priref.
            } ORDER BY ASC (?versie)

        } GROUP BY (?priref)"""
        sparql = SPARQL("https://stad.gent/sparql")
        qlod = sparql.queryAsListOfDicts(sparqlQuery)
        csv = CSV.toCSV(qlod)
        df_sparql_dmg = pd.DataFrame([x.split(',') for x in csv.split('\n')])

    # multiple pages to query
    else:
        ###################################################################################################
        # 1. SPARQL Query
        querylist = []
        for offset in offsetrange:
            querylist.append("""PREFIX purl: <http://purl.org/dc/terms/>

            SELECT ?priref MIN(?versie)
            WHERE {
                SELECT ?versie ?priref FROM <http://stad.gent/ldes/dmg>
                WHERE { 

                ?versie purl:isVersionOf ?priref.
                } ORDER BY ASC (?versie)

            } GROUP BY (?priref) LIMIT 1000 OFFSET """ + str(offset))

        df_sparql_dmg = pd.DataFrame()
        for query in querylist:
            sparqlQuery = query
            sparql = SPARQL("https://stad.gent/sparql")
            qlod = sparql.queryAsListOfDicts(sparqlQuery)
            csv = CSV.toCSV(qlod)
            df_result = pd.DataFrame([x.split(',') for x in csv.split('\n')])
            df_sparql_dmg = df_sparql_dmg.append(df_result, ignore_index=True)

    # STAM - QUERY
    # count number of versions
    sparqlQuery = """
    PREFIX purl: <http://purl.org/dc/terms/>

    SELECT COUNT(?priref)
    WHERE{
    SELECT DISTINCT ?priref
    WHERE {
        SELECT ?versie ?priref FROM <http://stad.gent/ldes/stam>
        WHERE { 
        ?versie purl:isVersionOf ?priref.
        }
    } 
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal_stam = qlod[0]['callret-0']
    offsetrange = aantal_stam / 1000

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
            SELECT ?versie ?priref FROM <http://stad.gent/ldes/stam>
            WHERE { 

            ?versie purl:isVersionOf ?priref.
            } ORDER BY ASC (?versie)

        } GROUP BY (?priref)"""
        sparql = SPARQL("https://stad.gent/sparql")
        qlod = sparql.queryAsListOfDicts(sparqlQuery)
        csv = CSV.toCSV(qlod)
        df_sparql_stam = pd.DataFrame([x.split(',') for x in csv.split('\n')])

    # multiple pages to query
    else:
        ###################################################################################################
        # 1. SPARQL Query
        querylist = []
        for offset in offsetrange:
            querylist.append("""PREFIX purl: <http://purl.org/dc/terms/>

            SELECT ?priref MIN(?versie)
            WHERE {
                SELECT ?versie ?priref FROM <http://stad.gent/ldes/stam>
                WHERE { 

                ?versie purl:isVersionOf ?priref.
                } ORDER BY ASC (?versie)

            } GROUP BY (?priref) LIMIT 1000 OFFSET """ + str(offset))

        df_sparql_stam = pd.DataFrame()
        for query in querylist:
            sparqlQuery = query
            sparql = SPARQL("https://stad.gent/sparql")
            qlod = sparql.queryAsListOfDicts(sparqlQuery)
            csv = CSV.toCSV(qlod)
            df_result = pd.DataFrame([x.split(',') for x in csv.split('\n')])
            df_sparql_stam = df_sparql_stam.append(df_result, ignore_index=True)

    # IM - QUERY
    # count number of versions
    sparqlQuery = """
    PREFIX purl: <http://purl.org/dc/terms/>

    SELECT COUNT(?priref)
    WHERE{
    SELECT DISTINCT ?priref
    WHERE {
        SELECT ?versie ?priref FROM <http://stad.gent/ldes/industriemuseum>
        WHERE { 
        ?versie purl:isVersionOf ?priref.
        }
    } 
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal_im = qlod[0]['callret-0']
    offsetrange = aantal_im / 1000

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
            SELECT ?versie ?priref FROM <http://stad.gent/ldes/industriemuseum>
            WHERE { 

            ?versie purl:isVersionOf ?priref.
            } ORDER BY ASC (?versie)

        } GROUP BY (?priref)"""
        sparql = SPARQL("https://stad.gent/sparql")
        qlod = sparql.queryAsListOfDicts(sparqlQuery)
        csv = CSV.toCSV(qlod)
        df_sparql_im = pd.DataFrame([x.split(',') for x in csv.split('\n')])

    # multiple pages to query
    else:
        ###################################################################################################
        # 1. SPARQL Query
        querylist = []
        for offset in offsetrange:
            querylist.append("""PREFIX purl: <http://purl.org/dc/terms/>

            SELECT ?priref MIN(?versie)
            WHERE {
                SELECT ?versie ?priref FROM <http://stad.gent/ldes/industriemuseum>
                WHERE { 

                ?versie purl:isVersionOf ?priref.
                } ORDER BY ASC (?versie)

            } GROUP BY (?priref) LIMIT 1000 OFFSET """ + str(offset))

        df_sparql_im = pd.DataFrame()
        for query in querylist:
            sparqlQuery = query
            sparql = SPARQL("https://stad.gent/sparql")
            qlod = sparql.queryAsListOfDicts(sparqlQuery)
            csv = CSV.toCSV(qlod)
            df_result = pd.DataFrame([x.split(',') for x in csv.split('\n')])
            df_sparql_im = df_sparql_im.append(df_result, ignore_index=True)

    # AG - QUERY
    # count number of versions
    sparqlQuery = """
    PREFIX purl: <http://purl.org/dc/terms/>

    SELECT COUNT(?priref)
    WHERE{
    SELECT DISTINCT ?priref
    WHERE {
        SELECT ?versie ?priref FROM <http://stad.gent/ldes/archief>
        WHERE { 
        ?versie purl:isVersionOf ?priref.
        }
    } 
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal_ag = qlod[0]['callret-0']
    offsetrange = aantal_ag / 1000

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
            SELECT ?versie ?priref FROM <http://stad.gent/ldes/archief>
            WHERE { 

            ?versie purl:isVersionOf ?priref.
            } ORDER BY ASC (?versie)

        } GROUP BY (?priref)"""
        sparql = SPARQL("https://stad.gent/sparql")
        qlod = sparql.queryAsListOfDicts(sparqlQuery)
        csv = CSV.toCSV(qlod)
        df_sparql_ag = pd.DataFrame([x.split(',') for x in csv.split('\n')])

    # multiple pages to query
    else:
        ###################################################################################################
        # 1. SPARQL Query
        querylist = []
        for offset in offsetrange:
            querylist.append("""PREFIX purl: <http://purl.org/dc/terms/>

            SELECT ?priref MIN(?versie)
            WHERE {
                SELECT ?versie ?priref FROM <http://stad.gent/ldes/archief>
                WHERE { 

                ?versie purl:isVersionOf ?priref.
                } ORDER BY ASC (?versie)

            } GROUP BY (?priref) LIMIT 1000 OFFSET """ + str(offset))

        df_sparql_ag = pd.DataFrame()
        for query in querylist:
            sparqlQuery = query
            sparql = SPARQL("https://stad.gent/sparql")
            qlod = sparql.queryAsListOfDicts(sparqlQuery)
            csv = CSV.toCSV(qlod)
            df_result = pd.DataFrame([x.split(',') for x in csv.split('\n')])
            df_sparql_ag = df_sparql_ag.append(df_result, ignore_index=True)

    df_sparql.rename(columns={0: 'priref', 1: 'timestamp'}, inplace=True, errors='raise')
    df_sparql_dmg.rename(columns={0: 'priref', 1: 'timestamp'}, inplace=True, errors='raise')
    df_sparql_stam.rename(columns={0: 'priref', 1: 'timestamp'}, inplace=True, errors='raise')
    df_sparql_im.rename(columns={0: 'priref', 1: 'timestamp'}, inplace=True, errors='raise')
    df_sparql_ag.rename(columns={0: 'priref', 1: 'timestamp'}, inplace=True, errors='raise')

    today = datetime.date.today()
    first = today.replace(day=1)
    interval = first - datetime.timedelta(days=186)
    datarange = pd.date_range(interval, today, freq='MS').strftime("%Y-%m").tolist()

    values = []
    for date in datarange:
        values.append(df_sparql['timestamp'].str.count(date).sum())

    values_dmg = []
    for date in datarange:
        values_dmg.append(df_sparql_dmg['timestamp'].str.count(date).sum())

    values_stam = []
    for date in datarange:
        values_stam.append(df_sparql_stam['timestamp'].str.count(date).sum())

    values_im = []
    for date in datarange:
        values_im.append(df_sparql_im['timestamp'].str.count(date).sum())

    values_ag = []
    for date in datarange:
        values_ag.append(df_sparql_ag['timestamp'].str.count(date).sum())

    values = [round(values) for values in values]
    values_sum = sum(values)
    values[0] = values[0] + (aantal_hva - values_sum)
    values = cumsum(values)
    values = values.tolist()

    values_dmg = [round(values_dmg) for values_dmg in values_dmg]
    values_sum_dmg = sum(values_dmg)
    values_dmg[0] = values_dmg[0] + (aantal_dmg - values_sum_dmg)
    values_dmg = cumsum(values_dmg)
    values_dmg = values_dmg.tolist()

    values_im = [round(values_im) for values_im in values_im]
    values_sum_im = sum(values_im)
    values_im[0] = values_im[0] + (aantal_im - values_sum_im)
    values_im = cumsum(values_im)
    values_im = values_im.tolist()

    values_stam = [round(values_stam) for values_stam in values_stam]
    values_sum_stam = sum(values_stam)
    values_stam[0] = values_stam[0] + (aantal_stam - values_sum_stam)
    values_stam = cumsum(values_stam)
    values_stam = values_stam.tolist()

    values_ag = [round(values_ag) for values_ag in values_ag]
    values_sum_ag = sum(values_ag)
    values_ag[0] = values_ag[0] + (aantal_ag - values_sum_ag)
    values_ag = cumsum(values_ag)
    values_ag = values_ag.tolist()

    plt.plot(datarange, values, label='HvA')
    plt.plot(datarange, values_im, label='IM')
    plt.plot(datarange, values_dmg, label='DMG')
    plt.plot(datarange, values_ag, label='AG')
    plt.plot(datarange, values_stam, label='STAM')
    plt.title("number of records / institution / month")
    plt.legend()
    plt.xlabel("month")
    plt.ylabel("number")

    bufferRecords = BytesIO()
    plt.savefig(bufferRecords, format='png')
    plt.clf()
    bufferRecords.seek(0)
    imageRecords_png = bufferRecords.getvalue()
    bufferRecords.close()

    graphRecords = base64.b64encode(imageRecords_png)
    graphRecords = graphRecords.decode('utf-8')

    return(graphRecords)
