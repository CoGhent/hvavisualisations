import pandas as pd
from lodstorage.sparql import SPARQL
from lodstorage.csv import CSV
import math
import ssl
import plotly.express as px

def map():
    ssl._create_default_https_context = ssl._create_unverified_context

    # count number of streets
    sparqlQuery = """
    PREFIX purl: <http://purl.org/dc/terms/>
    PREFIX cidoc: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT COUNT(?priref)
    WHERE{
    SELECT DISTINCT ?priref ?label
    WHERE {
    SELECT ?versie ?priref ?label FROM <http://stad.gent/ldes/hva>
    WHERE { 

        ?versie purl:isVersionOf ?priref.

        ?versie cidoc:P128_carries ?draagt.
        ?draagt cidoc:P129_is_about ?over.
        ?over cidoc:P2_has_type ?type.
        ?type skos:prefLabel ?label.

        FILTER (regex(?label, "(Gent)", "i"))

    } ORDER BY DESC(?versie)
    }
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal = qlod[0]['callret-0']
    offsetrange = aantal / 1000

    # determine number of pages to query
    pages = math.ceil(offsetrange)

    # determine offset range to query
    offsetrange = list(range(0, 1000 * pages, 1000))

    # only one page of results
    if pages < 1:
        ###################################################################################################
        # 1. SPARQL Query
        sparqlQuery = """PREFIX purl: <http://purl.org/dc/terms/>
        PREFIX cidoc: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT DISTINCT ?priref ?label
        WHERE {
        SELECT ?versie ?priref ?label FROM <http://stad.gent/ldes/hva>
        WHERE { 

            ?versie purl:isVersionOf ?priref.

            ?versie cidoc:P128_carries ?draagt.
            ?draagt cidoc:P129_is_about ?over.
            ?over cidoc:P2_has_type ?type.
            ?type skos:prefLabel ?label.

            FILTER (regex(?label, "(Gent)", "i"))

        } ORDER BY DESC(?versie)
        } LIMIT 1000"""
        sparql = SPARQL("https://stad.gent/sparql")
        qlod = sparql.queryAsListOfDicts(sparqlQuery)
        csv = CSV.toCSV(qlod)
        df_sparql = pd.DataFrame([x.split(',') for x in csv.split('\n')])

    # loop for obtaining multiple pages of results

    else:
        ###################################################################################################
        # 1. SPARQL Query
        querylist = []
        for offset in offsetrange:
            querylist.append("""PREFIX purl: <http://purl.org/dc/terms/>
        PREFIX cidoc: <http://www.cidoc-crm.org/cidoc-crm/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT DISTINCT ?priref ?label
        WHERE {
        SELECT ?versie ?priref ?label FROM <http://stad.gent/ldes/hva>
        WHERE { 

            ?versie purl:isVersionOf ?priref.

            ?versie cidoc:P128_carries ?draagt.
            ?draagt cidoc:P129_is_about ?over.
            ?over cidoc:P2_has_type ?type.
            ?type skos:prefLabel ?label.

            FILTER (regex(?label, "(Gent)", "i"))

        } ORDER BY DESC(?versie)
        } LIMIT 1000 OFFSET """ + str(offset))

        df_sparql = pd.DataFrame()
        for query in querylist:
            sparqlQuery = query
            sparql = SPARQL("https://stad.gent/sparql")
            qlod = sparql.queryAsListOfDicts(sparqlQuery)
            csv = CSV.toCSV(qlod)
            df_result = pd.DataFrame([x.split(',') for x in csv.split('\n')])
            df_sparql = df_sparql.append(df_result, ignore_index=True)

    # databank straten en coordinaten
    df_straten = pd.read_excel(r"C:\Users\Verkesfl\OneDrive - Groep Gent\Documenten\Documenten\COGHENT\code\hvavisualisations\generalvisualisations\graphs\stratencoordinaten.xlsx")

    # tel aantal keer een straat voorkomt in sparql resultaat
    aantal = df_sparql[1].value_counts()

    # straten + aantallen naar dataframe
    df_aantal = aantal.to_frame()
    df_aantal.index.name = 'straat'
    df_aantal.reset_index(inplace=True)
    df_aantal['straat'] = df_aantal['straat'].str.replace(r'"\r', '')
    df_aantal['straat'] = df_aantal['straat'].str.replace(r'"', '')

    # samenvoegen straten+aantallen met databank straten en coordinaten
    df_straten = pd.merge(df_straten, df_aantal, on='straat', how='outer')

    # lege velden laten vallen
    df_straten['Latitude'].fillna(0, inplace=True)
    df_straten[1].fillna(0, inplace=True)
    df_straten = df_straten[df_straten['Latitude'] != 0]
    df_straten = df_straten[df_straten[1] != 0]
    df_straten.rename(columns={1: 'aantal'}, inplace=True, errors='raise')
    df_straten['size'] = 3

    fig = px.scatter_mapbox(data_frame=df_straten, lat=df_straten["Longitude"], lon=df_straten["Latitude"], hover_data=["straat", 'aantal'], color_discrete_sequence=["yellow"], size='size', size_max=10, zoom=12, height=500)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.data[0].update(hovertemplate= 'straat=%{customdata[0]}<br>aantal=%{customdata[1]}<extra></extra>')
    graphMap = fig.to_html(full_html=False, default_height=650, default_width=1300)
    return(graphMap)