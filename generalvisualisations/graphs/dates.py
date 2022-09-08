import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from lodstorage.sparql import SPARQL
from lodstorage.csv import CSV
import math
import ssl
import pandas as pd
from numpy import cumsum
from io import BytesIO
import base64

def dates():
    # omzeilen certificaten (indien nodig)
    ssl._create_default_https_context = ssl._create_unverified_context

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
        ?draagt cidoc:P67_refers_to ?over.
        ?over cidoc:P2_has_type ?type.
        ?type skos:prefLabel ?label.

    } ORDER BY DESC(?versie)
    }
    }"""

    sparql = SPARQL("https://stad.gent/sparql")
    qlod = sparql.queryAsListOfDicts(sparqlQuery)
    aantal = qlod[0]['callret-0']
    offsetrange = aantal / 1000

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

    # determine number of pages to query
    pages = math.ceil(offsetrange)

    # determine offset range to query
    offsetrange = list(range(0, 1000*pages, 1000))

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
        ?draagt cidoc:P67_refers_to ?over.
        ?over cidoc:P2_has_type ?type.
        ?type skos:prefLabel ?label.

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

    df_sparql[1] = df_sparql[1].str.replace(r'"\r', '')
    df_sparql[1] = df_sparql[1].str.replace(r'"', '')

    periodes = ["jaren 1890", "jaren 1900", "jaren 1910", "jaren 1920", "jaren 1930", "jaren 1940", "jaren 1950",
                "jaren 1960", "jaren 1970", "jaren 1980", "jaren 1990", "jaren  2000", "jaren 2010"]

    aantallen = []
    for periode in periodes:
        aantal = df_sparql[1].str.contains(periode).sum()
        aantallen.append(aantal)
    
    aantal_dateringen = sum(aantallen)
    datering_afwezig = aantal_hva - aantal_dateringen
    datering_aanwezig = [aantal_dateringen, datering_afwezig]
    dateringen = ['Datering aanwezig', 'Datering afwezig']

    plt.style.use("seaborn-pastel")
    exploded = [0, 0.1]
    plt.pie(datering_aanwezig, labels=dateringen, explode=exploded, startangle=230)
    plt.tight_layout()
    
    bufferDatespresent = BytesIO()
    plt.savefig(bufferDatespresent, format='png')
    bufferDatespresent.seek(0)
    imageDatespresent_png = bufferDatespresent.getvalue()
    bufferDatespresent.close()

    graphDatesPresent = base64.b64encode(imageDatespresent_png)
    graphDatesPresent = graphDatesPresent.decode('utf-8')
    plt.close()

    plt.style.use("seaborn-pastel")
    plt.bar(periodes, aantallen, width=0.4)
    plt.xticks(rotation=90)
    plt.tight_layout()

    bufferDates = BytesIO()
    plt.savefig(bufferDates, format='png')
    bufferDates.seek(0)
    imageDates_png = bufferDates.getvalue()
    bufferDates.close()

    graphDates = base64.b64encode(imageDates_png)
    graphDates = graphDates.decode('utf-8')
    plt.close()

    return(graphDatesPresent, graphDates)