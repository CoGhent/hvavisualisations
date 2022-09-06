from http.client import HTTPResponse
from django.shortcuts import render
from django.http import HttpResponse
from .graphs import records, recordshva, dates, map

# Create your views here.
def graphs(request):
    graphRecords = records.records()
    graphRecordsHvA = recordshva.recordsHvA()
    graphDatesPresent, graphDates = dates.dates()
    graphMap = map.map()
    return render(request, 'graphs.html',{'graphRecords': graphRecords, 'graphRecordsHvA': graphRecordsHvA, 'graphDatesPresent': graphDatesPresent, 'graphDates': graphDates, 'graphMap': graphMap})

