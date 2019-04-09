#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    # Variable Declaration
    ranrattab = "RangeRatingsPart"
    rourobtab = "RoundRobinRatingsPart"
    fileout = outputPath
    lines = ""

    # Get partition number from metadatarangerating table
    cur = openconnection.cursor()
    query = "SELECT COUNT(*) FROM RANGERATINGSMETADATA;"
    cur.execute(query)
    partnum = int(cur.fetchone()[0])

    # get records from metadatarangerating table for rangepartition tables
    cur = openconnection.cursor()
    query = "SELECT * FROM RANGERATINGSMETADATA;"
    cur.execute(query)
    metarecords = cur.fetchall()

    # Generate tablenames to query
    querytables = []
    for i in metarecords:
        if i[2] < ratingMinValue or i[1] > ratingMaxValue:
            continue
        else:
            querytables.append(ranrattab + str(i[0]))


    for table in querytables:
        cur = openconnection.cursor()
        query = "SELECT * FROM " + table + " WHERE RATING <= " + str(ratingMaxValue) + " AND RATING >=" + str(
            ratingMinValue) + ";"
        cur.execute(query)
        recordsfromeachtable = cur.fetchall()
        for eachrecord in recordsfromeachtable:
            lines += (table + "," + str(eachrecord[0]) + "," + str(eachrecord[1]) + "," + str(eachrecord[2]) + "\n")


    # Extraction of records from RR tables
    cur = openconnection.cursor()
    query = "SELECT PARTITIONNUM FROM roundrobinratingsmetadata;"
    cur.execute(query)
    rrpartnum = int(cur.fetchone()[0])

    # Query data from each table and write to file
    recordsfromeachtable = []
    for i in range(rrpartnum):
        cur = openconnection.cursor()
        query = "SELECT * FROM " + rourobtab + str(i) + " WHERE RATING <= " + str(
            ratingMaxValue) + " AND RATING >=" + str(ratingMinValue) + ";"
        cur.execute(query)
        recordsfromeachtable = cur.fetchall()
        for eachrecord in recordsfromeachtable:
            lines+=(rourobtab + str(i) + "," + str(eachrecord[0]) + "," + str(eachrecord[1]) + "," + str(eachrecord[2]) + "\n")
        with open(outputPath, "w") as f:
            f.write(lines)


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    lines=""
    ranrattab = "RangeRatingsPart"
    rourobtab = "RoundRobinRatingsPart"
    fileout = outputPath

    # Get partition number from metadatarangerating table
    cur = openconnection.cursor()
    query = "SELECT COUNT(*) FROM RANGERATINGSMETADATA;"
    cur.execute(query)
    partnum = int(cur.fetchone()[0])

    # get records from metadatarangerating table for rangepartition tables
    cur = openconnection.cursor()
    query = "SELECT * FROM RANGERATINGSMETADATA;"
    cur.execute(query)
    metarecords = cur.fetchall()

    # Generate tablenames to query
    querytables = []
    count = 0
    for i in metarecords:
        count += 1
        if (i[1] < ratingValue and i[2] >= ratingValue) or (count == 1 and ratingValue == i[1]):
            querytables.append(ranrattab + str(i[0]))

    # Select data from Range tables
    for table in querytables:
        cur = openconnection.cursor()
        query = "SELECT * FROM " + table + " WHERE RATING = " + str(ratingValue) + ";"
        cur.execute(query)
        recordsfromeachtable = cur.fetchall()
        for eachrecord in recordsfromeachtable:
            lines+=(table + "," + str(eachrecord[0]) + "," + str(eachrecord[1]) + "," + str(eachrecord[2]) + "\n")

    # Select data from RR tables
    recordsfromeachtable = []
    for i in range(partnum):
        cur = openconnection.cursor()
        query = "SELECT * FROM " + rourobtab + str(i) + " WHERE RATING = " + str(ratingValue) + ";"
        cur.execute(query)
        recordsfromeachtable = cur.fetchall()
        for eachrecord in recordsfromeachtable:
            lines+=(rourobtab + str(i) + "," + str(eachrecord[0]) + "," + str(eachrecord[1]) + "," + str(eachrecord[2]) + "\n")
        with open(outputPath, "w") as f:
            f.write(lines)