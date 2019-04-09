#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    InputTable = InputTable.lower()
    OutputTable = OutputTable.lower()
    ptn = 5
    try:
        cur = openconnection.cursor()
        cur.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
        minval = cur.fetchone()[0]
        cur.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
        maxval = cur.fetchone()[0]
        diff = (maxval - minval) / 5.0

        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "'")
        schemata = cur.fetchall()

        for i in range(5):
            otabname = "temp_otab" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + otabname + "")
            cur.execute("CREATE TABLE " + otabname + " (" + schemata[0][0] + " " + schemata[0][1] + ")")

            for j in range(1, len(schemata)):
                cur.execute("ALTER TABLE " + otabname + " ADD COLUMN " + schemata[j][0] + " " + schemata[j][1] + ";")

        # create threads
        thread = [0] * 5
        for i in range(ptn):
            if i == 0:
                lval = minval
                uval = minval + diff
            else:
                lval = uval
                uval = uval + diff

            thread[i] = threading.Thread(target=p_sort_range,
                                         args=(InputTable, SortingColumnName, i, lval, uval, openconnection))
            thread[i].start()

        for j in range(0, ptn):
            thread[i].join()

        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + schemata[0][0] + " " + schemata[0][1] + ")")

        for i in range(1, len(schemata)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schemata[i][0] + " " + schemata[i][1] + ";")

        for i in range(ptn):
            query = "INSERT INTO " + OutputTable + " SELECT * FROM " + "temp_otab" + str(i) + ""
            cur.execute(query)

        for i in range(5):
            query = "DROP TABLE IF EXISTS temp_otab" + str(i) + ";"
            cur.execute(query)


    except Exception as warn:
        print "EXCEPTION :", warn


def p_sort_range(InputTable, SortingColumnName, i, lval, uval, openconnection):
    cur = openconnection.cursor()
    tabname = "temp_otab" + str(i)
    if i == 0:
        query = "insert into " + tabname + " select * from " + InputTable + "  where " + SortingColumnName + ">=" + str(
            lval) + " and " + SortingColumnName + " <= " + str(uval) + " order by " + SortingColumnName
    else:
        query = "insert into " + tabname + " select * from " + InputTable + "  where " + SortingColumnName + ">" + str(
            lval) + " and " + SortingColumnName + " <= " + str(uval) + " order by " + SortingColumnName
    cur.execute(query)
    cur.close()
    return

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    InputTable1 = InputTable1.lower()
    InputTable2 = InputTable2.lower()
    Table1JoinColumn = Table1JoinColumn.lower()
    Table2JoinColumn = Table2JoinColumn.lower()
    OutputTable = OutputTable.lower()
    ptn = 5
    cur = openconnection.cursor()
    temp = "SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    mintab1val = float(cur.fetchone()[0])

    temp = "SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    mintab2val = float(cur.fetchone()[0])

    temp = "SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    maxtab1val = float(cur.fetchone()[0])

    temp = "SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    maxtab2val = float(cur.fetchone()[0])

    maxtabval = max(maxtab1val, maxtab2val)
    mintabval = min(mintab1val, mintab2val)
    diff = (maxtabval - mintabval) / ptn

    query = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable1 + "\';"
    cur.execute(query)
    Itab1schemata = cur.fetchall()

    query = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';"
    cur.execute(query)
    Itab2schemata = cur.fetchall()

    cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
    query = "CREATE TABLE " + OutputTable + " (" + Itab1schemata[0][0] + " INTEGER);"
    # print query
    cur.execute(query)

    for i in range(ptn):
        query = "DROP TABLE IF EXISTS inp_temp_tab1" + str(i) + ";"
        cur.execute(query)
        query = "DROP TABLE IF EXISTS inp_temp_tab2" + str(i) + ";"
        cur.execute(query)
        query = "DROP TABLE IF EXISTS out_temp_tab" + str(i) + ";"
        cur.execute(query)


    for i in range(1, len(Itab1schemata)):
        query = "ALTER TABLE " + OutputTable + " ADD COLUMN " + Itab1schemata[i][0] + " " + Itab1schemata[i][1] + ";"
        # print query
        cur.execute(query)

    for i in range(len(Itab2schemata)):
        query = "ALTER TABLE " + OutputTable + " ADD COLUMN " + Itab2schemata[i][0] + " " + Itab2schemata[i][1] + ";"
        # print query
        cur.execute(query)

    for i in range(ptn):
        tabname = "inp_temp_tab1" + str(i)
        if i == 0:
            lval = mintab1val
            uval = lval + diff
            query = "CREATE TABLE " + tabname + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(
                lval) + " AND " + Table1JoinColumn + " <= " + str(uval) + ";"
        else:
            lval = uval
            uval = lval + diff
            query = "CREATE TABLE " + tabname + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(
                lval) + " AND " + Table1JoinColumn + " <= " + str(uval) + ";"
        cur.execute(query)

    for i in range(ptn):
        tabname = "inp_temp_tab2" + str(i)
        if i == 0:
            lval = mintab1val
            uval = lval + diff
            query = "CREATE TABLE " + tabname + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(
                lval) + " AND " + Table2JoinColumn + " <= " + str(uval) + ";"
        else:
            lval = uval
            uval = lval + diff
            query = "CREATE TABLE " + tabname + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(
                lval) + " AND " + Table2JoinColumn + " <= " + str(uval) + ";"
        #print query
        cur.execute(query)

    for i in range(ptn):
        otabname = "out_temp_tab" + str(i)
        query = "CREATE TABLE " + otabname + " (" + Itab1schemata[0][0] + " INTEGER);"
        #print query
        cur.execute(query)

        for i in range(1, len(Itab1schemata)):
            query = "ALTER TABLE " + otabname + " ADD COLUMN " + Itab1schemata[i][0] + " " + Itab1schemata[i][1] + ";"
            #print query
            cur.execute(query)

        for i in range(len(Itab2schemata)):
            query = "ALTER TABLE " + otabname + " ADD COLUMN " + Itab2schemata[i][0] + " " + Itab2schemata[i][1] + ";"
            #print query
            cur.execute(query)

    threads = [0] * ptn
    for i in range(ptn):
        threads[i] = threading.Thread(target=parallel_join,
                                      args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
        threads[i].start()

    for i in range(ptn):
        threads[i].join()

    for i in range(ptn):
        query = "INSERT INTO " + OutputTable + " SELECT * FROM out_temp_tab" + str(i) + ";"
        cur.execute(query)

    for i in range(ptn):
        query = "DROP TABLE IF EXISTS inp_temp_tab1" + str(i) + ";"
        cur.execute(query)
        query = "DROP TABLE IF EXISTS inp_temp_tab2" + str(i) + ";"
        cur.execute(query)
        query = "DROP TABLE IF EXISTS out_temp_tab" + str(i) + ";"
        cur.execute(query)

    openconnection.commit()



def parallel_join(Table1JoinColumn, Table2JoinColumn, openconnection, i):
    cur = openconnection.cursor()
    query = """INSERT INTO out_temp_tab""" + str(i) + """ SELECT * FROM inp_temp_tab1""" + str(
        i) + """ INNER JOIN inp_temp_tab2""" + str(i) + """ ON inp_temp_tab1""" + str(i) + """.""" + str(
        Table1JoinColumn) + """ = inp_temp_tab2""" + str(i) + """.""" + str(Table2JoinColumn) + """;"""
    cur.execute(query)
    return


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
