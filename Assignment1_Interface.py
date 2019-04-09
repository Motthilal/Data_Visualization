#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
	query = "CREATE TABLE " + ratingstablename + "(	userid INTEGER,r1 CHAR,movieid INTEGER,r2 CHAR,rating REAL,r3 CHAR,time_stamp BIGINT )"
	cur = openconnection.cursor()
	cur.execute(query)
	cur.copy_from(open(ratingsfilepath,'r'),ratingstablename,sep=':')
	query = "ALTER TABLE " + ratingstablename+ " DROP COLUMN r1,DROP COLUMN r2, DROP COLUMN r3, DROP COLUMN   time_stamp"
	cur.execute(query)
	cur.close()
	openconnection.commit()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
	s_val=0
	i_val=5.0/numberofpartitions
	txt="range_part"
	txt_list=[txt]*numberofpartitions
	new_txt_list=[txt_list[x]+str(x) for x in range(0,numberofpartitions)]
	for i in range(0,numberofpartitions):
		if i==0:
			query= "CREATE TABLE "+new_txt_list[i]+" AS SELECT * FROM " + ratingstablename + " WHERE RATING >= " + str(s_val) + """ AND RATING <= """ + str(s_val+i_val)
			cur = openconnection.cursor()
			cur.execute(query)
			openconnection.commit( )
		else:
			query= "CREATE TABLE "+new_txt_list[i]+" AS SELECT * FROM " + ratingstablename + " WHERE RATING > " + str(s_val) + """ AND RATING <= """ + str(s_val+i_val)
			cur = openconnection.cursor()
			cur.execute(query)
			openconnection.commit()
		s_val+=i_val




def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
	txt="rrobin_part"
	txt_list=[txt]*numberofpartitions
	new_txt_list=[txt_list[x]+str(x) for x in range(0,numberofpartitions)]
	for i in range(0,numberofpartitions):
		query = "CREATE TABLE " +new_txt_list[i] + " AS WITH T AS (SELECT ROW_NUMBER() OVER() as R,* FROM " + ratingstablename +")SELECT userid,movieid,rating FROM T WHERE (R-"+str(i+1) +")%" + str(numberofpartitions)+ " = 0"
		cur = openconnection.cursor()
		cur.execute(query)
		openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
	txt="rrobin_part"
	#Determine rrobin_part tbl count
	query = " SELECT COUNT(*) FROM pg_stat_user_tables WHERE RELNAME LIKE '" + txt + "%'"
	cur = openconnection.cursor()
	cur.execute(query)
	count = int(cur.fetchone()[0])
	#return count
	mark=False
	val=-1
	for i in range(0,count):
		new_txt_list=txt+str(i)
		cur = openconnection.cursor()
		query = "SELECT COUNT(*) FROM " + new_txt_list
		cur.execute(query)
		temp_count = int(cur.fetchone()[0])
		#return temp_count
		if val==-1:
			val=temp_count
			continue
		if val>temp_count:
			mark=True
			query = " INSERT INTO " + new_txt_list + " VALUES(" + str(userid)+ " , " + str(itemid) + " , " + str(rating) + ")"
			cur = openconnection.cursor()
			cur.execute(query)
			openconnection.commit()
			break
	
	if not mark:
		query = " INSERT INTO " + (txt+str(0)) + " VALUES(" + str(userid)+ " , " + str(itemid) + " , " + str(rating) + ")"
		cur = openconnection.cursor()
		cur.execute(query)
		openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	txt="range_part"
	#Determine rrobin_part tbl count
	query = " SELECT COUNT(*) FROM pg_stat_user_tables WHERE RELNAME LIKE '" + txt + "%'"
	cur = openconnection.cursor()
 	cur.execute(query)
	count = int(cur.fetchone()[0])
	#return count
	
	s_val=0
	i_val=float(5)/count
	for i in range(0,count):
		new_txt_list=txt+str(i)
		if((s_val==0 and rating>=s_val and rating <=s_val+i_val) or (rating>s_val and rating <=s_val+i_val)):
			query = " INSERT INTO " + new_txt_list + " VALUES(" + str(userid)+ " , " + str(itemid) + " , " + str(rating) + ")"
			cur = openconnection.cursor()
			cur.execute(query)
			openconnection.commit()
			break
		s_val+=i_val

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()
