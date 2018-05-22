# from __future__ import print_function
import csv
import multiprocessing
import sys
import traceback
from itertools import izip_longest
import pymysql
import pandas as pd
import urllib
import json
import time


# connect to the rds db
def db_connection():
    db_con = pymysql.Connect(host='data-engineer-rds.czmkgxqloose.us-east-1.rds.amazonaws.com',
                             port=3306,
                             user='cogo_read_only',
                             passwd='',
                             db='liveworks')
    return db_con


# find intersection between two datasets
def find_match(emd5, cogo_df, db):

    query = """
            SELECT emd5, job, company FROM cogo_list_v1 WHERE emd5 IN (%s);
    """

    results_lw = []

    with db.cursor() as cur:
        for split_value in grouper(emd5, 1000):

            q = query % ', '.join(['%s'] * len(split_value))
            cur.execute(q, split_value)

            for row in cur:
                results_lw.append(row)

    df_lw = pd.DataFrame(results_lw, columns=['emd5', 'job', 'company'])
    df_intersection = pd.merge(df_lw, cogo_df[['emd5', 'job', 'company']], on='emd5', how='left', suffixes=('_lw', '_cogo'))

    return df_intersection


# using multiprocessing to find intersections between two datasets
def find_match_worker(pages):
    results = pd.DataFrame()
    len_cogo = 0

    db_con = db_connection()
    for page in pages:
        # match_time = time.time()
        url = baseurl + "?page=%s" % page
        document = urllib.urlopen(url)

        if document.getcode() == 200:
            data = json.loads(document.read())  # [u'num_rows', u'rows', u'num_pages', u'page']

            if len(data['rows']) == 0:
                continue
            df = pd.DataFrame(data['rows'])
            len_cogo += len(df)

            # if not df[df.duplicated(['emd5'], keep=False)].empty:
            #     print "Duplicated in page %s is %s\n" % (page,  df[df.duplicated(['emd5'], keep=False)])

            # find intersection
            matches = find_match(df['emd5'], df, db_con)

            results = results.append(matches)
            # print "match time for one page is : ", match_time
    return results


# get total number of records in a table
def get_num_rows(table_name):
    db_con = db_connection()
    with db_con.cursor() as cur:
        cur.execute("SELECT COUNT(*) from %s;" % table_name)
        (total_rows,) = cur.fetchone()
    return total_rows


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values in izip_longest(fillvalue=fillvalue, *args))


# combine two columns into dict
def create_dict(val1, val2):
    return {"job": val1, 'company': val2}


# chunk a list into pieces for multiprocessing
def chunks(l, n):
    res = []
    for i in xrange(0, len(l), n):
        res.append(l[i:i + n])
    return res


def print_csv(csv_file, ranges):
    f = open(csv_file, 'r+')
    csvReader = csv.reader(f)
    for i in range(ranges):
        row = csvReader.next()
        if i == 0:
            print ' ' * 15 + row[0] + ' ' * 40 + row[1] + ' ' * 60 + row[2]
            continue
        print row[0] + ', ' + row[1] + ', ' + ' ' * (110 - int(len(row[0]) + int(len(row[1])))) + row[2]
    #
    # for itm in csvReader.next():
    #     print '\n'.join(['\t'.join([str(itm)])])


if __name__ == '__main__':

    # get dataset from API
    proc_time = time.time()
    baseurl = "http://EC2Co-EcsEl-MT18UEPNPS93-1308701016.us-east-1.elb.amazonaws.com:8000/records"
    # check how many pages and how many records in total for the dataset stored in the API
    url = baseurl + "?page=-1"
    info = json.loads(urllib.urlopen(url).read())
    num_pages = info['num_pages']
    total_len_cogo = info['num_rows']

    intersections = pd.DataFrame()
    total_len_inter = 0
    output_csv_name = 'results.csv'
    # nprocesses = 4

    # multiprocessing
    try:
        nprocesses = multiprocessing.cpu_count() - 1  # nprocesses or

    except NotImplementedError:
        nprocesses = 1
    else:
        nprocesses = 1 if nprocesses <= 0 else nprocesses

    pool = multiprocessing.Pool(nprocesses)

    pages_list = chunks(range(1, num_pages + 1), num_pages / nprocesses + 1)

    iterator = pool.imap_unordered(find_match_worker, pages_list)

    while True:
        try:
            matches = iterator.next()
        except multiprocessing.TimeoutError as e:
            print str(e)
        except StopIteration:
            break
        except KeyboardInterrupt as e:
            pool.terminate()
            pool.close()
            pool.join()
        except Exception as e:
            print "Failed finding matches"
            print str(e)
            traceback.print_exc(file=sys.stdout)
        else:
            intersections = intersections.append(matches)

    intersections['Liveworks'] = intersections.apply(lambda row: create_dict(row['job_lw'], row['company_lw']), axis=1)
    intersections['Cogo'] = intersections.apply(lambda row: create_dict(row['job_cogo'], row['company_cogo']), axis=1)
    # print intersections[intersections.duplicated(['emd5'], keep=False)]

    # check how many users with different titles within the intersection
    diff = intersections.groupby(['job_lw', 'job_cogo']).ngroups

    output = intersections[['emd5', 'Cogo', 'Liveworks']]
    output.to_csv(output_csv_name, sep=',', index=False)

    proc_time = time.time() - proc_time

    total_num_lw = get_num_rows('cogo_list_v1')

    # MySQL 5.7 supports JSON data type
    create_tbl_statement = """
            CREATE TABLE IF NOT EXISTS `cogo_table` (
            `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `emd5` varchar(32) NOT NULL,
            `Cogo` JSON NOT NULL,
            `Liveworks` JSON NOT NULL
            );
    """

    print "Total runtime is %f sec" % proc_time
    print "The number of users within both dataset: ", len(intersections)
    print "The number of users unique to Cogo's dataset: ", total_len_cogo - len(intersections)
    print "The number of users unique to Livework's dataset: ", total_num_lw - len(intersections)
    print "In the both datasets, the %s of users with different job titles. " % '%0.2f%%' % (diff * 100.0 / len(intersections))
    print_csv(output_csv_name, 10)
    print create_tbl_statement
