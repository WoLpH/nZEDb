#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
import sys
import os
import time
import threading
try:
    import queue
except ImportError:
    import Queue as queue
import subprocess
import string
import signal
import datetime
import lib.info as info
from lib.info import bcolors
conf = info.readConfig()
cur = info.connect()


def printer(color, format, *args, **kwargs):
    print(color + format.format(*args, **kwargs) + bcolors.ENDC)


def header(format, *args, **kwargs):
    printer(bgcolors.HEADER, format, *args, **kwargs)


def error(format, *args, **kwargs):
    printer(bgcolors.ERROR, format, *args, **kwargs)


if len(sys.argv) == 1:
    error('''
Wrong set of arguments.
The first argument [additional, nfo, movie, clean] determines the postprocessing to do.
The optional second argument for [additional, nfo] [groupid, categoryid] allows to process only that group or category.
The optional second argument for [movies, tv] [clean] allows processing only properly renamed releases.

python postprocess_threaded.py [additional, nfo] (optional [groupid, categoryid])
python postprocess_threaded.py [movie, tv] (optional [clean])
    ''')
    sys.exit()

if len(sys.argv) == 3 and sys.argv[2] == 'clean':
    header('\nPostProcess {} Clean Threaded Started at {}',
           sys.argv[1], datetime.datetime.now().strftime('%H:%M:%S'))
else:
    header('\nPostProcess {} Threaded Started at {}',
           sys.argv[1], datetime.datetime.now().strftime('%H:%M:%S'))


if sys.argv[1] == 'additional':
    header('Downloaded: b = yEnc article, f= failed ;Processing: z = zip file, r = rar file')
    header('Added: s = sample image, j = jpeg image, A = audio sample, a = audio mediainfo, v = video sample')
    header('Added: m = video mediainfo, n = nfo, ^ = file details from inside the rar/zip')

elif sys.argv[1] == 'nfo':
    header('* = hidden NFO, + = NFO, - = no NFO, f = download failed.')


# You can limit postprocessing for additional and nfo by groupid or categoryid
if len(sys.argv) == 3 and sys.argv[2].isdigit() and len(sys.argv[2]) < 4:
    groupID = 'AND groupid = '+sys.argv[2]
    header('Using groupid {}', sys.argv[2])

elif len(sys.argv) == 3 and sys.argv[2].isdigit() and len(sys.argv[2]) == 4:
    category_id = int(sys.argv[2])

    if category_id % 1000 == 0 and 1000 <= category_id <= 8000:
        groupID = 'AND categoryid BETWEEN {} AND {}'.format(
            category_id, category_id + 999)
        header('Using categoryids {}-{}', category_id, category_id + 999)
    else:
        groupID = 'AND categoryid = {}'.format(category_id)
        header('Using categoryid {}', category_id)
else:
    groupID = ''


# you can sort tv releases by searchname
if len(sys.argv) == 3 and (sys.argv[2] == "asc" or sys.argv[2] == "desc"):
    orderBY = 'ORDER BY searchname ' + sys.argv[2]
    header('Using ORDER BY searchname {}', sys.argv[2])


if len(sys.argv) == 4 and (sys.argv[3] == "asc" or sys.argv[3] == "desc"):
    orderBY = 'ORDER BY searchname ' + sys.argv[3]
    header('Using CLEAN - ORDER BY searchname {}', sys.argv[3])
else:
    orderBY = 'ORDER BY postdate DESC'

start_time = time.time()
pathname = os.path.abspath(os.path.dirname(sys.argv[0]))

if len(sys.argv) > 1 and sys.argv[1] == 'additional':
    cur[0].execute('''
    SELECT
    (SELECT value
    FROM site
    WHERE setting = 'postthreads') AS a,

    (SELECT value
    FROM site
    WHERE setting = 'maxaddprocessed') AS b,

    (SELECT value
    FROM site
    WHERE setting = 'maxnfoprocessed') AS c,

    (SELECT value
    FROM site
    WHERE setting = 'maximdbprocessed') AS d,

    (SELECT value
    FROM site
    WHERE setting = 'maxrageprocessed') AS e,

    (SELECT value
    FROM site
    WHERE setting = 'maxsizetopostprocess') AS f,

    (SELECT value
    FROM site
    WHERE setting = 'tmpunrarpath') AS g,

    (SELECT value
    FROM tmux
    WHERE setting = 'post') AS h,

    (SELECT value
    FROM tmux
    WHERE setting = 'post_non') AS i,

    (SELECT count(*)
    FROM releases
    WHERE haspreview = -1
        and passwordstatus = -1 {groupID} ) as j,

    (SELECT count(*)
    FROM releases
    WHERE haspreview = -1
        and passwordstatus = -2 {groupID}) as k,

    (SELECT count(*)
    FROM releases
    WHERE haspreview = -1
        and passwordstatus = -3 {groupID}) as l,

    (SELECT count(*)
    FROM releases
    WHERE haspreview = -1
        and passwordstatus = -4 {groupID}) as m,

    (SELECT count(*)
    FROM releases
    WHERE haspreview = -1
        and passwordstatus = -5 {groupID}) as n,

    (SELECT count(*)
    FROM releases
    WHERE haspreview = -1
        and passwordstatus = -6) as o
    '''.format(group_id=groupID))

    dbgrab = cur[0].fetchall()
    ps1 = format(int(dbgrab[0][9]))
    ps2 = format(int(dbgrab[0][10]))
    ps3 = format(int(dbgrab[0][11]))
    ps4 = format(int(dbgrab[0][12]))
    ps5 = format(int(dbgrab[0][13]))
    ps6 = format(int(dbgrab[0][14]))
elif len(sys.argv) > 1 and sys.argv[1] == "nfo":
    cur[0].execute("SELECT (SELECT value FROM site WHERE setting = 'postthreads') AS a, (SELECT value FROM site WHERE setting = 'maxaddprocessed') AS b, (SELECT value FROM site WHERE setting = 'maxnfoprocessed') AS c, (SELECT value FROM site WHERE setting = 'maximdbprocessed') AS d, (SELECT value FROM site WHERE setting = 'maxrageprocessed') AS e, (SELECT value FROM site WHERE setting = 'maxsizetopostprocess') AS f, (SELECT value FROM site WHERE setting = 'tmpunrarpath') AS g, (SELECT value FROM tmux WHERE setting = 'post') AS h, (SELECT value FROM tmux WHERE setting = 'post_non') AS i, (SELECT count(*) FROM releases WHERE nfostatus = -1 "+ \
                   groupID+") as j, (SELECT count(*) FROM releases WHERE nfostatus = -2 "+groupID+") as k, (SELECT count(*) FROM releases WHERE nfostatus = -3 "+groupID+") as l, (SELECT count(*) FROM releases WHERE nfostatus = -4 "+groupID+") as m, (SELECT count(*) FROM releases WHERE nfostatus = -5 "+groupID+") as n, (SELECT count(*) FROM releases WHERE nfostatus = -6 "+groupID+") as o")
    dbgrab = cur[0].fetchall()
    ps1 = format(int(dbgrab[0][9]))
    ps2 = format(int(dbgrab[0][10]))
    ps3 = format(int(dbgrab[0][11]))
    ps4 = format(int(dbgrab[0][12]))
    ps5 = format(int(dbgrab[0][13]))
    ps6 = format(int(dbgrab[0][14]))
elif len(sys.argv) > 1 and (sys.argv[1] == "movie" or sys.argv[1] == "tv"):
    cur[0].execute('''
    SELECT
        (SELECT value
        FROM site
        WHERE setting = 'postthreadsnon') AS a,

        (SELECT value
        FROM site
        WHERE setting = 'maxaddprocessed') AS b,

        (SELECT value
        FROM site
        WHERE setting = 'maxnfoprocessed') AS c,

        (SELECT value
        FROM site
        WHERE setting = 'maximdbprocessed') AS d,

        (SELECT value
        FROM site
        WHERE setting = 'maxrageprocessed') AS e,

        (SELECT value
        FROM site
        WHERE setting = 'maxsizetopostprocess') AS f,

        (SELECT value
        FROM site
        WHERE setting = 'tmpunrarpath') AS g,

        (SELECT value
        FROM tmux
        WHERE setting = 'post') AS h,

        (SELECT value
        FROM tmux
        WHERE setting = 'post_non') AS i
    ''')
    dbgrab = cur[0].fetchall()
else:
    error('''
An argument is required,
postprocess_threaded.py [additional, nfo, movie, tv]
    ''')
    sys.exit()

run_threads = int(dbgrab[0][0])
ppperrun = int(dbgrab[0][1])
nfoperrun = int(dbgrab[0][2])
movieperrun = int(dbgrab[0][3])
tvrageperrun = int(dbgrab[0][4])
maxsizeck = int(dbgrab[0][5])
tmppath = dbgrab[0][6]
posttorun = int(dbgrab[0][7])
postnon = dbgrab[0][8]
maxsize = (int(maxsizeck * 1073741824))

if sys.argv[1] == "additional" or sys.argv[1] == "nfo":
    header('Available to process: -6 = {}, -5 = {}, -4 = {}, -3 = {}, -2 = {}, -1 = {}',
           ps6, ps5, ps4, ps3, ps2, ps1)


if maxsize == 0:
    maxsize = ''
else:
    maxsize = 'AND r.size < {}'.format(maxsizeck * 1073741824)


datas = []
maxtries = -1

process_additional = run_threads * ppperrun
process_nfo = run_threads * nfoperrun


def process_additional_func(tries=6):
    global maxretries
    cur[0].execute('''
    SELECT r.id,
        r.guid,
        r.name,
        c.disablepreview,
        r.size,
        r.groupid,
        r.nfostatus,
        r.categoryid
    from releases r
    LEFT JOIN category c ON c.id = r.categoryid
    WHERE nzbstatus = 1 {} maxsize
    AND r.passwordstatus = -1
    AND r.haspreview = -1
    AND c.disablepreview = 0
    ORDER BY postdate DESC LIMIT {}
     '''.format(maxsize, groupID, process_additional))
    datas = cur[0].fetchall()

    if len(datas) < process_additional and tries > 0:
        process_additional_func(tries - 1)


def process_nfo_func(tries=6):
    global maxretries
    cur[0].execute('''
    SELECT id, guid, groupid, name
    from releases
    WHERE nzbstatus = 1
    AND nfostatus = -1 {}
    ORDER BY postdate DESC LIMIT {}
    '''.format(groupID, process_nfo))
    datas = cur[0].fetchall()

    if len(datas) < process_nfo and tries > 0:
        process_nfo_func(tries - 1)

if sys.argv[1] == 'additional':
    process_additional_func()


elif sys.argv[1] == 'nfo':
    process_nfo_func()


elif sys.argv[1] == "movie" and len(sys.argv) == 3 and sys.argv[2] == "clean":
    run = '''

    SELECT DISTINCT searchname AS name,
                    id,
                    categoryid
    from releases
    WHERE nzbstatus = 1
    AND isrenamed = 1
    AND searchname IS NOT NULL
    AND imdbid IS NULL
    AND categoryid IN
        (SELECT id
        FROM category
        WHERE parentid = 2000)
    ORDER BY postdate DESC LIMIT %s
    '''
    cur[0].execute(run, (run_threads * movieperrun))
    datas = cur[0].fetchall()

elif sys.argv[1] == "movie":
    run = '''
    SELECT searchname AS name,
        id,
        categoryid
    from releases
    WHERE nzbstatus = 1
    AND searchname IS NOT NULL
    AND imdbid IS NULL
    AND categoryid IN
        (SELECT id
        FROM category
        WHERE parentid = 2000)
    ORDER BY postdate DESC LIMIT %s
    '''
    cur[0].execute(run, run_threads * movieperrun)
    datas = cur[0].fetchall()
elif sys.argv[1] == 'tv' and len(sys.argv) == 3 and sys.argv[2] == 'clean':
    run = '''
    SELECT searchname,
        id
    from releases
    WHERE nzbstatus = 1
    AND isrenamed = 1
    AND searchname IS NOT NULL
    AND rageid = -1
    AND categoryid IN
        (SELECT id
        FROM category
        WHERE parentid = 5000)
    {}
    LIMIT {}
    '''.format(orderBY, run_threads * tvrageperrun)
    cur[0].execute(run)
    datas = cur[0].fetchall()
elif sys.argv[1] == 'tv':
    run = '''
    SELECT searchname,
        id
    from releases
    WHERE nzbstatus = 1
    AND searchname IS NOT NULL
    AND rageid = -1
    AND categoryid IN
        (SELECT id
        FROM category
        WHERE parentid = 5000) {} LIMIT {}
    '''.format(orderBY, run_threads * tvrageperrun)
    cur[0].execute(run)
    datas = cur[0].fetchall()

# close connection to mysql
info.disconnect(cur[0], cur[1])

if not datas:
    header('No Work to Process')
    sys.exit()

my_queue = queue.Queue()
time_of_last_run = time.time()

class queue_runner(threading.Thread):
    def __init__(self, my_queue):
        threading.Thread.__init__(self)
        self.my_queue = my_queue

    def run(self):
        global time_of_last_run

        while True:
            try:
                my_id = self.my_queue.get(True, 1)
            except:
                if time.time() - time_of_last_run > 3:
                    return
            else:
                if my_id:
                    time_of_last_run = time.time()
                    subprocess.call([
                        'php',
                        pathname + '/../nix/tmux/bin/postprocess_new.php',
                        '' + my_id,
                    ])
                    time.sleep(.02)
                    self.my_queue.task_done()


def u(x):
    if sys.version_info[0] < 3:
        import codecs
        return codecs.unicode_escape_decode(x)[0]
    else:
        return x


def main(args):
    global time_of_last_run
    time_of_last_run = time.time()

    if sys.argv[1] == 'additional':
        header('We will be using a max of {} threads, a queue of {:,} {} releases. passwordstatus range {} to -1',
               run_threads, len(datas), sys.argv[1], maxtries)
    elif sys.argv[1] == 'nfo':
        header('We will be using a max of {:,} threads, a queue of {} {} releases. nfostatus range {} to -1',
               run_threads, len(datas), sys.argv[1], maxtries)
    else:
        header('We will be using a max of {} threads, a queue of {:,} {} releases.',
               run_threads, len(datas), sys.argv[1])
    time.sleep(2)

    def signal_handler(signal, frame):
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    if True:
        # spawn a pool of place worker threads
        for i in range(run_threads):
            p = queue_runner(my_queue)
            p.setDaemon(False)
            p.start()

    # now load some arbitrary jobs into the queue
    if sys.argv[1] == 'additional':
        for release in datas:
            time.sleep(.02)
            my_queue.put(u('%s           =+=            %s           =+=            %s           =+=            %s           =+=            %s           =+=            %s           =+=            %s           =+=            %s') %
                         (release[0], release[1], release[2], release[3], release[4], release[5], release[6], release[7]))
    elif sys.argv[1] == 'nfo':
        for release in datas:
            time.sleep(.02)
            my_queue.put(u('%s           =+=            %s           =+=            %s           =+=            %s') %
                         (release[0], release[1], release[2], release[3]))
    elif sys.argv[1] == 'movie':
        for release in datas:
            time.sleep(.02)
            my_queue.put(u('%s           =+=            %s           =+=            %s') %
                         (release[0], release[1], release[2]))
    elif sys.argv[1] == 'tv':
        for release in datas:
            time.sleep(.02)
            my_queue.put(u('%s           =+=            %s') %
                         (release[0], release[1]))

    my_queue.join()

    if sys.argv[1] == 'nfo':
        cur = info.connect()
        cur[0].execute('SELECT id from releases WHERE nfostatus <= -6')
        final = cur[0].fetchall()
        if len(datas) > 0:
            for item in final:
                run = 'DELETE FROM releasenfo WHERE nfo IS NULL AND releaseid = %s'
                cur[0].execute(run, (item[0]))
                final = cur[0].fetchall()

        # close connection to mysql
        info.disconnect(cur[0], cur[1])

    header('\nPostProcess {} Threaded Completed at {}',
           sys.argv[1], datetime.datetime.now().strftime('%H:%M:%S'))
    header('Running time: {}\n\n',
           str(datetime.timedelta(seconds=time.time() - start_time)))

if __name__ == '__main__':
    main(sys.argv[1:])

