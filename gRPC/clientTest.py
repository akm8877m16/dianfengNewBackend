# -*- coding:utf-8 -*-
"""
This is an example RPC client that connects to the RPyC based scheduler service.

It first connects to the RPyC server on localhost:12345.
Then it schedules a job to run on 2 second intervals and sleeps for 10 seconds.
After that, it unschedules the job and exits.
"""

from time import sleep

import rpyc


if __name__ == '__main__':
        taskName = 'ywh_test'
        taskTopic = 'D/ywhywh'
        # Cron时间
        year =  '*'  # (int|str) – 4-digit year
        month =  '*' # (int|str) – month (1-12)
        day =  '*' # day of the (1-31)
        week =  '*'  # (int|str) – ISO week (1-53)
        day_of_week =  '*' # number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour = '*'  # (int|str) – hour (0-23)
        minute =  '*'  # (int|str) – minute (0-59)
        second =  '40'  # (int|str) – second (0-59)
        start_date = '2018-12-22 22:00:00'  # 起始时间 需要转成 str 类型  或 NoneType
        end_date = None  # 结束时间 需要转成 str 类型  或 NoneType
        sn = 'ywhywh'
        ack='1155 1155 1155 1155'

        conn = rpyc.connect('localhost', 12345)
        #print conn.root
        #job = conn.root.add_job('cron', args=[taskTopic, ack], year=year, month=month, day=day, week=week,
        #                                     day_of_week=day_of_week, hour=hour, minute=minute, second=second,
        #                                 start_date=start_date, end_date=end_date)
        #print job.id
        #result = conn.root.get_jobs()

        jobId = '003c5b2bb7a64afd89ca9ca860505dc3'
        result = conn.root.pause_job(jobId)
        sleep(20)
        result = conn.root.resume_job(jobId)
        print result
        sleep(20)
        result = conn.root.remove_job(jobId)
        print result
        sleep(20)
        #print result
        conn.close()
        #sleep(10)
        #job = conn.root.pause_job(jobid)
        #conn.root.remove_job(job.id)