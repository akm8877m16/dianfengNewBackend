# -*- coding:utf-8 -*-
from tornado.concurrent import run_on_executor
from tornado import gen
from tornado.web import asynchronous
import sys
sys.path.append('/home/webapps/dianfeng')
from restServer.apiHandler.base import BaseHandler
from sqlORM.sqlORM import CronTask,OnceTask
from utils.ajax import JsonResponse,JsonError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
import rpyc
import logging
logging.basicConfig()

hexTrans = lambda x: int(x,16)
rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

class getTasksHandler(BaseHandler):
    @run_on_executor
    def getTasks(self, sn):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        oncetasks = []
        query = session.query(OnceTask).filter(and_(OnceTask.device_sn == sn,OnceTask.device_sn != 3)).all()
        for tasks in query:
            task = {
                'id': tasks.id,
                'job': tasks.job,
                'device_sn': tasks.device_sn,
                'name': tasks.name,
                'topic': tasks.topic,
                'payload': tasks.payload,
                'run_date': tasks.run_date.strftime('%Y-%m-%d %H:%M:%S'),
                'status': tasks.status,
                'create_time': tasks.create_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            oncetasks.append(task)

        crontasks = []
        query = session.query(CronTask).filter(and_(CronTask.device_sn == sn,CronTask.status != 3)).all()
        for tasks in query:
            task = {
                'id': tasks.id,
                'job': tasks.job,
                'device_sn': tasks.device_sn,
                'name': tasks.name,
                'topic': tasks.topic,
                'payload': tasks.payload,
                'year': tasks.year,
                'month': tasks.month,
                'day': tasks.day,
                'week': tasks.week,
                'day_of_week': tasks.day_of_week,
                'hour': tasks.hour,
                'minute': tasks.minute,
                'second': tasks.second,
                'start_date': tasks.start_date.strftime('%Y-%m-%d %H:%M:%S') if tasks.start_date is not None else '',
                'end_date': tasks.end_date.strftime('%Y-%m-%d %H:%M:%S') if tasks.end_date is not None else '',
                'status': tasks.status,
                'create_time': tasks.create_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            crontasks.append(task)
        result = {'OnceTask': oncetasks, 'crontasks': crontasks}
        return result

    @asynchronous
    @gen.coroutine
    def get(self):
        sn = self.get_argument('sn', None)
        if sn is None:
            self.write(JsonError('参数错误'))
            self.finish()
        else:
            sn = sn.upper()
            res = yield self.getTasks(sn)
            self.write(JsonResponse(res))
            self.finish()


class pauseJobHandler(BaseHandler):
    @run_on_executor
    def pauseTask(self,job, type):
        print job
        Session = sessionmaker(bind=self.engine)
        session = Session()
        task = None
        if type == 'date':
            task = session.query(OnceTask).filter(OnceTask.job == job).first()
        elif type == 'cron':
            task = session.query(CronTask).filter(CronTask.job == job).first()
        print task
        if task is not None:
            task.status = 2
            session.commit()
        session.close()
        return 'success'

    @asynchronous
    @gen.coroutine
    def get(self):
        job = self.get_argument('job', '')
        type = self.get_argument('type', '')
        if not job or not type:
            self.write(JsonError(u'无参数'))
            self.finish()
        else:
            splStr = job.split('_')
            jobid = splStr[1]
            try:
                conn = rpyc.connect('localhost', 12345)
                conn.root.pause_job(jobid)
                yield self.pauseTask(job, type)
                self.write(JsonResponse(job))
            except Exception as e:
                print e
                self.write(JsonError('pause task failed'))
            finally:
                conn.close()
                self.finish()


class resumeJobHandler(BaseHandler):
    @run_on_executor
    def resumeTask(self, job, type):
        print job
        Session = sessionmaker(bind=self.engine)
        session = Session()
        task = None
        if type == 'date':
            task = session.query(OnceTask).filter(OnceTask.job == job).first()
        elif type == 'cron':
            task = session.query(CronTask).filter(CronTask.job == job).first()
        print task
        if task is not None:
            task.status = 0
            session.commit()
        session.close()
        return 'success'

    @asynchronous
    @gen.coroutine
    def get(self):
        job = self.get_argument('job', '')
        type = self.get_argument('type', '')
        if not job or not type:
            self.write(JsonError(u'无参数'))
            self.finish()
        else:
            splStr = job.split('_')
            jobid = splStr[1]
            try:
                conn = rpyc.connect('localhost', 12345)
                conn.root.resume_job(jobid)
                yield self.resumeTask(job, type)
                self.write(JsonResponse(job))
            except Exception as e:
                print e
                self.write(JsonError('resume task failed'))
            finally:
                conn.close()
                self.finish()



class addTaskHandler(BaseHandler):
    @run_on_executor
    def addCronTask(self,result,sn,taskName,taskTopic,taskMsg,year,month,day,week,day_of_week,hour,
                    minute,second,start_date,end_date):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        newCronTask = CronTask(job=result, device_sn=sn, name=taskName, topic=taskTopic,
                               payload=taskMsg,year = year,month = month, day = day, week = week, day_of_week = day_of_week,
                               hour = hour, minute = minute, second = second, start_date = start_date,end_date = end_date)
        session.add(newCronTask)
        session.commit()
        session.close()
        return 'success'

    @asynchronous
    @gen.coroutine
    def post(self):
        taskName = self.get_argument('taskName', '')
        taskTopic = self.get_argument('taskTopic', '')
        # base64编码的消息内容
        taskMsg = self.get_argument('taskMsg', '')
        # Cron时间
        year = self.get_argument('year', '*')  # (int|str) – 4-digit year
        month = self.get_argument('month', '*')  # (int|str) – month (1-12)
        day = self.get_argument('day', '*')  # day of the (1-31)
        week = self.get_argument('week', '*')  # (int|str) – ISO week (1-53)
        day_of_week = self.get_argument('day_of_week', '*')  # number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour = self.get_argument('hour', '*')  # (int|str) – hour (0-23)
        minute = self.get_argument('minute', '*')  # (int|str) – minute (0-59)
        second = self.get_argument('second', '*')  # (int|str) – second (0-59)
        start_date = self.get_argument('start_date', None)  # 起始时间 需要转成 str 类型  或 NoneType
        end_date = self.get_argument('end_date', None)  # 结束时间 需要转成 str 类型  或 NoneType
        sn = self.get_argument('sn', None)
        if not taskName or not taskTopic or not taskMsg or not sn:
            self.write(JsonError('参数错误'))
            self.finish()
        else:
            payload = taskMsg.split(' ')
            ack = bytearray()
            ack.extend(map(hexTrans,payload))
            print payload
            try:
                conn = rpyc.connect('localhost', 12345)
                task = conn.root.add_job('cron',args=[taskTopic, ack], year=year, month=month, day=day, week=week,
                                     day_of_week=day_of_week, hour=hour, minute=minute, second=second,
                                     start_date=start_date, end_date=end_date)

                yield self.addCronTask('hpy_'+task.id, sn, taskName, taskTopic, taskMsg, year, month,
                                             day, week, day_of_week, hour, minute, second, start_date, end_date)
                self.write(JsonResponse('hpy_'+task.id))
            except Exception as e:
                print e
                self.write(JsonResponse(e.__str__()))
            finally:
                conn.close()
                self.finish()


class removeJobHandler(BaseHandler):
    @run_on_executor
    def removeTask(self,job, type):
        print job
        Session = sessionmaker(bind=self.engine)
        session = Session()
        task = None
        if type == 'date':
            task = session.query(OnceTask).filter(OnceTask.job == job).first()
        elif type == 'cron':
            task = session.query(CronTask).filter(CronTask.job == job).first()
        print task
        if task is not None:
            task.status = 3
            session.commit()
        return 'success'

    @asynchronous
    @gen.coroutine
    def get(self):
        job = self.get_argument('job', '')
        type = self.get_argument('type', '')
        if not job or not type:
            self.write(JsonError(u'无参数'))
            self.finish()
        else:
            splStr = job.split('_')
            jobid = splStr[1]
            try:
                conn = rpyc.connect('localhost', 12345)
                conn.root.remove_job(jobid)
                yield self.removeTask(job, type)
                self.write(JsonResponse(job))
            except Exception as e:
                print e
                self.write(JsonResponse(e.__str__()))
            finally:
                conn.close()
                self.finish()


class addOnceTaskHandler(BaseHandler):
    @run_on_executor
    def addonceTask(self,job,name,topic,payload,run_date,sn):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        newOnceTask = OnceTask(job=job, device_sn=sn, name=name, topic=topic,
                               payload=payload, run_date=run_date )
        session.add(newOnceTask)
        session.commit()
        session.close()
        return 'success'

    @asynchronous
    @gen.coroutine
    def post(self):
        taskName = self.get_argument('taskName', '')
        taskTopic = self.get_argument('taskTopic', '')
        # base64编码的消息内容
        taskMsg = self.get_argument('taskMsg', '')
        # 定时时间
        run_date = self.get_argument('run_date', '')
        sn = self.get_argument('sn', None)
        if not taskName or not taskTopic or not taskMsg or not sn:
            self.write(JsonError('参数错误'))
            self.finish()
        else:
            # alarm_time = run_date.strftime('%Y-%m-%d %H:%M:%S')
            payload = taskMsg.split(' ')
            ack = bytearray()
            ack.extend(map(hexTrans, payload))
            print payload
            try:
                conn = rpyc.connect('localhost', 12345)
                task = conn.root.add_job('date', run_date=run_date, args=[taskTopic, ack])
                # 任务存入数据库  时间可以以str形式 也可以格式化为 datetime对象后再存入 datetime.strptime(run_date,'%Y-%m-%d %H:%M:%S')
                yield self.addonceTask('hpy_'+task.id, taskName, taskTopic, taskMsg, run_date, sn)
                self.write(JsonResponse('hpy_'+task.id))
            except Exception as e:
                print e
                self.write(JsonResponse(e.__str__()))
            finally:
                conn.close()
                self.finish()

class resumeOnceTaskHandler(BaseHandler):
    @run_on_executor
    def removeTask(self, job, type):
        print job
        Session = sessionmaker(bind=self.engine)
        session = Session()
        task = None
        if type == 'date':
            task = session.query(OnceTask).filter(OnceTask.job == job).first()
        elif type == 'cron':
            task = session.query(CronTask).filter(CronTask.job == job).first()
        print task
        if task is not None:
            task.status = 3
            session.commit()
        session.close()
        return 'success'

    @run_on_executor
    def addonceTask(self, job, name, topic, payload, run_date, sn, status):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        newOnceTask = OnceTask(job=job, device_sn=sn, name=name, topic=topic,
                               payload=payload, run_date=run_date, status=status)
        session.add(newOnceTask)
        session.commit()
        session.close()
        return 'success'

    @asynchronous
    @gen.coroutine
    def post(self):
        job = str(self.get_argument('job', ''))
        type = self.get_argument('type', '')
        task_name = self.get_argument('taskName', '')
        task_topic = self.get_argument('taskTopic', '')
        task_msg = self.get_argument('taskMsg', '')
        run_date = self.get_argument('run_date', '')
        sn = self.get_argument('sn', '')
        status = self.get_argument('status', '')
        if not job or not type or not task_name or not task_topic or not task_msg or not run_date or not sn or not status:
            self.write(JsonError('参数错误'))
            self.finish()
        else:
            #remove first
            splStr = job.split('_')
            jobid = splStr[1]
            try:
                conn = rpyc.connect('localhost', 12345)
                conn.root.remove_job(jobid)
                yield self.removeTask(job, type)
                conn.close()
            except Exception as e:
                print e
                self.write(JsonResponse(e.__str__()))
                conn.close()
                self.finish()
                return
            #add again
            payload = task_msg.split(' ')
            ack = bytearray()
            ack.extend(map(hexTrans, payload))
            print payload
            try:
                conn = rpyc.connect('localhost', 12345)
                task = conn.root.add_job('date', run_date=run_date, args=[task_topic, ack])
                # 任务存入数据库  时间可以以str形式 也可以格式化为 datetime对象后再存入 datetime.strptime(run_date,'%Y-%m-%d %H:%M:%S')
                yield self.addonceTask('hpy_' + task.id, task_name, task_topic, task_msg, run_date, sn, 1)
                self.write(JsonResponse('hpy_' + task.id))
            except Exception as e:
                print e
                self.write(JsonResponse(e.__str__()))
            finally:
                conn.close()
                self.finish()
