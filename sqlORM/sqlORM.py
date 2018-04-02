# -*- coding:utf-8 -*-
from sqlalchemy import create_engine, text, Column, Integer, String, Sequence,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime


Base = declarative_base()

class CronTask(Base):
    __tablename__ = 'task_crontask'

    id = Column(Integer, Sequence('id'), primary_key=True)
    job = Column(String(length=100))
    device_sn = Column(String(length=16))
    name = Column(String(length=255))
    topic = Column(String(length=100))
    payload = Column(String(length=255))
    year = Column(String(length=5))
    month = Column(String(length=5))
    day = Column(String(length=5))
    week = Column(String(length=5))
    day_of_week = Column(String(length=15))
    hour = Column(String(length=5))
    minute = Column(String(length=5))
    second = Column(String(length=5))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    status = Column(Integer, default=0)
    create_time = Column(DateTime,default=datetime.datetime.now())


class OnceTask(Base):
    __tablename__ = 'task_oncetask'

    id = Column(Integer, Sequence('id'), primary_key=True)
    job = Column(String(length=100))
    device_sn = Column(String(length=16))
    name = Column(String(length=255))
    topic = Column(String(length=100))
    payload = Column(String(length=255))
    run_date = Column(DateTime)
    status = Column(Integer, default=0)
    create_time = Column(DateTime,default=datetime.datetime.now())

# 主程序
if __name__ == '__main__':
    '''
    Session = sessionmaker(bind=engine)
    session = Session()

    res = session.query(OnceTask).first()
    print res.id, res.name, res.job

    session.close()
    '''
    '''
    type = 'cron'
    job = 'hpy_6c8acea724274a138b0338a5aa49c641'
    Session = sessionmaker(bind=engine)
    session = Session()
    task = None
    if type == 'date':
        task = session.query(OnceTask).filter(OnceTask.job==job).first()
    elif type == 'cron':
        task = session.query(CronTask).filter(CronTask.job==job).first()
    if task is not None:
        session.delete(task)
        session.commit()
    session.close()
    '''