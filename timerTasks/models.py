# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.db import models


# Create your models here.
# 运行一次定时任务
class OnceTask(models.Model):
    job = models.CharField(max_length = 100)               # 定时job标识   hpyx:xxxxxxxx    hpyx为gearman中的workid  xxxxx为APSscheduler中的job_id号
    device_sn = models.CharField(max_length=16, blank=True)# 设备sn
    name = models.CharField(max_length = 500)              # 定时任务名(描述)
    topic = models.CharField(max_length = 100)             # 设备订阅主题
    payload = models.CharField(max_length = 500)           # 发送指令
    run_date = models.DateTimeField()                      # 指定运行时间
    status = models.IntegerField(default=0)                # 状态 0未执行；1已执行，2暂停
    create_time = models.DateTimeField(auto_now_add=True)  # 任务创建时间

# 运行Cron定时任务
class CronTask(models.Model):
    job = models.CharField(max_length = 100)               # 定时job标识   hpyx:xxxxxxxx    hpyx为gearman中的workid  xxxxx为APSscheduler中的job_id号
    device_sn = models.CharField(max_length=16, blank=True)# 设备sn
    name = models.CharField(max_length = 500)              # 定时任务名(描述)
    topic = models.CharField(max_length = 100)             # 设备订阅主题
    payload = models.CharField(max_length = 500)           # 发送指令
    # cron时间
    year = models.CharField(max_length = 4)
    month = models.CharField(max_length = 2)
    day = models.CharField(max_length = 2)
    week = models.CharField(max_length = 2)
    day_of_week = models.CharField(max_length = 15)
    hour = models.CharField(max_length = 2)
    minute = models.CharField(max_length = 2)
    second = models.CharField(max_length = 2)
    # 指定有效时间
    start_date = models.DateTimeField(null=True)           # 起始时间
    end_date = models.DateTimeField(null=True)             # 结束时间
    status = models.IntegerField(default=0)                # 状态 0未执行；1已执行，2暂停
    create_time = models.DateTimeField(auto_now_add=True)  # 任务创建时间