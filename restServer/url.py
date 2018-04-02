# -*- coding:utf-8 -*-

from apiHandler.devices.electricMeter import realDataHandler, dayHisDataHandler, getDevicesHandler, monthHisDataHandler
from apiHandler.timerTasks.tasks import getTasksHandler,pauseJobHandler,resumeJobHandler,addTaskHandler,removeJobHandler,\
    addOnceTaskHandler, resumeOnceTaskHandler
urls = [
    [r'/api/electricMeter/real/(\w+)', realDataHandler],
    [r'/api/electricMeter/history/month/(\w+)', monthHisDataHandler],
    [r'/api/electricMeter/history/(\w+)',dayHisDataHandler],
    [r'/api/electricMeter/getDevices/(\w+)', getDevicesHandler],
    [r'/api/task/getTasks', getTasksHandler],  #获取某个网关下的所有定时任务
    [r'/api/task/pauseJob', pauseJobHandler],  #暂停定时任务
    [r'/api/task/resumeJob', resumeJobHandler],  #从暂停恢复定时任务
    [r'/api/task/addCronTask', addTaskHandler],  #添加定时任务
    [r'/api/task/removeJob', removeJobHandler],  #删除定时任务
    [r'/api/task/addOnceTask', addOnceTaskHandler],  #添加一次性定时任务
    [r'/api/task/update_once_task', resumeOnceTaskHandler],  #暂停后恢复一次性定时任务
]
