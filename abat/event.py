# -*- coding: utf-8 -*-
"""
Created on 2017/11/13
@author: MG
"""
import logging
from enum import IntEnum, unique
from config import Config
from abat.common import PeriodType
logger = logging.getLogger()


@unique
class EventType(IntEnum):
    Tick_MD_Event = 0
    Min1_MD_Event = 1
    Min5_MD_Event = 5
    Min15_MD_Event = 15

    @staticmethod
    def try_2_period_type(event_type):
        """
        将 EventType类型转换为 PeriodType类型
        由于两个对象不一定一一对应，因此，无法匹配的类型返回None
        :param event_type:
        :return:
        """
        if event_type == EventType.Tick_MD_Event:
            period_type = PeriodType.Tick
        elif event_type == EventType.Min1_MD_Event:
            period_type = PeriodType.Min1
        elif event_type == EventType.Min5_MD_Event:
            period_type = PeriodType.Min5
        elif event_type == EventType.Min15_MD_Event:
            period_type = PeriodType.Min15
        else:
            # raise ValueError('event_type:%s 不是有效的类型' % event_type)
            logger.warning('event_type:%s 不是有效的类型', event_type)
            period_type = None
        return  period_type


class EventAgent:
    """
    事件注册器，用于集中通过事件具备 处理相关类型的事件
    """
    def __init__(self):
        self._event_handler_dic = {}
        self._event_key_handler_dic = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_handler(self, event_type, handler, handler_name=None, key=None):
        """
        注册器:注册时间类型及对应的事件处理程序
        Key!=None，则该句柄只响应匹配 key 的事件
        Key=None，公共事件响应句柄，所有相应类型事件均响应
        :param event_type: 
        :param handler: 
        :param handler_name: 事件句柄名称，每个事件中，句柄名称唯一，重复将被覆盖
        :param key: 事件匹配 key，None代表公共事件响应句柄，所有该类型事件均响应
        :return: 
        """
        if event_type not in self._event_handler_dic:
            self._event_handler_dic[event_type] = {}
            self._event_key_handler_dic[event_type] = {}
        if handler_name is None:
            handler_name = handler.__name__
        self.logger.debug("注册事件处理句柄 %s -> <%s>%s",
                         event_type, handler_name, (" [%s]" % key) if key is not None else "")
        if key is None:
            if handler_name in self._event_handler_dic[event_type]:
                self.logger.warning('相应事件处理句柄 %s -> <%s> 已经存在，重新注册将覆盖原来执行句柄',
                                    event_type, handler_name)
            self._event_handler_dic[event_type][handler_name] = handler
        else:
            if key not in self._event_key_handler_dic[event_type]:
                self._event_key_handler_dic[event_type][key] = {}
            self._event_key_handler_dic[event_type][key][handler_name] = handler

    def send_event(self, event_type, data, key=None):
        """
        触发事件
        如果带Key，则触发 匹配 key 事件，以及全部 无 key 的事件响应句柄
        否则，只触发公共事件响应句柄
        :param event_type: 
        :param data: 
        :param key: 事件匹配 key，None代表只触发公共事件响应句柄
        :return: 
        """
        # 公共事件响应句柄
        if event_type in self._event_handler_dic:
            handler_dic = self._event_handler_dic[event_type]
            error_name_list = []
            for handler_name, handler in handler_dic.items():
                try:
                    handler(data)
                except:
                    self.logger.exception('%s run with error will be remove from register', handler_name)
                    error_name_list.append(handler_name)
            for handler_name in error_name_list:
                del handler_dic[handler_name]
                self.logger.warning('从注册器中移除 %s - %s', event_type, handler_name)
        # key 匹配事件响应句柄
        if key is not None and event_type in self._event_key_handler_dic:
            key_handler_dic = self._event_key_handler_dic[event_type]
            if key in key_handler_dic:
                handler_dic = key_handler_dic[key]
                error_name_list = []
                for handler_name, handler in handler_dic.items():
                    try:
                        handler(data)
                    except:
                        self.logger.exception('%s run with error will be remove from register', handler_name)
                        error_name_list.append(handler_name)
                for handler_name in error_name_list:
                    del handler_dic[handler_name]
                    self.logger.warning('从注册器中移除 %s - %s - %s', event_type, str(key), handler_name)


event_agent = EventAgent()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    event_agent.register_handler(EventType.Tick_MD_Event, lambda x: print('T:', x), 'print_t')
    event_agent.register_handler(EventType.Min1_MD_Event, lambda x: print('M:', x), 'print_m')
    event_agent.register_handler(EventType.Tick_MD_Event, lambda x: print('T 1:', x), 'print_t with key1', key='1')
    event_agent.send_event(EventType.Tick_MD_Event, 'sdf')
    event_agent.send_event(EventType.Tick_MD_Event, 'sdf', key='1')
