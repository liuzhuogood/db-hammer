# -*- coding: utf-8 -*-
import functools
import logging
import threading


def Async(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        my_thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        my_thread.start()

    return wrapper


def Except(func):
    """捕获异常装饰器"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # serror_info = traceback.format_exc()
            # log.error(serror_info)
            # msg_box = QMessageBox(QMessageBox.Warning, '报错', serror_info)
            # msg_box.show()
            # msg_box.exec_()
            logging.exception(e)

    return wrapper
