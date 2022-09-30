# -*- coding: utf-8 -*-
import re


class DictUtil(object):
    """
    将list数据（下划线式）转换成驼峰式的字典结构
    """

    def __init__(self, class_name, trans_type='camel'):
        func = self.to_lower_camel
        if trans_type == 'snake':
            func = self._to_snake
        for key, index in class_name.__dict__.items():
            if '__' in key:
                continue
            new_key = func(key)
            setattr(self, new_key, index)

    @staticmethod
    def to_lower_camel(name: str):
        """下划线转小驼峰法命名"""
        return re.sub('_([a-zA-Z])', lambda m: (m.group(1).upper()), name.lower())

    @staticmethod
    def to_uper_camel(name: str):
        """下划线转大写驼峰法命名"""
        a = re.sub('_([a-zA-Z])', lambda m: (m.group(1).upper()), name.lower())
        return a[0].upper() + a[1:]

    def _to_snake(self, name: str) -> str:
        """驼峰转下划线"""
        if '_' not in name:
            name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        else:
            raise ValueError(f'{name}字符中包含下划线，无法转换')
        return name.lower()

    def list2dict(self, arrays):
        return {key: arrays[index] for key, index in self.__dict__.items()}

    @staticmethod
    def dict_arrays_keyupper(arrays):
        arrays_new = []
        for obj in arrays:
            arrays_new.append({DictUtil.to_uper_camel(key): obj[key] for key in obj.keys()})
        return arrays_new

    @staticmethod
    def dict_keyupper(dict_obj):
        return {str(key).upper(): value for key, value in dict_obj}


if __name__ == '__main__':
    print(DictUtil.to_lower_camel("qqq_cvvv"))
