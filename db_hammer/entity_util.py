import datetime
from db_hammer.page import PageInput
from db_hammer.sql_exception import EntityException
from db_hammer.util.date import date_to_str

"""
__table_name__
__primary_key__
__column_map__ : dict
__ignore_columns__ : [] or ()
"""


def init_entity(entity):
    if not hasattr(entity, "__primary_key__"):
        if hasattr(entity, "id"):
            setattr(entity, "__primary_key__", "id")
        elif hasattr(entity, "guid"):
            setattr(entity, "__primary_key__", "guid")
        elif hasattr(entity, "uuid"):
            setattr(entity, "__primary_key__", "uuid")
        else:
            raise EntityException(f"{entity.__class__.__name__} No defind __primary_key__")

    if not hasattr(entity, "__table_name__"):
        setattr(entity, "__table_name__", f"`{entity.__class__.__name__}`")
    else:
        if getattr(entity, "__table_name__")[0] != '`':
            setattr(entity, "__table_name__", f"`{getattr(entity, '__table_name__')}`")


def get_entity_fields(entity, none_tag=False):
    """
    获取对象的所的字段
    none_tag : 没有值标签
    """
    if isinstance(entity, type):
        entity = entity()
    dd = dir(entity)
    _dict = {}
    ignore_columns = getattr(entity, "__ignore_columns__", None)
    for d in dd:
        if d.startswith("_"):
            continue

        if ignore_columns is not None and d in ignore_columns:
            continue
        if hasattr(entity, d):
            v = getattr(entity, d)
            if none_tag and v is None:
                _dict[d] = None
            else:
                if isinstance(v, (str, int, float, datetime.datetime, datetime.date)):
                    _dict[d] = v
        else:
            _dict[d] = None
    return _dict


def tag_value(value):
    if value is None:
        return "%s", "null"
    else:
        if isinstance(value, str):
            return "%s", value
        elif isinstance(value, (datetime.datetime, datetime.date)):
            return "%s", date_to_str(value)
        elif isinstance(value, int):
            return "%d", value
        elif isinstance(value, float):
            return "%f", value
        else:
            raise Exception(f"不支持的类型:{value}-{type(value)}")


def insert_sql(entity) -> (str, []):
    dd = get_entity_fields(entity)
    values_tag = []
    values = {}
    table_name = getattr(entity, "__table_name__")
    fields = []
    for d in dd:
        value = getattr(entity, d)
        values_tag.append(f":{d}")  # 占位符
        fields.append(f"`{d}`")
        values[d] = value
    return f"""INSERT INTO {table_name}({",".join(fields)}) VALUES ({",".join(values_tag)})""", values


def update_sql(entity, pass_null=False):
    primary_key = getattr(entity, "__primary_key__")
    table_name = getattr(entity, "__table_name__")
    where = ""
    values_tag = []
    values = {}
    dd = get_entity_fields(entity)
    for d in dd:
        value = getattr(entity, d)
        if value is None or value == "":
            if pass_null:
                continue
        values_tag.append(f"{d}=:{d}")
        values[d] = value
    if isinstance(primary_key, list):
        for k in primary_key:
            value = getattr(entity, k)
            values_tag.append(f"{k}=:{k}")
            values[k] = value
        if where == "":
            where += ' '.join(values_tag)
        else:
            where += ' AND '.join(values_tag)
    else:
        value = getattr(entity, primary_key)
        if where == "":
            where += f' {primary_key}=:{primary_key}'
        else:
            where += f' AND {primary_key}=:{primary_key}'

        values[primary_key] = value

    return f"""UPDATE {table_name} SET {",".join(values_tag)} WHERE{where}""", values


def delete_sql(entity):
    primary_key = getattr(entity, "__primary_key__")
    table_name = getattr(entity, "__table_name__")
    where = ""
    values_tag = []
    values = {}
    if isinstance(primary_key, list):
        for k in primary_key:
            value = getattr(entity, k)
            values_tag.append(f"{k}=:{k}")
            values[k] = value
            where += ' AND '.join(values_tag)
    else:
        value = getattr(entity, primary_key)
        where += f' {primary_key}=:{primary_key}'
        values[primary_key] = value
    return f"""DELETE FROM {table_name} WHERE {where}""", values


def entity_list(dict_list, entity_class) -> list:
    cols = get_entity_fields(entity_class, none_tag=True)
    result = []
    for record in dict_list:
        entity = dic_to_entity(entity_class, record, cols)
        result.append(entity)
    return result


def dic_to_entity(entity_class, record, cols=None):
    en = {}
    if cols is None:
        cols = record.keys()
    for c in cols:
        en[c] = record[str(c)]
    if type(entity_class.__class__) == type:
        if len(cols) != 0:
            if callable(entity_class):
                entity = entity_class(**en)
            else:
                entity = entity_class.__class__()
                for c in record.keys():
                    setattr(entity, c, record[str(c)])
        else:
            # 说明没有定义dataclasses
            entity = entity_class()
            for c in record.keys():
                setattr(entity, c, record[str(c)])
    else:
        entity = entity_class.__class__(**en)
    return entity


def where_entity(entity, rel="AND"):
    dd = get_entity_fields(entity)
    where = ""
    values = {}
    for d in dd:
        value = getattr(entity, d)
        if value is not None:
            # 字符串为空时，不加条件
            if isinstance(value, str) and value == "":
                continue
            if where == "":
                where += f' WHERE {d}=:{d}'
            else:
                where += f' {rel} {d}=:{d}'
            values[d] = value
    return where, values


def where_like_entity(entity, rel="AND"):
    dd = get_entity_fields(entity)
    where = ""
    values = {}
    for d in dd:
        value = getattr(entity, d)
        if value is not None:
            # 字符串为空时，不加条件
            if isinstance(value, str) and value == "":
                continue
            if where == "":
                where += f' WHERE {d} LIKE :{d}'
            else:
                where += f' {rel} {d} LIKE :{d}'
            values[d] = f"%{value}%"
    return where, values


def order_by_pagination(entity, pagination: PageInput):
    dd = get_entity_fields(entity)
    sortBy = pagination.sort_by
    descending = pagination.descending
    if sortBy != "" and sortBy in dd.keys():
        return f" ORDER BY {sortBy} {'' if descending else 'DESC'}"
    return ""


def get_entity_primary_key(entity):
    primary_key = getattr(entity, "__primary_key__")
    if isinstance(primary_key, list):
        pk_value = {}
        for pk in primary_key:
            pk_value[pk] = getattr(entity, pk)
        return primary_key, pk_value
    else:
        pk_value = None
        if hasattr(entity, primary_key):
            pk_value = getattr(entity, primary_key)
        return primary_key, pk_value
