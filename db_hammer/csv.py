import datetime
import decimal
import gzip
import logging
import os

from db_hammer.util.date import date_to_str


def get_headers(cursor):
    col_names = cursor.description
    cc = []
    for c in col_names:
        cc.append(c[0])
    return cc


def count_rows(cursor, sql, log=None):
    log = log or logging.getLogger(__name__)
    count_sql = f"SELECT COUNT(0) FROM ({sql}) tmp_count"
    log.debug("execute sql:" + count_sql.replace("\n", " "))
    cursor.execute(count_sql)
    data = cursor.fetchone()
    log.debug("fetch rows:" + str(len(data)))
    num = data[0]
    return int(num)


def start(cursor, sql, bachSize, PACK_SIZE, path, file_mode, add_header, CSV_SPLIT, CSV_FIELD_CLOSE, encoding,
          callback, log=None):
    log = log or logging.getLogger(__name__)
    try:
        os.makedirs(path, exist_ok=True)
        log.info(f"export path：%s" % path)
        cursor.execute(sql)
        col_names = get_headers(cursor)
        csv_data = cursor.fetchmany(int(bachSize))
        file_i = 1
        CACHE_COUNT = PACK_SIZE
        CACHE_ROWS = 0
        write_f = None
        c_count = 0
        total = 0
        if callback is not None:
            total = count_rows(cursor, sql, log)
            callback(c_count, total)
        while len(csv_data) > 0:
            log.debug("export row::" + str(c_count))
            CACHE_ROWS += len(csv_data)
            if CACHE_ROWS < CACHE_COUNT and c_count > 0:
                file_i, file_name, write_f = write_file(col_names, csv_data, path, file_i, False, write_f, file_mode,
                                                        add_header, CSV_SPLIT, CSV_FIELD_CLOSE, encoding)
            else:
                file_i, file_name, write_f = write_file(col_names, csv_data, path, file_i, True, write_f, file_mode,
                                                        add_header, CSV_SPLIT, CSV_FIELD_CLOSE, encoding)
                CACHE_ROWS = 0
            c_count += len(csv_data)
            csv_data = cursor.fetchmany(int(bachSize))
            if callback is not None:
                callback(c_count, total)
        if write_f is not None:
            write_f.flush()
            write_f.close()
        log.info(f"export end, total row::%d" % c_count)
    except Exception as e:
        log.exception(e)
    finally:
        pass


def write_file(col_names, csv_data, path, file_i, new, f, file_mode, add_header, CSV_SPLIT, CSV_FIELD_CLOSE,
               encoding):
    add = False
    if file_mode == 'gz':
        file_name = f"{str(file_i).rjust(6, '0')}.gz"
    elif file_mode == 'csv':
        file_name = f"{str(file_i).rjust(6, '0')}.csv"
    else:
        file_name = f"{str(file_i).rjust(6, '0')}.txt"
    if len(csv_data) == 0:
        return file_i, None, None
    if new:
        if f is not None:
            f.flush()
            f.close()
        file_i += 1
        add = True
        if file_mode == 'gz':
            f = gzip.open(f"{path}/{file_name}", 'wb+')
        else:
            f = open(f"{path}/{file_name}", 'wb+')
    __run_rows(rows=csv_data,
               header=col_names,
               file=f,
               add_header=add and add_header,
               CSV_SPLIT=CSV_SPLIT,
               CSV_FIELD_CLOSE=CSV_FIELD_CLOSE,
               encoding=encoding
               )
    return file_i, file_name, f


def __run_rows(rows, header, add_header=False, file=None, CSV_FIELD_CLOSE='"', CSV_SPLIT=",", encoding="utf-8"):
    for r in rows:
        fields = r
        field_map = {}
        for i in range(0, len(header)):
            c = header[i]
            v = fields[i]
            if isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
                if v is None:
                    field_map[c] = ""
                else:
                    field_map[c] = date_to_str(v)
            elif isinstance(v, int):
                if v is None:
                    field_map[c] = '0'
                else:
                    field_map[c] = str(v)
            elif isinstance(v, decimal.Decimal):
                if v is None:
                    field_map[c] = '0'
                else:
                    field_map[c] = str(round(v, 4))
            elif isinstance(v, float):
                if v is None:
                    v = '0'
                field_map[c] = str(v)
            elif isinstance(v, str):
                # 空处理
                if v is None:
                    field_map[c] = ""
                else:
                    # 回车换行处理
                    field_map[c] = str(v).replace("\n", "").replace(CSV_FIELD_CLOSE, ""). \
                        replace(CSV_SPLIT, "")
            else:
                if v is None:
                    field_map[c] = ""
                else:
                    raise Exception(f"[{c}]--NO SUPPORT TYPE--[{type(v)}]")
        if add_header:
            row_str = CSV_FIELD_CLOSE + f'{CSV_FIELD_CLOSE}{CSV_SPLIT}{CSV_FIELD_CLOSE}'.join(header) + CSV_FIELD_CLOSE
            line = (row_str + '\n').encode(encoding)
            add_header = False
            file.write(line)
        row_str = CSV_FIELD_CLOSE + f'{CSV_FIELD_CLOSE}{CSV_SPLIT}{CSV_FIELD_CLOSE}'.join(
            field_map.values()) + CSV_FIELD_CLOSE
        line = (row_str + '\n').encode(encoding)
        file.write(line)
