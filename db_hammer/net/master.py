# -*- coding: utf-8 -*-
# !/usr/bin/env python
# coding=utf-8
from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import time
import binascii
import struct
import collections
import logging
import socket
import functools
import threading
import traceback
import warnings
import os
import tempfile
import queue
import atexit

try:
    import selectors
    from selectors import EVENT_READ, EVENT_WRITE

    EVENT_READ_WRITE = EVENT_READ | EVENT_WRITE
except ImportError:
    import select

    warnings.warn('selectors module not available, fallback to select')
    selectors = None

try:
    import ssl
except ImportError:
    ssl = None
    warnings.warn('ssl module not available, ssl feature is disabled')

try:
    # for pycharm type hinting
    from typing import Union, Callable
except:
    pass

# socket recv buffer, 16384 bytes
RECV_BUFFER_SIZE = 2 ** 14

# default secretkey, use -k/--secretkey to change
SECRET_KEY = None  # "shootback"

# how long a SPARE slaver would keep
# once slaver received an heart-beat package from master,
#   the TTL would be reset. And heart-beat delay is less than TTL,
#   so, theoretically, spare slaver never timeout,
#   except network failure
# notice: working slaver would NEVER timeout
SPARE_SLAVER_TTL = 300

# internal program version, appears in CtrlPkg
INTERNAL_VERSION = 0x0013

# # how many packet are buffed, before delaying recv
# SOCKET_BRIDGE_SEND_BUFF_SIZE = 5

# version for human readable
__version__ = (2, 6, 1, INTERNAL_VERSION)

# just a logger
log = logging  # logging.getLogger(__name__)


def version_info():
    """get program version for human. eg: "2.1.0-r2" """
    return "{}.{}.{}-r{}".format(*__version__)


def configure_logging(level):
    logging.basicConfig(
        level=level,
        format='[%(levelname)s %(asctime)s] %(message)s',
    )


def fmt_addr(socket):
    """(host, int(port)) --> "host:port" """
    return "{}:{}".format(*socket)


def split_host(x):
    """ "host:port" --> (host, int(port))"""
    try:
        host, port = x.split(":")
        port = int(port)
    except:
        raise ValueError(
            "wrong syntax, format host:port is "
            "required, not {}".format(x))
    else:
        return host, port


def try_close(closable):
    """try close something

    same as
        try:
            connection.close()
        except:
            pass
    """
    try:
        closable.close()
    except:
        pass


def select_recv(conn, buff_size, timeout=None):
    """add timeout for socket.recv()
    :type conn: socket.socket
    :type buff_size: int
    :type timeout: float
    :rtype: Union[bytes, None]
    """
    if selectors:
        sel = selectors.DefaultSelector()
        sel.register(conn, EVENT_READ)
        events = sel.select(timeout)
        sel.close()
        if not events:
            # timeout
            raise RuntimeError("recv timeout")
    else:
        rlist, _, _ = select.select([conn], [], [], timeout)

    buff = conn.recv(buff_size)
    if not buff:
        raise RuntimeError("received zero bytes, socket was closed")

    return buff


def set_secretkey(key):
    global SECRET_KEY
    SECRET_KEY = key
    CtrlPkg.recalc_crc32()


class SocketBridge(object):
    """
    transfer data between sockets
    """

    def __init__(self):
        self.conn_rd = set()  # record readable-sockets
        self.conn_wr = set()  # record writeable-sockets
        self.map = {}  # record sockets pairs
        self.callbacks = {}  # record callbacks
        self.send_buff = {}  # buff one packet for those sending too-fast socket

        if selectors:
            self.sel = selectors.DefaultSelector()
        else:
            self.sel = None

    def add_conn_pair(self, conn1, conn2, callback=None):
        """
        transfer anything between two sockets

        :type conn1: socket.socket
        :type conn2: socket.socket
        :param callback: callback in connection finish
        :type callback: Callable
        """
        # change to non-blocking
        #   we use select or epoll to notice when data is ready
        conn1.setblocking(False)
        conn2.setblocking(False)

        # mark as readable+writable
        self.conn_rd.add(conn1)
        self.conn_wr.add(conn1)
        self.conn_rd.add(conn2)
        self.conn_wr.add(conn2)

        # record sockets pairs
        self.map[conn1] = conn2
        self.map[conn2] = conn1

        # record callback
        if callback is not None:
            self.callbacks[conn1] = callback

        if self.sel:
            self.sel.register(conn1, EVENT_READ_WRITE)
            self.sel.register(conn2, EVENT_READ_WRITE)

    def start_as_daemon(self):
        t = threading.Thread(target=self.start)
        t.daemon = True
        t.start()
        log.info("SocketBridge daemon started")
        return t

    def start(self):
        while True:
            try:
                self._start()
            except:
                log.error("FATAL ERROR! SocketBridge failed {}".format(
                    traceback.format_exc()
                ))

    def _start(self):

        while True:
            if not self.conn_rd and not self.conn_wr:
                # sleep if there is no connections
                time.sleep(0.01)
                continue

            # blocks until there is socket(s) ready for .recv
            # notice: sockets which were closed by remote,
            #   are also regarded as read-ready by select()
            if self.sel:
                events = self.sel.select(0.5)
                socks_rd = tuple(key.fileobj for key, mask in events if mask & EVENT_READ)
                socks_wr = tuple(key.fileobj for key, mask in events if mask & EVENT_WRITE)
            else:
                r, w, _ = select.select(self.conn_rd, self.conn_wr, [], 0.5)
                socks_rd = tuple(r)
                socks_wr = tuple(w)
                # log.debug('socks_rd: %s, socks_wr:%s', len(socks_rd), len(socks_wr))

            if not socks_rd and not self.send_buff:  # reduce CPU in low traffic
                time.sleep(0.005)
            # log.debug('got rd:%s wr:%s', socks_rd, socks_wr)

            # ----------------- RECEIVING ----------------
            # For prevent high CPU at slow network environment, we record if there is any
            #   success network operation, if we did nothing in single loop, we'll sleep a while.
            _stuck_network = True

            for s in socks_rd:  # type: socket.socket
                # if this socket has non-sent data, stop recving more, to prevent buff blowing up.
                if self.map[s] in self.send_buff:
                    # log.debug('delay recv because another too slow %s', self.map.get(s))
                    continue
                _stuck_network = False

                try:
                    received = s.recv(RECV_BUFFER_SIZE)
                    # log.debug('recved %s from %s', len(received), s)
                except Exception as e:
                    # ssl may raise SSLWantReadError or SSLWantWriteError
                    #   just continue and wait it complete
                    if ssl and isinstance(e, (ssl.SSLWantReadError, ssl.SSLWantWriteError)):
                        # log.warning('got %s, wait to read then', repr(e))
                        continue

                    # unable to read, in most cases, it's due to socket close
                    log.warning('error reading socket %s, %s closing', repr(e), s)
                    self._rd_shutdown(s)
                    continue

                if not received:
                    self._rd_shutdown(s)
                    continue
                else:
                    self.send_buff[self.map[s]] = received

            # ----------------- SENDING ----------------
            for s in socks_wr:
                if s not in self.send_buff:
                    if self.map.get(s) not in self.conn_rd:
                        self._wr_shutdown(s)
                    continue
                _stuck_network = False

                data = self.send_buff.pop(s)
                try:
                    s.send(data)
                    # log.debug('sent %s to %s', len(data), s)
                except Exception as e:
                    if ssl and isinstance(e, (ssl.SSLWantReadError, ssl.SSLWantWriteError)):
                        # log.warning('got %s, wait to write then', repr(e))
                        self.send_buff[s] = data  # write back for next write
                        continue
                    # unable to send, close connection
                    log.warning('error sending socket %s, %s closing', repr(e), s)
                    self._wr_shutdown(s)
                    continue

            if _stuck_network:  # slower at bad network
                time.sleep(0.001)

    def _sel_disable_event(self, conn, ev):
        try:
            _key = self.sel.get_key(conn)  # type:selectors.SelectorKey
        except KeyError:
            pass
        else:
            if _key.events == EVENT_READ_WRITE:
                self.sel.modify(conn, EVENT_READ_WRITE ^ ev)
            else:
                self.sel.unregister(conn)

    def _rd_shutdown(self, conn, once=False):
        """action when connection should be read-shutdown
        :type conn: socket.socket
        """
        if conn in self.conn_rd:
            self.conn_rd.remove(conn)
            if self.sel:
                self._sel_disable_event(conn, EVENT_READ)

        # if conn in self.send_buff:
        #     del self.send_buff[conn]

        try:
            conn.shutdown(socket.SHUT_RD)
        except:
            pass

        if not once and conn in self.map:  # use the `once` param to avoid infinite loop
            # if a socket is rd_shutdowned, then it's
            #   pair should be wr_shutdown.
            self._wr_shutdown(self.map[conn], True)

        if self.map.get(conn) not in self.conn_rd:
            # if both two connection pair was rd-shutdowned,
            #   this pair sockets are regarded to be completed
            #   so we gonna close them
            self._terminate(conn)

    def _wr_shutdown(self, conn, once=False):
        """action when connection should be write-shutdown
        :type conn: socket.socket
        """
        try:
            conn.shutdown(socket.SHUT_WR)
        except:
            pass

        if conn in self.conn_wr:
            self.conn_wr.remove(conn)
            if self.sel:
                self._sel_disable_event(conn, EVENT_WRITE)

        if not once and conn in self.map:  # use the `once` param to avoid infinite loop
            #   pair should be rd_shutdown.
            # if a socket is wr_shutdowned, then it's
            self._rd_shutdown(self.map[conn], True)

    def _terminate(self, conn, once=False):
        """terminate a sockets pair (two socket)
        :type conn: socket.socket
        :param conn: any one of the sockets pair
        """
        try_close(conn)  # close the first socket

        # ------ close and clean the mapped socket, if exist ------
        _another_conn = self.map.pop(conn, None)

        self.send_buff.pop(conn, None)
        if self.sel:
            try:
                self.sel.unregister(conn)
            except:
                pass

        # ------ callback --------
        # because we are not sure which socket are assigned to callback,
        #   so we should try both
        if conn in self.callbacks:
            try:
                self.callbacks[conn]()
            except Exception as e:
                log.error("traceback error: {}".format(e))
                log.debug(traceback.format_exc())
            del self.callbacks[conn]

        # terminate another
        if not once and _another_conn in self.map:
            self._terminate(_another_conn)


class CtrlPkg(object):
    """
    Control Packages of shootback, not completed yet
    current we have: handshake and heartbeat

    NOTICE: If you are non-Chinese reader,
        please contact me for the following Chinese comment's translation
        http://github.com/aploium

    控制包结构 总长64bytes      CtrlPkg.FORMAT_PKG
    使用 big-endian

    体积   名称        数据类型           描述
    1    pkg_ver    unsigned char       包版本  *1
    1    pkg_type    signed char        包类型 *2
    2    prgm_ver    unsigned short    程序版本 *3
    20    N/A          N/A              预留
    40    data        bytes           数据区 *4

    *1: 包版本. 包整体结构的定义版本, 目前只有 0x01

    *2: 包类型. 除心跳外, 所有负数包代表由Slaver发出, 正数包由Master发出
        -1: Slaver-->Master 的握手响应包       PTYPE_HS_S2M
         0: 心跳包                            PTYPE_HEART_BEAT
        +1: Master-->Slaver 的握手包          PTYPE_HS_M2S

    *3: 默认即为 INTERNAL_VERSION

    *4: 数据区中的内容由各个类型的包自身定义

    -------------- 数据区定义 ------------------
    包类型: -1 (Slaver-->Master 的握手响应包)
        体积   名称           数据类型         描述
         4    crc32_s2m   unsigned int     简单鉴权用 CRC32(Reversed(SECRET_KEY))
         1    ssl_flag   unsigned char     是否支持SSL
       其余为空
       *注意: -1握手包是把 SECRET_KEY 字符串翻转后取CRC32, +1握手包不预先反转

    包类型: 0 (心跳)
       数据区为空

    包理性: +1 (Master-->Slaver 的握手包)
        体积   名称           数据类型         描述
         4    crc32_m2s   unsigned int     简单鉴权用 CRC32(SECRET_KEY)
         1    ssl_flag   unsigned char     是否支持SSL
       其余为空

    """
    PACKAGE_SIZE = 2 ** 6  # 64 bytes
    CTRL_PKG_TIMEOUT = 5  # CtrlPkg recv timeout, in second

    # CRC32 for SECRET_KEY and Reversed(SECRET_KEY)
    #   these values are set by `set_secretkey`
    SECRET_KEY_CRC32 = None  # binascii.crc32(SECRET_KEY.encode('utf-8')) & 0xffffffff
    SECRET_KEY_REVERSED_CRC32 = None  # binascii.crc32(SECRET_KEY[::-1].encode('utf-8')) & 0xffffffff

    # Package Type
    PTYPE_HS_S2M = -1  # handshake pkg, slaver to master
    PTYPE_HEART_BEAT = 0  # heart beat pkg
    PTYPE_HS_M2S = +1  # handshake pkg, Master to Slaver

    TYPE_NAME_MAP = {
        PTYPE_HS_S2M: "PTYPE_HS_S2M",
        PTYPE_HEART_BEAT: "PTYPE_HEART_BEAT",
        PTYPE_HS_M2S: "PTYPE_HS_M2S",
    }

    # formats
    # see https://docs.python.org/3/library/struct.html#format-characters
    #   for format syntax
    FORMAT_PKG = b"!b b H 20x 40s"
    FORMATS_DATA = {
        PTYPE_HS_S2M: b"!I B 35x",
        PTYPE_HEART_BEAT: b"!40x",
        PTYPE_HS_M2S: b"!I B 35x",
    }

    SSL_FLAG_NONE = 0
    SSL_FLAG_AVAIL = 1

    def __init__(self, pkg_ver=0x01, pkg_type=0,
                 prgm_ver=INTERNAL_VERSION, data=(),
                 raw=None,
                 ):
        """do not call this directly, use `CtrlPkg.pbuild_*` instead"""
        self.pkg_ver = pkg_ver
        self.pkg_type = pkg_type
        self.prgm_ver = prgm_ver
        self.data = data
        if raw:
            self.raw = raw
        else:
            self._build_bytes()

    @property
    def type_name(self):
        """返回人类可读的包类型"""
        return self.TYPE_NAME_MAP.get(self.pkg_type, "TypeUnknown")

    def __str__(self):
        return """pkg_ver: {} pkg_type:{} prgm_ver:{} data:{}""".format(
            self.pkg_ver,
            self.type_name,
            self.prgm_ver,
            self.data,
        )

    def __repr__(self):
        return self.__str__()

    def _build_bytes(self):
        self.raw = struct.pack(
            self.FORMAT_PKG,
            self.pkg_ver,
            self.pkg_type,
            self.prgm_ver,
            self.data_encode(self.pkg_type, self.data),
        )

    @classmethod
    def recalc_crc32(cls):
        cls.SECRET_KEY_CRC32 = binascii.crc32(SECRET_KEY.encode('utf-8')) & 0xffffffff
        cls.SECRET_KEY_REVERSED_CRC32 = binascii.crc32(SECRET_KEY[::-1].encode('utf-8')) & 0xffffffff

    @classmethod
    def data_decode(cls, ptype, data_raw):
        return struct.unpack(cls.FORMATS_DATA[ptype], data_raw)

    @classmethod
    def data_encode(cls, ptype, data):
        return struct.pack(cls.FORMATS_DATA[ptype], *data)

    def verify(self, pkg_type=None):
        try:
            if pkg_type is not None and self.pkg_type != pkg_type:
                return False
            elif self.pkg_type == self.PTYPE_HS_S2M:
                # Slaver-->Master 的握手响应包
                return self.data[0] == self.SECRET_KEY_REVERSED_CRC32

            elif self.pkg_type == self.PTYPE_HEART_BEAT:
                # 心跳
                return True

            elif self.pkg_type == self.PTYPE_HS_M2S:
                # Master-->Slaver 的握手包
                return self.data[0] == self.SECRET_KEY_CRC32

            else:
                return True
        except:
            return False

    @classmethod
    def decode_only(cls, raw):
        """
        decode raw bytes to CtrlPkg instance, no verify
        use .decode_verify() if you also want verify

        :param raw: raw bytes content of package
        :type raw: bytes
        :rtype: CtrlPkg
        """
        if not raw or len(raw) != cls.PACKAGE_SIZE:
            raise ValueError("content size should be {}, but {}".format(
                cls.PACKAGE_SIZE, len(raw)
            ))
        pkg_ver, pkg_type, prgm_ver, data_raw = struct.unpack(cls.FORMAT_PKG, raw)
        data = cls.data_decode(pkg_type, data_raw)

        return cls(
            pkg_ver=pkg_ver, pkg_type=pkg_type,
            prgm_ver=prgm_ver,
            data=data,
            raw=raw,
        )

    @classmethod
    def decode_verify(cls, raw, pkg_type=None):
        """decode and verify a package
        :param raw: raw bytes content of package
        :type raw: bytes
        :param pkg_type: assert this package's type,
            if type not match, would be marked as wrong
        :type pkg_type: int

        :rtype: CtrlPkg, bool
        :return: tuple(CtrlPkg, is_it_a_valid_package)
        """
        try:
            pkg = cls.decode_only(raw)
        except:
            log.error('unable to decode package. raw: %s', raw, exc_info=True)
            return None, False
        else:
            return pkg, pkg.verify(pkg_type=pkg_type)

    @classmethod
    def pbuild_hs_m2s(cls, ssl_avail=False):
        """pkg build: Handshake Master to Slaver
        """
        ssl_flag = cls.SSL_FLAG_AVAIL if ssl_avail else cls.SSL_FLAG_NONE
        return cls(
            pkg_type=cls.PTYPE_HS_M2S,
            data=(cls.SECRET_KEY_CRC32, ssl_flag),
        )

    @classmethod
    def pbuild_hs_s2m(cls, ssl_avail=False):
        """pkg build: Handshake Slaver to Master"""
        ssl_flag = cls.SSL_FLAG_AVAIL if ssl_avail else cls.SSL_FLAG_NONE
        return cls(
            pkg_type=cls.PTYPE_HS_S2M,
            data=(cls.SECRET_KEY_REVERSED_CRC32, ssl_flag),
        )

    @classmethod
    def pbuild_heart_beat(cls):
        """pkg build: Heart Beat Package"""
        return cls(
            pkg_type=cls.PTYPE_HEART_BEAT,
        )

    @classmethod
    def recv(cls, sock, timeout=CTRL_PKG_TIMEOUT, expect_ptype=None):
        """just a shortcut function
        :param sock: which socket to recv CtrlPkg from
        :type sock: socket.socket
        :rtype: CtrlPkg,bool
        """
        buff = select_recv(sock, cls.PACKAGE_SIZE, timeout)
        pkg, verify = CtrlPkg.decode_verify(buff, pkg_type=expect_ptype)  # type: CtrlPkg,bool
        return pkg, verify


_listening_sockets = []  # for close at exit
__author__ = "Aploium <i@z.codes>"
__website__ = "https://github.com/aploium/shootback"

_DEFAULT_SSL_KEY = '''\
-----BEGIN PRIVATE KEY-----
MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAMqPYWmoQJaHwu3/
0Q5R8RFpOzFwys/5xgypGMbFmW0oSpiVMA0u+lakxb7Z46RU/Y/kV4XUjB9+uDcI
FlU520jWJVZ9g09gMKaPyDjxdKueKl2KDfn7i4unBQwouqzipPWXv0L8RnHF1gzG
XgN/Dh6+5ZoY53JvUZaFbQhjjtkjAgMBAAECgYB8WRjL6+X6gs0/ndOQnu0GaztT
VpKqqgLSstvq6lMNl7ZzhOJCtZwopG5ggxIkR6iBNQQlvB1pGDmuTuCm4SWjsXsS
x2iDvPTlx9Y1ke9SWszZ6mOvumeHLnnxU0tp+ECySphQN5XxzH2yHzeXVTWpIim6
ADFrtbltaPFLghZbQQJBAOT0tU+QLtlaKit6iY1QrZntsVzeCZYxh2ktz4Hsk8RN
K4U4FCoWz3rJrkj1gFIrgoSgQ6+VzjXzFDbKJeP7b38CQQDifIBhi9JAVKlPgklG
e595EYa3J4a601AAIxYVVwyfn8CiUUHCHKM+roJywh0uvKVXN6sn1Wi4zU8H8BQ1
9KhdAkAnIY/PfmQTb+6fKb1SssRI97AFoElhKyvqlRLPMOD8fvf+N9xyaR2i7c9k
1tjMsnUHN+D5pI/u9pGw35HkSjf/AkB4rpCV6bQhtTr2c9zpoqvKDi2zYGtpF3oU
aJ22x0ihsbUqiJO6hBn0J3a5AXgdVEXh4Hbh5dRETJnlB+ctDO29AkBXDgUXbltm
CRtey5ZwnlAKI8jwx3q4jaldTCJrwThfgNdlvWPnKjPhIYG/OogLAm82grruSzOQ
a2xDsGiXCZoM
-----END PRIVATE KEY-----
'''
_DEFAULT_SSL_CERT = '''\
-----BEGIN CERTIFICATE-----
MIICmzCCAgSgAwIBAgIJANe4OK2h+u9iMA0GCSqGSIb3DQEBCwUAMGUxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApTb21lLVN0YXRlMS0wKwYDVQQKDCRodHRwczovL2dp
dGh1Yi5jb20vYXBsb2l1bS9zaG9vdGJhY2sxEjAQBgNVBAMMCWxvY2FsaG9zdDAe
Fw0xODEwMjQxNjA5MDVaFw0yODEwMjExNjA5MDVaMGUxCzAJBgNVBAYTAlVTMRMw
EQYDVQQIDApTb21lLVN0YXRlMS0wKwYDVQQKDCRodHRwczovL2dpdGh1Yi5jb20v
YXBsb2l1bS9zaG9vdGJhY2sxEjAQBgNVBAMMCWxvY2FsaG9zdDCBnzANBgkqhkiG
9w0BAQEFAAOBjQAwgYkCgYEAyo9haahAlofC7f/RDlHxEWk7MXDKz/nGDKkYxsWZ
bShKmJUwDS76VqTFvtnjpFT9j+RXhdSMH364NwgWVTnbSNYlVn2DT2Awpo/IOPF0
q54qXYoN+fuLi6cFDCi6rOKk9Ze/QvxGccXWDMZeA38OHr7lmhjncm9RloVtCGOO
2SMCAwEAAaNTMFEwHQYDVR0OBBYEFEJESWZQ80wg2+1HmLW8nFtYyg9NMB8GA1Ud
IwQYMBaAFEJESWZQ80wg2+1HmLW8nFtYyg9NMA8GA1UdEwEB/wQFMAMBAf8wDQYJ
KoZIhvcNAQELBQADgYEAJD3ZWeLJaxWyCVSsKSxFZfTyYMhKSI9UjssDU48ENbfz
hTdU2KxHFmRlTIdl02BcvpjiCHOCYxGkSBXctpXAHp9oU8fpzNIdekmgmZD2GjHS
NbE3PCsO3NNtnc3Oj96HCww2MeC0ro9j0uDkyl+0zx47UeFUDqqSFJZ3bQa8j0w=
-----END CERTIFICATE-----
'''


@atexit.register
def close_listening_socket_at_exit():
    log.info("exiting...")
    for s in _listening_sockets:
        log.info("closing: {}".format(s))
        try_close(s)


def try_bind_port(sock, addr):
    while True:
        try:
            sock.bind(addr)
        except Exception as e:
            log.error(("unable to bind {}, {}. If this port was used by the recently-closed shootback itself\n"
                       "then don't worry, it would be available in several seconds\n"
                       "we'll keep trying....").format(addr, e))
            log.debug(traceback.format_exc())
            time.sleep(3)
        else:
            break


class Master(object):
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 slaver_pool=None, working_pool=None,
                 ssl=False
                 ):
        """

        :param customer_listen_addr: equals to the -c/--customer param
        :param communicate_addr: equals to the -m/--master param
        """
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}

        self.working_pool = working_pool or {}

        self.socket_bridge = SocketBridge()

        # a queue for customers who have connected to us,
        #   but not assigned a slaver yet
        self.pending_customers = queue.Queue()

        if ssl:
            self.ssl_context = self._make_ssl_context()
            self.ssl_avail = self.ssl_context is not None
        else:
            self.ssl_avail = False
            self.ssl_context = None

        self.communicate_addr = communicate_addr

        _fmt_communicate_addr = fmt_addr(self.communicate_addr)

        if slaver_pool:
            # 若使用外部slaver_pool, 就不再初始化listen
            # 这是以后待添加的功能
            self.external_slaver = True
            self.thread_pool["listen_slaver"] = None
        else:
            # 自己listen来获取slaver
            self.external_slaver = False
            self.slaver_pool = collections.deque()
            # prepare Thread obj, not activated yet
            self.thread_pool["listen_slaver"] = threading.Thread(
                target=self._listen_slaver,
                name="listen_slaver-{}".format(_fmt_communicate_addr),
                daemon=True,
            )

        # prepare Thread obj, not activated yet
        self.customer_listen_addr = customer_listen_addr
        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare Thread obj, not activated yet
        self.thread_pool["heart_beat_daemon"] = threading.Thread(
            target=self._heart_beat_daemon,
            name="heart_beat_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare assign_slaver_daemon
        self.thread_pool["assign_slaver_daemon"] = threading.Thread(
            target=self._assign_slaver_daemon,
            name="assign_slaver_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

    def serve_forever(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["heart_beat_daemon"].start()
        self.thread_pool["listen_customer"].start()
        self.thread_pool["assign_slaver_daemon"].start()
        self.thread_pool["socket_bridge"] = self.socket_bridge.start_as_daemon()

        while True:
            time.sleep(10)

    def _make_ssl_context(self):
        if ssl is None:
            log.warning('ssl module is NOT valid in this machine! Fallback to plain')
            return None

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
        ctx.verify_mode = ssl.CERT_NONE

        _certfile = tempfile.mktemp()
        with open(_certfile, 'w') as fw:
            fw.write(_DEFAULT_SSL_CERT)
        _keyfile = tempfile.mktemp()
        with open(_keyfile, 'w') as fw:
            fw.write(_DEFAULT_SSL_KEY)
        ctx.load_cert_chain(_certfile, _keyfile)
        os.remove(_certfile)
        os.remove(_keyfile)

        return ctx

    def _transfer_complete(self, addr_customer):
        """a callback for SocketBridge, do some cleanup jobs"""
        log.info("customer complete: {}".format(addr_customer))
        del self.working_pool[addr_customer]

    def _serve_customer(self, conn_customer, conn_slaver):
        """put customer and slaver sockets into SocketBridge, let them exchange data"""
        self.socket_bridge.add_conn_pair(
            conn_customer, conn_slaver,
            functools.partial(  # it's a callback
                # 这个回调用来在传输完成后删除工作池中对应记录
                self._transfer_complete,
                conn_customer.getpeername()
            )
        )

    @staticmethod
    def _send_heartbeat(conn_slaver):
        """send and verify heartbeat pkg"""
        conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        pkg, verify = CtrlPkg.recv(
            conn_slaver, expect_ptype=CtrlPkg.PTYPE_HEART_BEAT)  # type: CtrlPkg,bool

        if not verify:
            return False

        if pkg.prgm_ver < 0x000B:
            # shootback before 2.2.5-r10 use two-way heartbeat
            #   so there is no third pkg to send
            pass
        else:
            # newer version use TCP-like 3-way heartbeat
            #   the older 2-way heartbeat can't only ensure the
            #   master --> slaver pathway is OK, but the reverse
            #   communicate may down. So we need a TCP-like 3-way
            #   heartbeat
            conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        return verify

    def _heart_beat_daemon(self):
        """

        每次取出slaver队列头部的一个, 测试心跳, 并把它放回尾部.
            slaver若超过 SPARE_SLAVER_TTL 秒未收到心跳, 则会自动重连
            所以睡眠间隔(delay)满足   delay * slaver总数  < TTL
            使得一轮循环的时间小于TTL,
            保证每个slaver都在过期前能被心跳保活
        """
        default_delay = 5 + SPARE_SLAVER_TTL // 12
        delay = default_delay
        log.info("heart beat daemon start, delay: {}s".format(delay))
        while True:
            time.sleep(delay)
            # log.debug("heart_beat_daemon: hello! im weak")

            # ---------------------- preparation -----------------------
            slaver_count = len(self.slaver_pool)
            if not slaver_count:
                log.warning("heart_beat_daemon: sorry, no slaver available, keep sleeping")
                # restore default delay if there is no slaver
                delay = default_delay
                continue
            else:
                # notice this `slaver_count*2 + 1`
                # slaver will expire and re-connect if didn't receive
                #   heartbeat pkg after SPARE_SLAVER_TTL seconds.
                # set delay to be short enough to let every slaver receive heartbeat
                #   before expire
                delay = 1 + SPARE_SLAVER_TTL // max(slaver_count * 2 + 1, 12)

            # pop the oldest slaver
            #   heartbeat it and then put it to the end of queue
            slaver = self.slaver_pool.popleft()
            addr_slaver = slaver["addr_slaver"]

            # ------------------ real heartbeat begin --------------------
            start_time = time.perf_counter()
            try:
                hb_result = self._send_heartbeat(slaver["conn_slaver"])
            except Exception as e:
                log.warning("error during heartbeat to {}: {}".format(
                    fmt_addr(addr_slaver), e))
                log.debug(traceback.format_exc())
                hb_result = False
            finally:
                time_used = round((time.perf_counter() - start_time) * 1000.0, 2)
            # ------------------ real heartbeat end ----------------------

            if not hb_result:
                log.warning("heart beat failed: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                try_close(slaver["conn_slaver"])
                del slaver["conn_slaver"]

                # if heartbeat failed, start the next heartbeat immediately
                #   because in most cases, all 5 slaver connection will
                #   fall and re-connect in the same time
                delay = 0

            else:
                log.debug("heartbeat success: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                self.slaver_pool.append(slaver)

    def _handshake(self, conn_slaver):
        """
        handshake before real data transfer
        it ensures:
            1. client is alive and ready for transmission
            2. client is shootback_slaver, not mistakenly connected other program
            3. verify the SECRET_KEY, establish SSL
            4. tell slaver it's time to connect target

        handshake procedure:
            1. master hello --> slaver
            2. slaver verify master's hello
            3. slaver hello --> master
            4. (immediately after 3) slaver connect to target
            4. master verify slaver
            5. [optional] establish SSL
            6. enter real data transfer

        Args:
            conn_slaver (socket.socket)
        Return:
            socket.socket|ssl.SSLSocket: socket obj(may be ssl-socket) if handshake success, else None
        """
        conn_slaver.send(CtrlPkg.pbuild_hs_m2s(ssl_avail=self.ssl_avail).raw)

        buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, 2)
        if buff is None:
            return None

        pkg, correct = CtrlPkg.decode_verify(buff, CtrlPkg.PTYPE_HS_S2M)  # type: CtrlPkg,bool

        if not correct:
            return None

        if not self.ssl_avail or pkg.data[1] == CtrlPkg.SSL_FLAG_NONE:
            if self.ssl_avail:
                log.warning('client %s not enabled SSL, fallback to plain.', conn_slaver.getpeername())
            return conn_slaver
        else:
            ssl_conn_slaver = self.ssl_context.wrap_socket(conn_slaver, server_side=True)  # type: ssl.SSLSocket
            log.debug('ssl established slaver: %s', ssl_conn_slaver.getpeername())
            return ssl_conn_slaver

    def _get_an_active_slaver(self):
        """get and activate an slaver for data transfer"""
        try_count = 100
        while True:
            if not try_count:
                return None
            try:
                dict_slaver = self.slaver_pool.popleft()
            except:
                time.sleep(0.02)
                try_count -= 1
                if try_count % 10 == 0:
                    log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))
                continue

            conn_slaver = dict_slaver["conn_slaver"]

            try:
                # this returned conn may be ssl-socket or plain socket
                actual_conn = self._handshake(conn_slaver)
            except Exception as e:
                log.warning("Handshake failed. %s %s", dict_slaver["addr_slaver"], e)
                log.debug(traceback.format_exc())
                actual_conn = None

                try_count -= 1
                if try_count % 10 == 0:
                    log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))

            if actual_conn is not None:
                return actual_conn
            else:
                log.warning("slaver handshake failed: %s", dict_slaver["addr_slaver"])
                try_close(conn_slaver)

                time.sleep(0.02)

    def _assign_slaver_daemon(self):
        """assign slaver for customer"""
        while True:
            # get a newly connected customer
            conn_customer, addr_customer = self.pending_customers.get()

            try:
                conn_slaver = self._get_an_active_slaver()
            except:
                log.error('error in getting slaver', exc_info=True)
                continue
            if conn_slaver is None:
                log.warning("Closing customer[%s] because no available slaver found", addr_customer)
                try_close(conn_customer)

                continue
            else:
                log.debug("Using slaver: %s for %s", conn_slaver.getpeername(), addr_customer)

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer,
                "conn_slaver": conn_slaver,
            }

            try:
                self._serve_customer(conn_customer, conn_slaver)
            except:
                log.error('error adding to socket_bridge', exc_info=True)
                try_close(conn_customer)
                try_close(conn_slaver)
                continue

    def _listen_slaver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_bind_port(sock, self.communicate_addr)
        sock.listen(10)
        _listening_sockets.append(sock)
        log.info("Listening for slavers: {}".format(
            fmt_addr(self.communicate_addr)))
        while True:
            conn, addr = sock.accept()
            self.slaver_pool.append({
                "addr_slaver": addr,
                "conn_slaver": conn,
            })
            log.info("Got slaver {} Total: {}".format(
                fmt_addr(addr), len(self.slaver_pool)
            ))

    def _listen_customer(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_bind_port(sock, self.customer_listen_addr)
        sock.listen(20)
        _listening_sockets.append(sock)
        log.info("Listening for customers: {}".format(
            fmt_addr(self.customer_listen_addr)))
        while True:
            conn_customer, addr_customer = sock.accept()
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, self.pending_customers.qsize() + 1
            ))

            # just put it into the queue,
            #   let _assign_slaver_daemon() do the else
            #   don't block this loop
            self.pending_customers.put((conn_customer, addr_customer))


def run_master(communicate_addr, customer_listen_addr, secretkey, ssl=False):
    if isinstance(communicate_addr, str):
        communicate_addr = (communicate_addr.split(":")[0], int(communicate_addr.split(":")[1]))
    if isinstance(customer_listen_addr, str):
        customer_listen_addr = (customer_listen_addr.split(":")[0], int(customer_listen_addr.split(":")[1]))
    log.info("shootback {} running as master".format(version_info()))
    log.info("author: {}  site: {}".format(__author__, __website__))
    log.info("slaver from: {} customer from: {}".format(
        fmt_addr(communicate_addr), fmt_addr(customer_listen_addr)))
    set_secretkey(secretkey)
    Master(customer_listen_addr, communicate_addr, ssl=ssl).serve_forever()


def argparse_master():
    import argparse
    parser = argparse.ArgumentParser(
        description="""shootback (master) {ver}
A fast and reliable reverse TCP tunnel. (this is master)
Help access local-network service from Internet.
https://github.com/aploium/shootback""".format(ver=version_info()),
        epilog="""
Example1:
tunnel local ssh to public internet, assume master's ip is 1.2.3.4
  Master(this pc):                master.py -m 0.0.0.0:10000 -c 0.0.0.0:10022
  Slaver(another private pc):     slaver.py -m 1.2.3.4:10000 -t 127.0.0.1:22
  Customer(any internet user):    ssh 1.2.3.4 -p 10022
  the actual traffic is:  customer <--> master(1.2.3.4 this pc) <--> slaver(private network) <--> ssh(private network)

Example2:
Tunneling for www.example.com
  Master(this pc):                master.py -m 127.0.0.1:10000 -c 127.0.0.1:10080
  Slaver(this pc):                slaver.py -m 127.0.0.1:10000 -t example.com:80
  Customer(this pc):              curl -v -H "host: example.com" 127.0.0.1:10080

Tips: ANY service using TCP is shootback-able.  HTTP/FTP/Proxy/SSH/VNC/...
""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-m", "--master", required=True,
                        metavar="host:port",
                        help="listening for slavers, usually an Public-Internet-IP. Slaver comes in here  eg: 2.3.3.3:10000")
    parser.add_argument("-c", "--customer", required=True,
                        metavar="host:port",
                        help="listening for customers, 3rd party program connects here  eg: 10.1.2.3:10022")
    parser.add_argument("-k", "--secretkey", default="shootback",
                        help="secretkey to identity master and slaver, should be set to the same value in both side")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="verbose output")
    parser.add_argument("-q", "--quiet", action="count", default=0,
                        help="quiet output, only display warning and errors, use two to disable output")
    parser.add_argument("-V", "--version", action="version", version="shootback {}-master".format(version_info()))
    parser.add_argument("--ttl", default=300, type=int, dest="SPARE_SLAVER_TTL",
                        help="standing-by slaver's TTL, default is 300. "
                             "In master side, this value affects heart-beat frequency. "
                             "Default value is optimized for most cases")
    parser.add_argument('--ssl', action='store_true', help='[experimental] try using ssl for data encryption. '
                                                           'It may be enabled by default in future version')

    return parser.parse_args()


def main_master():
    global SPARE_SLAVER_TTL

    args = argparse_master()

    if args.verbose and args.quiet:
        print("-v and -q should not appear together")
        exit(1)

    communicate_addr = split_host(args.master)
    customer_listen_addr = split_host(args.customer)

    set_secretkey(args.secretkey)

    SPARE_SLAVER_TTL = args.SPARE_SLAVER_TTL
    if args.quiet < 2:
        if args.verbose:
            level = logging.DEBUG
        elif args.quiet:
            level = logging.WARNING
        else:
            level = logging.INFO
        configure_logging(level)

    run_master(communicate_addr, customer_listen_addr, ssl=args.ssl)


if __name__ == '__main__':
    run_master("0.0.0.0:9999", "0.0.0.0:8888", secretkey="abc123")
