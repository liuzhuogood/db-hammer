import socket


def is_inuse(ip, port):
    """端口是否被占用"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except:
        return False


def get_random_port(ip, port):
    """根据IP获取一个随机端口（15000~20000）"""
    import random
    times = 0
    max_times = 50
    if port == 0:
        port = random.randint(15000, 20000)
    while is_inuse(ip, port) and times < max_times:
        port = random.randint(15000, 20000)
        times += 1
    if times > max_times:
        Exception("端口号获取失败")
    return port


def get_pc_name_ip(host):
    """获取当前IP与主机名 返回:(ip,name)"""
    name = socket.getfqdn(socket.gethostname())
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host.split(":")[0], int(host.split(":")[1])))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return name, ip


def recv_end(the_socket, SOCKET_END_TAG):
    """通过寻找接收的协议数据中的尾标识字符串，获取完整的数据的数据报文"""
    total_data = []
    while True:
        data = the_socket.recv(8192)
        if SOCKET_END_TAG in data:
            total_data.append(data[:data.find(SOCKET_END_TAG)])
            break
        total_data.append(data)
        if len(total_data) > 1:
            # check if end_of_data was split
            last_pair = total_data[-2] + total_data[-1]
            if SOCKET_END_TAG in last_pair:
                total_data[-2] = last_pair[:last_pair.find(SOCKET_END_TAG)]
                total_data.pop()
                break
    if len(total_data) == 0:
        return None
    return b''.join(total_data)
