import os
import time
import paramiko


def sh(cmd, host, username='root', password='', port=22):
    # print("ssh", host, "==>", cmd)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, port=port, username=username, password=password)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    res, err = stdout.read(), stderr.read()
    result = res if res else err
    # print("<==", result.decode())
    time.sleep(0.5)
    ssh.close()
    return result.decode()


def command(cmd, host, username='root', password='', nohup=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, port=22, username=username,
                password=password)
    if nohup:
        cmd += ' & \n '
        invoke = ssh.invoke_shell()
        invoke.send(cmd)
        # 等待命令执行完成
        time.sleep(2)
    else:
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read()
        return result
    ssh.close()


def scp_put(local_file, remote_file, host, username='root', password=''):
    sh(cmd=f"mkdir -p {os.path.dirname(remote_file)}", host=host, username=username, password=password)
    transport = paramiko.Transport((host, 22))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(local_file, remote_file)
    transport.close()


def scp_get(local_file, remote_file, host, username='root', password=''):
    transport = paramiko.Transport((host, 22))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get(remote_file, local_file)
    transport.close()
