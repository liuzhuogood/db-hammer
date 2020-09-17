import subprocess


def execute(cmd):
    p = subprocess.Popen(cmd,
                         shell=True,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        return p.returncode, stderr
    return p.returncode, stdout


def execute_get_code(cmd):
    return execute(cmd)[0]


def execute_get_sout(cmd):
    return str(execute(cmd)[1], 'utf-8')


if __name__ == '__main__':
    print(execute_get_sout("ls"))
