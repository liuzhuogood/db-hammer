import os
import re


def get_dir_files(path: str, mode='F', absolute=False, filter_re=""):
    """获取目录下所有文件
    :param path: 获取路径
    :arg mode
        F   当前目录下所有文件
        D   当前目录下所有文件夹
        FD  当前目录下所有文件和目录
        AD  当前目录和子目录
        AF  当前文件和子目录文件
        AFD 当前目录的子目录所有目录和文件
    :arg absolute 是否全路径
    :arg filter_re 正则正则
    """
    if not os.path.exists(path):
        raise Exception("文件目录不存在:" + path)

    if not os.path.isdir(path):
        raise Exception("不是目录：" + path)
    aa = []
    if mode == "F" or mode == "D" or mode == "FD":
        for root, dirs, files in os.walk(top=path, topdown=False):
            if mode == "F":
                if absolute:
                    ff = []
                    for f in files:
                        if root == path and re.match(filter_re, os.path.join(root, f)) is not None:
                            ff.append(os.path.join(root, f))
                    files = ff
                aa += files
            if mode == "D":
                if os.path.isdir(root) and re.match(filter_re, root) is not None:
                    aa.append(root)
            if mode == "FD" and re.match(filter_re, root) is not None:
                aa.append(root)
        return aa
    if mode == "AF" or mode == "AD" or mode == "AFD":
        for root, dirs, files in os.walk(top=path, topdown=True):
            if mode == "AF":
                if absolute:
                    ff = []
                    for f in files:
                        if re.match(filter_re, os.path.join(root, f)) is not None:
                            ff.append(os.path.join(root, f))
                    files = ff
                aa += files
            if mode == "AD":
                if os.path.isdir(root):
                    if re.match(filter_re, root) is not None:
                        aa.append(root)
            if mode == "AFD":
                if re.match(filter_re, root) is not None:
                    aa.append(root)
        return aa


if __name__ == '__main__':
    print(get_dir_files(path="/Users/lawman/", absolute=True, mode="F", filter_re=".*msi$"))
    # print(re.match("", "1234456"))
