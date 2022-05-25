import os
import random


def get_random_user_agent():
    pwd = os.path.dirname(__file__)
    ua_list = open(f"{pwd}/pcua.txt").readlines()
    u = random.choice(ua_list).replace("\n", "")
    while u.strip() == "":
        u = random.choice(ua_list).replace("\n", "")
    return u.strip()


def get_firefox_user_agent():
    pwd = os.path.dirname(__file__)
    ua_list = open(f"{pwd}/pcua.txt").readlines()
    u = random.choice(ua_list).replace("\n", "")
    while u.strip() == "" and "Firefox" not in u:
        u = random.choice(ua_list).replace("\n", "")
    return u.strip()


if __name__ == '__main__':
    print(get_random_user_agent())
