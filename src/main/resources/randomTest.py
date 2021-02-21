#coding=utf8

import random
import string

def row(row):
    return ",".join(map(str, row)) + "\n"

def run():
    headers = ["uid", "age", "name", "last_login_time"]
    with open("test.csv", "w") as f:
        f.write(row(headers))
        for uid in range(1, 5001):
            #age 90%概率非空
            if random.random() > 0.1:
                if random.random() > 0.1:
                    age = random.randint(1,100)
                else:
                    # 10%的概率为缺失值
                    age = random.sample([-1, -999, -9999], 1)[0]
            else:
                age = ""
            #name 80%概率非空
            if random.random() > 0.1:
                name = ''.join(random.sample(string.ascii_letters + string.digits, random.randint(2,8)))
            else:
                name = ""

            if random.random() > 0.5:
                last_login_time = random.sample(['2020-09-01', '2020-09-02', '2020-09-20', '2020-09-25'], 1)[0]
            else:
                last_login_time = random.sample(['', '1970-01-01'], 1)[0]

            f.write(row([uid, age, name, last_login_time]))



if __name__ == "__main__":
    run()