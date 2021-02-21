#coding=utf8

import random
import string

def row(row):
    return ",".join(map(str, row)) + "\n"

def run():
    headers = ["uid", "label", "feature_date", "role"]
    with open("scene.csv", "w") as f:
        #f.write(row(headers))
        for uid in range(1, 3001):
            # 20%好人
            if random.random() > 0.2:
                label = 1
            else:
                label = 0

            feature_date = random.sample(['2020-07-12', '2020-07-11'], 1)[0]

            role = random.sample(['1', '2'], 1)[0]

            f.write(row([uid, label, feature_date, role]))


if __name__ == "__main__":
    run()