##标签计算

特征价值分主要解决以下痛点问题
   * 特征覆盖度、稳定性、分布不知
   * 不知特定场景是否可用，缺乏可信度

特征价值分有以下几个特点：
   * 多维度(质量、重要性、应用)
   * 多场景(营销、风控、算法)
   * 多评价指标(相关系数、IV值)


## 场景表定义

```
uid -- 用户id, 场景下uid唯一
label -- 正负定义，坏1 好0
feature_date -- 所需特征日期
role -- 角色 乘客1， 司机2
```

## 调用方式
```
spark-submit --conf spark.driver.memory=12g \
    --conf spark.executor.memory=12g \
    --conf spark.executor.memoryOverhead=2048 \
    --conf spark.executor.cores=2 \
    --conf spark.dynamicAllocation.maxExecutors=500 \
    --conf spark.dynamicAllocation.minExecutors=30 \
    --conf spark.yarn.queue=xxx \
    --conf spark.driver.extraJavaOptions=-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.F2jBLAS \
    --conf spark.executor.extraJavaOptions=-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.F2jBLAS \
    --class om.github.skycrab.asset.job.BootApplication \
    asset-job-1.0-SNAPSHOT.jar  db.table year=2021/month=01/day=03 [] [integrity] false
 ```

