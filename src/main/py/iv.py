import numpy as np 
import pandas as pd 
import math 

from sklearn.model_selection import train_test_split
from sklearn import metrics 
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeClassifier,_tree

from pyspark.sql.session import SparkSession
from pyspark.sql import Row

import warnings
warnings.filterwarnings('ignore')



def tree_split(df,col,target,max_bin,min_binpct,nan_value):
    """
    决策树分箱
    param:
        df -- 数据集 Dataframe
        col -- 分箱的字段名 string
        target -- 标签的字段名 string
        max_bin -- 最大分箱数 int
        min_binpct -- 箱体的最小占比 float
        nan_value -- 缺失的映射值 int/float
    return:
        split_list -- 分割点 list
    """
    miss_value_rate = df[df[col]==nan_value].shape[0]/df.shape[0]
    print("miss_value_rate:" + miss_value_rate)
    # 如果缺失占比小于5%，则直接对特征进行分箱
    if miss_value_rate<0.05:
        x = np.array(df[col]).reshape(-1,1)
        y = np.array(df[target])
        tree = DecisionTreeClassifier(max_leaf_nodes=max_bin,
                                  min_samples_leaf = min_binpct)
        tree.fit(x,y)
        thresholds = tree.tree_.threshold
        thresholds = thresholds[thresholds!=_tree.TREE_UNDEFINED]
        split_list = sorted(thresholds.tolist())
    # 如果缺失占比大于5%，则把缺失单独分为一箱，剩余部分再进行决策树分箱
    else:
        max_bin2 = max_bin-1
        x = np.array(df[~(df[col]==nan_value)][col]).reshape(-1,1)
        y = np.array(df[~(df[col]==nan_value)][target])
        tree = DecisionTreeClassifier(max_leaf_nodes=max_bin2,
                                  min_samples_leaf = min_binpct)
        tree.fit(x,y)
        thresholds = tree.tree_.threshold
        thresholds = thresholds[thresholds!=_tree.TREE_UNDEFINED]
        split_list = sorted(thresholds.tolist())
        split_list.insert(0,nan_value)
    
    return split_list


def quantile_split(df,col,target,max_bin,nan_value):
    """
    等频分箱
    param:
        df -- 数据集 Dataframe
        col -- 分箱的字段名 string
        target -- 标签的字段名 string
        max_bin -- 最大分箱数 int
        nan_value -- 缺失的映射值 int/float
    return:
        split_list -- 分割点 list
    """
    miss_value_rate = df[df[col]==nan_value].shape[0]/df.shape[0]
    
    # 如果缺失占比小于5%，则直接对特征进行分箱
    if miss_value_rate<0.05:
        bin_series,bin_cut = pd.qcut(df[col],q=max_bin,duplicates='drop',retbins=True)
        split_list = bin_cut.tolist()
        split_list.remove(split_list[0])
    # 如果缺失占比大于5%，则把缺失单独分为一箱，剩余部分再进行等频分箱
    else:
        df2 = df[~(df[col]==nan_value)]
        max_bin2 = max_bin-1
        bin_series,bin_cut = pd.qcut(df2[col],q=max_bin2,duplicates='drop',retbins=True)
        split_list = bin_cut.tolist()
        split_list[0] = nan_value
        
    split_list.remove(split_list[-1])
    
    # 当出现某个箱体只有好用户或只有坏用户时，进行前向合并箱体
    var_arr = np.array(df[col])
    target_arr = np.array(df[target])
    bin_trans = np.digitize(var_arr,split_list,right=True)
    var_tuple = [(x,y) for x,y in zip(bin_trans,target_arr)]
    
    delete_cut_list = []
    for i in set(bin_trans):
        target_list = [y for x,y in var_tuple if x==i]
        if target_list.count(1)==0 or target_list.count(0)==0:
            if i ==min(bin_trans):
                index=i
            else:
                index = i-1
            delete_cut_list.append(split_list[index])
    split_list = [x for x in split_list if x not in delete_cut_list]
    
    return split_list


def binning_var(df,col,target,bin_type='dt',max_bin=5,min_binpct=0.05,nan_value=-999):
    """
    特征分箱，计算iv
    param:
        df -- 数据集 Dataframe
        col -- 分箱的字段名 string
        target -- 标签的字段名 string
        bin_type -- 分箱方式 默认是'dt',还有'quivantile'(等频分箱)
        max_bin -- 最大分箱数 int
        min_binpct -- 箱体的最小占比 float
        nan_value -- 缺失映射值 int/float
    return:
        bin_df -- 特征的分箱明细表 Dataframe
        cut -- 分割点 list
    """
    total = df[target].count()
    bad = df[target].sum()
    good = total-bad
    
    # 离散型特征分箱,直接根据类别进行groupby
    if df[col].dtype == np.dtype('object') or df[col].dtype == np.dtype('bool') or df[col].nunique()<=max_bin:
        group = df.groupby([col],as_index=True)
        bin_df = pd.DataFrame()

        bin_df['total'] = group[target].count()
        bin_df['totalrate'] = bin_df['total']/total
        bin_df['bad'] = group[target].sum()
        bin_df['badrate'] = bin_df['bad']/bin_df['total']
        bin_df['good'] = bin_df['total'] - bin_df['bad']
        bin_df['goodrate'] = bin_df['good']/bin_df['total']
        bin_df['badattr'] = bin_df['bad']/bad
        bin_df['goodattr'] = (bin_df['total']-bin_df['bad'])/good
        bin_df['woe'] = np.log(bin_df['badattr']/bin_df['goodattr'])
        bin_df['bin_iv'] = (bin_df['badattr']-bin_df['goodattr'])*bin_df['woe']
        bin_df['IV'] = bin_df['bin_iv'].sum()
        cut = df[col].unique().tolist()
    # 连续型特征的分箱
    else:
        if bin_type=='dt':
            cut = tree_split(df,col,target,max_bin=max_bin,min_binpct=min_binpct,nan_value=nan_value)
        elif bin_type=='quantile':
            cut = quantile_split(df,col,target,max_bin=max_bin,nan_value=nan_value)
        cut.insert(0,float('-inf'))
        cut.append(float('inf'))
        
        bucket = pd.cut(df[col],cut)
        group = df.groupby(bucket)
        bin_df = pd.DataFrame()

        bin_df['total'] = group[target].count()
        bin_df['totalrate'] = bin_df['total']/total
        bin_df['bad'] = group[target].sum()
        bin_df['badrate'] = bin_df['bad']/bin_df['total']
        bin_df['good'] = bin_df['total'] - bin_df['bad']
        bin_df['goodrate'] = bin_df['good']/bin_df['total']
        bin_df['badattr'] = bin_df['bad']/bad
        bin_df['goodattr'] = (bin_df['total']-bin_df['bad'])/good
        bin_df['woe'] = np.log(bin_df['badattr']/bin_df['goodattr'])
        bin_df['bin_iv'] = (bin_df['badattr']-bin_df['goodattr'])*bin_df['woe']
        bin_df['IV'] = bin_df['bin_iv'].sum()
        
    return bin_df,cut




def calIv():
    spark = SparkSession.builder.appName("Word Count").enableHiveSupport().getOrCreate()
    sql = """
    select
        *
    from
    (
        select
            uid as label_uid,
            if(label==2,1,label) label
        from
            shujuzhinengsuanfa.dwd_loan_duanxin_experiment_sample_webapp_3day
        where concat_ws('-', year, month, day) = '2021-06-06' 
        and exp_mode='loan_open_account'
        and label in (0, 2)
    )t1
    inner join
    (
        select
            *
        from fin_new_pro.feature_wju_wujie_whole_base_info
        where concat_ws('-', year, month, day) = '2021-05-30'
        and pid>0
    )t2
    on(t1.label_uid=t2.pid)
    """

    df = spark.sql(sql)
    print(df.dtypes)

    #只要数字类型
    numberColumns = []
    for (column, dtype) in df.dtypes:
        if dtype in ("bigint", "integer"):
            numberColumns.append(column)

    numberDf = df.select(numberColumns).fillna(-999)
    pandasDF = numberDf.toPandas()
    pandasDF.head()

    #计算iv
    target="label"
    for column in numberColumns:
        pirnt("column:" + column)
        bin_df, cut = binning_var(pandasDF, column, target, bin_type='dt', max_bin=5, min_binpct=0.05, nan_value=-999)
        print(bin_df["IV"])
        print(bin_df["bin_iv"])
        print(cut)

calIv()

# df=data
# col="market_score"
# 

# bin_df,_=binning_var(df,col,target,bin_type='dt',max_bin=5,min_binpct=0.05,nan_value=-999)

# data=pd.read_csv('./youmeng_data.txt',sep='\t',dtype=np.float32)

    
