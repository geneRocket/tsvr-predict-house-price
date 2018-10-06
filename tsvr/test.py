# -*- coding: utf-8 -*-  
from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler,StandardScaler
from sklearn.decomposition import PCA, KernelPCA
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import DictVectorizer
from sklearn import neural_network
from sklearn.svm import SVR
from TSVR import *
import pandas as pd
from sklearn import tree
from sklearn import linear_model
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
from sklearn.preprocessing import Imputer
from sklearn.base import TransformerMixin
from datetime import datetime
import chinese2digits


def print_dict(d):
     print json.dumps(d, encoding="UTF-8", ensure_ascii=False)

class DataFrameImputer(TransformerMixin):
    def __init__(self):
        """Impute missing values.

        Columns of dtype object are imputed with the most frequent value 
        in column.

        Columns of other types are imputed with mean of column.

        """
    def fit(self, X, y=None):
        self.fill = pd.Series([X[c].value_counts().index[0] if X[c].dtype == np.dtype('O')\
                else X[c].mean() for c in X],index=X.columns)
        return self

    def transform(self, X, y=None):
        return X.fillna(self.fill)

#params1 = {'Epsilon1': 0.1, 'Epsilon2': 0.1, 'C1': 0.01, 'C2': 0.01,'kernel_type':0,'kernel_param': 1}
params1 = {'Epsilon1': 1, 'Epsilon2': 1, 'C1': 0.1, 'C2': 0.1,'kernel_type':0,'kernel_param': 1}
#params2 = {'Epsilon1': 1, 'Epsilon2': 1, 'C1': 1, 'C2': 1,'kernel_type':3,'kernel_param': 7}
params2 = {'Epsilon1': 1, 'Epsilon2': 1, 'C1': 0.1, 'C2': 0.1,'kernel_type':3,'kernel_param': 10}

classifiers = [
    ("Twin",TwinSVMRegressor(**params1)),
    #("Twin rbf",TwinSVMRegressor(**params2)),
    #("tree",linear_model.BayesianRidge()),
    #("tree",linear_model.Ridge()),
    ]


v=DictVectorizer()

def load_data():
    df=pd.DataFrame()
    cnt=0
    df_list=[]
    for filename in os.listdir("lianjia_data"):
        if not filename.endswith(".csv"):
            continue
        part_df=pd.read_csv(os.path.join("lianjia_data",filename),usecols=\
                ["交易权属","产权年限","供暖方式","小区名称","建成年代","建筑类型",\
                "建筑结构","建筑面积","总楼层","户型结构","房屋年限","房屋户型",\
                "房屋朝向","房屋用途","房权所属","挂牌时间","梯户比例","楼层","片区","行政区域","装修情况","配备电梯","总价"],\
                header=0)
        df_list.append(part_df)
    df=pd.concat(df_list)

    for col_name in ['建成年代','建筑面积','总楼层','产权年限']:
        df[col_name]= df[col_name].convert_objects(convert_numeric=True)

    #删除没有成交价
    df=df.dropna(subset=['总价'])

    #选择特定区域的数据
    df=df[df['行政区域'].isin(['海淀'])]
    #df=df[df['小区名称'].isin(['兴隆家园'])]

    #数字日期转换成数字
    start_time=datetime.strptime('2018-5-1','%Y-%m-%d')
    date_col_list=[]
    for timestr in df['挂牌时间']:
        time_object=datetime.strptime(timestr,'%Y-%m-%d')
        diff_day=  start_time - time_object
        diff_day=diff_day.days
        date_col_list.append(diff_day)
    df['挂牌时间']=date_col_list

    #删除过时的数据
    df=df[df['挂牌时间']<400]
    
    #梯户比例
    ladder_col_list=[]
    most_popular=df['梯户比例'].value_counts().index[0]

    ladder_house_str_dict={}
    for ori in df['梯户比例'].value_counts().index:
        ladder_house_str=ori
        if type(ladder_house_str)!=str:
           continue 

        ladder_house_str=ladder_house_str.decode()
        ladder_str=ladder_house_str[:ladder_house_str.find('梯')]
        ladder_num=chinese2digits.chinese2digits(ladder_str)
        house_str=ladder_house_str[ladder_house_str.find('梯')+1:ladder_house_str.find('户')]
        house_num=chinese2digits.chinese2digits(house_str)
        ladder_house_str_dict[ori]= ladder_num / house_num

    for ladder_house_str in df['梯户比例']:
        if type(ladder_house_str)!=str:
            ladder_house_str=most_popular
        ladder_col_list.append(ladder_house_str_dict[ladder_house_str])
    df['梯户比例']=ladder_col_list


    #房屋户型
    shi_list=[]
    ting_list=[]
    chu_list=[]
    wei_list=[]
    most_popular=df['房屋户型'].value_counts().index[0]

    type_str_dict={}
    for ori in df['房屋户型'].value_counts().index:
        type_str=ori
        if type(type_str)!=str:
           continue 
        type_str=type_str.decode()
        if not any(char.isdigit() for char in type_str):
            continue
        start_pos=0
        num_list=[] 
        for char in ['室','厅','厨','卫']:
            end_pos=type_str.find(char)
            room_num=int(type_str[start_pos:end_pos])
            num_list.append(room_num)
            start_pos=end_pos+1
        type_str_dict[ori]=num_list
            

    for type_str in df['房屋户型']:
        if type(type_str)!=str:
            type_str=most_popular
        if not any(char.isdigit() for char in type_str.decode()):
            type_str=most_popular
        num_list=type_str_dict[type_str]

        shi_list.append(num_list[0])
        ting_list.append(num_list[1])
        chu_list.append(num_list[2])
        wei_list.append(num_list[3])
    df['室']=shi_list
    df['厅']=ting_list
    df['厨']=chu_list
    df['卫']=wei_list
    df.pop('房屋户型')




    #房屋朝向
    most_popular=df['房屋朝向'].value_counts().index[0]

    '''
    dir_set=set()
    for ori in df['房屋朝向'].value_counts().index:
        dir_str=ori
        if type(dir_str)!=str:
           continue 
        dir_str=dir_str.decode()
        dir_arr=dir_str.split(' ')
        for d in dir_arr:
            dir_set.add(d)
    '''
    
    dir_dict={}
    for d in ['东','南','西','北','东北','东南','西南','西北']:
        dir_dict[d]=[]
        
    for dir_str in df['房屋朝向']:
       
        if type(dir_str)!=str:
            dir_str=most_popular
        dir_arr=dir_str.decode().split(' ')
        for k in dir_dict:
            if k in dir_arr:
                dir_dict[k].append(1)
               
            else:
                dir_dict[k].append(0)
               

    for k in dir_dict:
        df[k]=dir_dict[k]
    df.pop('房屋朝向')
    
    print df.iloc[0]


    #将“未知”设为空值
    df=df.replace('未知',np.NaN)
        
    
    #众数,平均值补充缺失值
    df=DataFrameImputer().fit_transform(df)

    df.to_csv("transformed_dataset.csv", sep=str(","), encoding="utf-8")
   
    #拆分X，y
    y=df.pop('总价')

    #文字转数字
    X=v.fit_transform(df.to_dict(orient='records'))

    with open("map","w") as f:
        for col_value in v.vocabulary_:
            matrix_pos=v.vocabulary_[col_value]
            f.write(col_value+"="+str(matrix_pos)+"\n")



    
    #中心化，方差
    X=X.toarray()

    #pca = PCA(n_components=X.shape[1]-60)
    #X = pca.fit_transform(X)


    #return X,y.values.reshape(len(y),1)
    print X.shape,y.shape
    return X,y


def load_juying_data():
    df=pd.read_excel('juying.xlsx')
    df=DataFrameImputer().fit_transform(df)
    y=df.pop('y')
    X=df.values
    X=preprocessing.scale(X)
    return X,y

    
X,y=load_data()
#X,y=load_juying_data()

np.set_printoptions(threshold='nan') 
X_train, X_test, y_train, y_test = train_test_split(X,y)

for name, clf in  classifiers:
    print X_train
    clf.fit(X_train, y_train)

    y_predict=clf.predict(X_test)

    print "ori:",clf.score(X_train,y_train)
    print "test:",clf.score(X_test,y_test)

    y_test=y_test.reshape(len(y_test),1)
    y_predict=y_predict.reshape(len(y_test),1)

    print np.hstack((y_predict,y_test))

    avg_err_percent=0;
    print y_test.shape[0]

    #输出预测的结果保存到csv中
    raw_data=v.inverse_transform(X_test)
    data=[]
    for row in raw_data:
        row_dict={}
        for k in row:
            if len(k.split('='))==2:
                row_dict[k.split('=')[0]]= k.split('=')[1]
            else:
                row_dict[k]=row[k]
        data.append(row_dict)

                

        
    df_test=pd.DataFrame(data)
    df_test['实际成交价']=y_test
    df_test['预测价']=y_predict
    error_list=[]

    for i in xrange(y_test.shape[0]):
        print y_predict[i][0],y_test[i][0],abs( (y_predict[i][0]-y_test[i][0]) / y_test[i][0])
        error_list.append(abs( (y_predict[i][0]-              y_test[i][0]) / y_test[i][0]))
        avg_err_percent+=abs( (y_predict[i][0]-y_test[i][0]) / y_test[i][0])
        #avg_err_percent+=abs((y_predict[i][0]-y_test[i][0])) 
        #avg_err_percent+=y_test[i][0]
    avg_err_percent/=y_test.shape[0]
    print avg_err_percent

    df_test['相对误差']=error_list
    df_test.to_csv("predict_output.csv", sep=str(","), encoding="utf-8")

'''
for name, clf in  classifiers:
    clf.fit(X, y)
'''
