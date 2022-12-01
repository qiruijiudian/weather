## 温度实时同步

>  &nbsp;&nbsp;&nbsp;&nbsp;针对各个地区（目前加入天津、错那）的温度数据自动同步



### 1. 文件结构

- 主程序
  - `cona.py`：错那板块代码，数据源：`http://www.tianqihoubao.com`
  - `tianjin.py`：天津板块代码，数据源：`https://api.weather.com`
- `settings.py`: 参数配置
- `log`：日志文件
- `data_check`：数据检查文件，包含每次访问获取到的数据文件信息
- `README.md`：说明文档
- `requirements.txt`：依赖文件
- `其他`: 其他配置文件

![weather文件结构.jpg](http://tva1.sinaimg.cn/large/bf776e91ly1h8oczonk9aj20ea0awdgt.jpg)


### 2. 项目概述

&nbsp;&nbsp;&nbsp;&nbsp;不同每个地区的数据同步程序除了数据源和数据获取的操作不同，其余部分基本一致，主要分为实时数据获取和历史数据获取。实时获取根据项目执行周期有所不同，比如天津板块数据每小时执行一次，数据方面每半小时记录一次，错那板块则是只记录每日平均温度，历史数据获取则是在实时获取的基础上将时间线延长。



### 3. 项目执行

&nbsp;&nbsp;&nbsp;&nbsp;weather目录下即为执行程序，直接切在环境中执行主程序即可，程序中以类结构编写，如果需要修改执行函数可进入编辑，实时数据获取：`Weather().update_real_time_data()`，历史数据获取：`Weather().update_history_data()`。
&nbsp;&nbsp;&nbsp;&nbsp;实时数据更新会基于数据库中最新数据日期进行更新，历史数据则是基于设定中的初始数据进行更新历史数据


~~~
1. 安装依赖项
pip install -r requirements.txt

2. 执行函数
python cona.py
~~~

