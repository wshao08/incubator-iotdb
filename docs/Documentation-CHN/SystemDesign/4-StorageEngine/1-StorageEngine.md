<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# 存储引擎

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625255-03fe2680-467f-11ea-91ae-64407ef1125c.png">

## 设计思想

存储引擎基于 LSM 设计。数据首先写入内存缓冲区 memtable 中，再刷到磁盘。内存中为每个设备维护当前持久化的（包括已经落盘的和正在持久化的）最大时间戳，根据这个时间戳将数据区分为顺序数据和乱序数据，不同种类的数据通过不同的 memtable 和 TsFile 管理。

每个数据文件 TsFile 在内存中对应一个文件索引信息 TsFileResource，供查询使用。

此外，存储引擎还包括异步持久化和文件合并机制。

## 写入流程

### 相关代码

* org.apache.iotdb.db.engine.StorageEngine

	负责一个 IoTDB 实例的写入和访问，管理所有的 StorageGroupProsessor。
	
* org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor

	负责一个存储组一个时间分区内的数据写入和访问。管理所有分区的TsFileProcessor。

* org.apache.iotdb.db.engine.storagegroup.TsFileProcessor

	负责一个 TsFile 文件的数据写入和访问。
	
	
### 单行数据（一个设备一个时间戳多个值）写入

* 对应的接口
	* JDBC 的 execute 和 executeBatch 接口
	* Session 的 insert 和 insertInBatch

* 总入口: public void insert(InsertPlan insertPlan)
	* 找到对应的 StorageGroupProsessor
	* 根据写入数据的时间以及当前设备落盘的最后时间戳，找到对应的 TsFileProcessor
	* 写入 TsFileProcessor 对应的 memtable 中
	* 记录写前日志
	* 根据 memtable 大小，来判断是否触发异步持久化 memtable 操作
	* 根据当前磁盘 TsFile 的大小，判断是否触发文件关闭操作

### 批量数据（一个设备多个时间戳多个值）写入

* 对应的接口
	* Session 的 insertBatch

* 总入口: public Integer[] insertBatch(BatchInsertPlan batchInsertPlan)
	* 找到对应的 StorageGroupProsessor
	* 根据这批数据的时间以及当前设备落盘的最后时间戳，将这批数据分成小批，分别对应到一个 TsFileProcessor 中
	* 分别将每小批写入 TsFileProcessor 对应的 memtable 中
	* 记录写前日志
	* 根据 memtable 大小，来判断是否触发异步持久化 memtable 操作
	* 根据当前磁盘 TsFile 的大小，判断是否触发文件关闭操作


## 数据访问

* 总入口（StorageEngine）: public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context,
      QueryFileManager filePathsManager)
      
	* 找到所有包含这个时间序列的顺序和乱序的 TsFileResource 进行返回，供查询引擎使用。

## 相关文档

* [写前日志 (WAL)](/#/SystemDesign/progress/chap4/sec2)

* [memtable 持久化](/#/SystemDesign/progress/chap4/sec3)

* [文件合并机制](/#/SystemDesign/progress/chap4/sec4)