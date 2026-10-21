# 实验二 文档倒排索引算法实现

大数据管理与分析课程实验，使用Java Hadoop MapReduce构建英文文档倒排索引。

## 环境与源码

- Ubuntu 22.04、OpenJDK 1.8.0_502、Apache Hadoop 3.2.2，伪分布式模式。
- `src/main/java/edu/course/bigdata/index/InvertedIndex.java`：Mapper、Combiner、Reducer与作业配置。
- `src/main/java/edu/course/bigdata/index/IndexSupport.java`：英文分词和索引结果格式化。
- `config/stopwords.txt`：133个自建英文停词。

## 算法

统一转换为小写，以连续a-z为单词，过滤停词，不进行词干提取。
Mapper输出单词与“文档文件名、出现次数”，Combiner累加局部词频。
Reducer按文档名汇总次数，计算总词频，以固定文档顺序输出。
配置一个Reducer，生成按词项排序的单个结果文件。

输出格式（下面是格式示例）：

```text
word    <document01.txt,6>;<document02.txt,3>;<total,9>.
```

## 编译

在本仓库根目录、已配置Hadoop的Ubuntu环境中执行：

```bash
mkdir -p build/classes
javac -encoding UTF-8 -source 8 -target 8 -classpath "$(hadoop classpath)" -d build/classes src/main/java/edu/course/bigdata/index/*.java
jar cf build/experiment2.jar -C build/classes .
```

## 运行

启动HDFS和YARN，并将自己的英文TXT文件上传到独立HDFS输入目录。输入文件名需唯一，不能包含制表符。
本实验使用10份Project Gutenberg英文文本，保留并统计原文的附加说明及许可内容；语料和截图不包含在此源码仓库中。
使用新的HDFS输出路径，避免和原实验结果冲突。以下演示沿用本实验已上传的输入数据，输出到单独的 `output-github-demo`：

```bash
hadoop jar build/experiment2.jar edu.course.bigdata.index.InvertedIndex /user/vboxuser/experiment2/input /user/vboxuser/experiment2/output-github-demo file:///home/vboxuser/experiment2/config/stopwords.txt
```

上例的停词表本地URI应指向当前克隆仓库内 `config/stopwords.txt` 的实际绝对路径。
它通过Hadoop分布式缓存分发到任务中；也可传入HDFS上的停词表URI。
若演示输出目录已存在，换用其他新目录，不自动删除任何结果。

## 已验证的实验结果

2026年10月21日实际运行10篇英文文档，共读取6,373,341字节。
有效词出现524,916次，移除停词611,924次，生成29,665个词项。
完整索引的各文档词频及总词频已通过独立串行程序逐条校验。
YARN作业名为 `experiment2-inverted-index`，Application ID为 `application_1792546249794_0001`，状态为 `FINISHED / SUCCEEDED`。

实际结果示例：

```text
alice   <document03.txt,403>;<document04.txt,12>;<document07.txt,1>;<total,416>.
```
