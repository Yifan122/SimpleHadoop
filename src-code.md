# 源码编译错误

* Class Not Found
    - pox.xml(hadoop-hdfs) 中将对应的provided 改为 compile

* codec Not Fond
    - jdk.tools 设置 <systemPath>C:/Program Files/Java/jdk1.8.0_251/lib/tools.jar</systemPath>
    - 移除 profiles tag

* 修改 hdfs-site.xml
    - dfs.namenode.name.dir
    
* 第一次运行NameNode
    - 需要初始化 NameNode -format