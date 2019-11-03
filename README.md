> gmq是一个轻量级的消息中间件;第一个版本的gmq是基于redis实现,因为功能和存储严重依赖于redis特性,使得之后的优化受到限制,所以在最新的版本不再使用`redis`,完全移除对redis依赖;最新版本的消息存储部分使用文件存储,并使用内存映射文件的技术,使得gmq访问磁盘上的数据文件更加高效;
> 如果对于redis版本的gmq有兴趣,可以参考[gmq-redis](https://github.com/wuzhc/gmq-redis)

## 1. 架构
gmq是一个简单的推拉模型,基本架构如下:
![架构](https://gitee.com/wuzhc123/zcnote/raw/master/images/gmq/gmq%E6%9E%B6%E6%9E%84%E8%AE%BE%E8%AE%A1.png)
### 1.1 名词:  
- register 注册中心,负责节点信息存储
- node 节点,提供消息服务
- dispatcher 调度器,负责管理topic
- topic 消息主题,即消息类型,每一条消息都有所属topic,topic会维护一个queue
- queue 队列,消息存储地方
### 1.2 大概流程:  
- 1. 先启动注册中心服务
- 2. 启动节点服务,节点向注册中心注册自己的基本信息
- 3. 客户端通过连接注册中心,获取注册中心保存的所有节点信息
- 4. 客户端根据某个算法从节点列表中选择一个节点建立连接,然后进行消息推送
- 5. 消息通过调度器获取对应的topic(不存在则新建一个topic),然后交由topic处理
- 6. topic将消息存储于queue队列中,等待客户端消费

## 2. 安装运行
### 2.1 安装glide依赖管理工具
如果你已经安装了,可以直接跳过这步
```
git clone https://github.com/xkeyideal/glide.git $GOPATH/src/github.com/xkeyideal/glide
cd $GOPATH/src/github.com/xkeyideal/glide
make install

# 如果你翻不了墙,可以将`golang.org/x/sys/unix`映射到`github.com/golang/sys`上,在终端上执行下面命令
glide mirror set https://golang.org/x/sys/unix https://github.com/golang/sys --base golang.org/x/sys
```

### 2.2 安装gmq
```bash
git clone https://github.com/wuzhc/gmq.git $GOPATH/src/github.com/wuzhc/gmq
cd $GOPATH/src/github.com/wuzhc/gmq
make # 编译后执行文件(`gregister`,`gnode`)存储在$GOPATH/bin目录

# 启动注册中心服务
$GOPATH/bin/gregister -http_addr=":9595"
# 启动节点
# http_addr http服务
# tcp_addr tcp服务
# node_id节点ID,唯一值,范围在1到1024
# node_weight节点权重,用于多节点选择节点的依据
$GOPATH/bin/gnode -http_addr=":9504" -tcp_addr=":9503" -register_addr="http://127.0.0.1:9595" -node_id=1 -node_weight=1 
```
除此之外,还可以直接指定配置文件,命令行参数为`-config_file`,如下:
```bash
gregister -config_file="conf.ini"
gnode -config_file="conf.ini"
```
配置文件`conf.ini`,参考
- gnode配置文件 https://github.com/wuzhc/gmq/blob/gmq-dev-v3/cmd/gnode/conf.ini
- gregister配置文件 https://github.com/wuzhc/gmq/blob/gmq-dev-v3/cmd/gregister/conf.ini

### 将node服务添加到系统service服务
```bash
# 安装node服务,文件位于`/etc/systemd/system/gmq-node.service`
$GOPATH/bin/gnode install
# 卸载node服务
$GOPATH/bin/gnode uninstall
# 启动node服务
$GOPATH/bin/gnode start
# 停止node服务
$GOPATH/bin/gnode stop
# 重启node服务
$GOPATH/bin/gnode restart
# 查看node运行状态
$GOPATH/bin/gnode status
```

注意:  
- 先启动注册中心`gregister`,再启动节点`gnode`,因为每个节点启动时候需要把节点基本信息上报给注册中心
- 如果想快速体验gmq,也可以直接用docker容器运行gmq的镜像,[参考](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%AE%B9%E5%99%A8docker.md)

## 3. 测试
启动注册中心和节点之后,便可以开始消息推送和消费了,打开终端,执行如下命令
```bash
# 推送消息
curl -d 'data={"body":"hello world","topic":"topic-1","delay":0}' 'http://127.0.0.1:9504/push'
# 消费消息
curl http://127.0.0.1:9504/pop?topic=topic-1 
# 确认消息
curl http://127.0.0.1:9504/ack?msgId=xxxxx&topic=topic-1
```

## 4. 客户端
gmq提供了一个golang版本的客户端,目前还只是demo级的,参考[gmq-client客户端](https://github.com/wuzhc/gmq-client-go)

## 5. web管理系统
在`gmq-web`系统中,可以进行手动注册注销节点,查看各个topic统计信息,修改topic配置信息,进行消息推送,拉取,确认等等功能,参考[gmq-web管理系统](https://github.com/wuzhc/gmq-web),以下是`gmq-web`的截图
![gmq-web](https://gitee.com/wuzhc123/zcnote/raw/master/images/gmq/gmq-web%E4%B8%BB%E9%A2%98%E5%88%97%E8%A1%A8.png)

## 6. 相关文章
- [gmq架构设计](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8.md)
- [gmq通信协议](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E9%80%9A%E4%BF%A1%E5%8D%8F%E8%AE%AE.md)
- [gmq多节点使用](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%A4%9A%E8%8A%82%E7%82%B9%E4%BD%BF%E7%94%A8.md)
- [gmq消息持久化](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%8C%81%E4%B9%85%E5%8C%96%E5%AD%98%E5%82%A8.md) 
- [gmq消息确认机制](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%B6%88%E6%81%AF%E7%A1%AE%E8%AE%A4%E6%9C%BA%E5%88%B6.md)
- [gmq延迟消息机制](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%BB%B6%E8%BF%9F%E6%B6%88%E6%81%AF%E6%9C%BA%E5%88%B6.md)
- [gmq队列处理消息过程]()
- [gmq性能分析pprof工具](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%80%A7%E8%83%BD%E7%9B%91%E6%8E%A7.md)
- [用docker运行gmq](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%AE%B9%E5%99%A8docker.md)

## 7. 链接
- [gmq-client客户端](https://github.com/wuzhc/gmq-client-go)
- [gmq-web管理系统](https://github.com/wuzhc/gmq-web)
- [gmq-redis基于redis版本的gmq](https://github.com/wuzhc/gmq-redis)

**相对于其他消息中间件,gmq足够简单,没有复杂而晦涩的概念,当然这也是gmq还没有提供更加丰富功能,gmq现在还是处于开发阶段,欢迎大家参与进来并提交你们的代码~**