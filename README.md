> gmq是一个轻量级的消息中间件;第一个版本的gmq是基于redis实现,因为功能和存储严重依赖于redis特性,使得之后的优化受到限制,所以在最新的版本不再使用`redis`,完全移除对redis依赖;最新版本的消息存储部分使用文件存储,并使用内存映射文件的技术,使得gmq访问磁盘上的数据文件更加高效;
> 如果对于redis版本的gmq有兴趣,可以参考[gmq-redis](https://github.com/wuzhc/gmq-redis)

## 功能
- 支持延迟消息
- 类似rabbitmq路由模式，支持路由键全匹配和模糊匹配，根据路由键将消息路由到指定队列
- 消息集群，使用etcd发现注册服务
- 支持消息持久化
- 支持消息确认
- 支持消息重试机制
- 支持死信队列
- 提供tls可选安全加密
- 提供http api接口
- 提供web管理界面
- 内置了pprof调试工具

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
### 2.1 启动etcd
gmq节点启动需要向etcd注册节点信息，所以需要先启动etcd，安装etcd过程很简单，可以在网上找到对应的教程，安装成功后启动命令如下：
```bash
# 最简单启动命令，指定etcd名称为gmq-register
etcd --name gmq-register
```

### 2.2 启动gmq节点
gmq使用`go mod`来管理第三方依赖包，需要设置环境变量`GO111MODULE=on`,因为国内墙问题，还需要设置代理加快速度`GOPROXY=https://goproxy.io`，如下：
```bash
export GO111MODULE=on 
export GOPROXY=https://goproxy.io
```

下载gmq源码
```bash
# clone项目
git clone -b gmq-dev-v3  https://github.com/wuzhc/gmq.git 
# 进入gmq目录
cd gmq
```

`go mod`工具安装不了`go-systemd`库，所以需要先手动安装`go-systemd`
```bash
# 将coreos/go-systemd下载到GOPATH路径下
git clone https://github.com/coreos/go-systemd.git ${GOPATH}/src/github.com/coreos/go-systemd
# 修改go.mod文件
vi go.mod
# 替换replace的/data/wwwroot/go修改为你的电脑GOPATH的实际路径，/data/wwwroot/go是我电脑的GOPATH路径
replace github.com/coreos/go-systemd => /data/wwwroot/go/src/github.com/coreos/go-systemd
```

编译安装
```bash
# 使用go mod安装需要的依赖，并将依赖包移动到gmq根目录下的vendor目录下，例如gmq/vendor
go mod tidy
go mod vendor
# 编译
make build
# 启动节点
# http_addr 指定http服务IP和端口
# tcp_addr 指定tcp服务IP和端口
# node_id节点ID,每个节点都是一个唯一值,范围在1到1024之间
# node_weight节点权重,用于多节点选择节点的依据
build/gnode -http_addr=":9504" -tcp_addr=":9503" -etcd_endpoints="127.0.0.1:2379,127.0.0.1:2479" -node_id=1 -node_weight=1 
# 或者使用`go run`直接运行源码
go run cmd/gnode/main.go -http_addr=":9504" -tcp_addr=":9503" -etcd_endpoints="127.0.0.1:2379,127.0.0.1:2479" -node_id=1 -node_weight=1 
```

### 2.3 配置启动参数
上面启动节点时，命令行可以指定参数，除此之外，还可以直接指定配置文件,命令行参数为`-config_file`,如下:
```bash
build/gnode -config_file="conf.ini"
```
配置文件`conf.ini`,参考
- gnode配置文件 https://github.com/wuzhc/gmq/blob/gmq-dev-v3/cmd/gnode/conf.ini

### 2.4 添加到系统service服务
添加到系统service服务，方便线上环境操作
```bash
# 安装node服务,文件位于`/etc/systemd/system/gmq-node.service`
build/gnode install
# 卸载node服务
build/gnode uninstall
# 启动node服务
build/gnode start
# 停止node服务
build/gnode stop
# 重启node服务
build/gnode restart
# 查看node运行状态
build/gnode status
```

### 2.5 docker运行
- 如果想快速体验gmq,也可以直接用docker容器运行gmq的镜像,[参考](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%AE%B9%E5%99%A8docker.md)

## 3. 测试
启动注册中心和节点之后,便可以开始消息推送和消费了,打开终端,执行如下命令
```bash
# 配置topic
# isAutoAck 是否自动确认消息，1是，0否，默认为0
# mode 路由key匹配模式，1全匹配，2模糊匹配，默认为1
# msgTTR 消息执行超时时间，在msgTTR内没有确认消息，则消息重新入队，再次被消费,默认为30
# msgRetry 消息重试次数，超过msgRetry次数后，消息会被写入到死信队列，默认为5
curl -s "http://127.0.0.1:9504/config?topic=ketang&isAuthoAck=1&mode=1&msgTTR=30&msgRetry=5"

# 声明队列，
# bindKey 通过key绑定都topic某个队列上，队列名称为gmq自动生成（queue_name = topic + bind_key）
curl -s "http://127.0.0.1:9504/declareQueue?topic=ketang&bindKey=homework"

# 推送消息
# route_key 路由key，当topic.mode为模糊匹配时，可以指定为正则
curl http://127.0.0.1:9504/push -X POST -d 'data={"body":"this is a job","topic":"ketang","delay":0,"route_key":"homework"}'

# 消费消息
# bindKey 绑定key，必须和声明队列bindKey一致，bindKey可以理解为topic.queue的标识
curl "http://127.0.0.1:9504/pop?topic=ketang&bindKey=homework"

# 确认消息
# msgId 消息ID
# bindKey 绑定key，必须和声明队列bindKey一致，bindKey可以理解为topic.queue的标识
curl "http://127.0.0.1:9504/ack?msgId=384261148721025024&topic=ketang&bindKey=homework"
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

## 8. 未来
- 节点注册,发现强一致性问题,节点变动无法即时反馈给消费者
- 没有应用级别的客户端,目前只是一个demo级别的例子,并且没有失败重试功能
- 消息存在丢失情况,在内存映射的文件写数据,会延迟写到磁盘文件,除非手动触发`msync`系统调用,考虑做节点主从复制
- 提供完善的消息追踪功能,目前无法根据消息ID在队列中定位,因为目前没有消息ID和offset建立的map关系
- 提供消息广播功能,广播需要服务端主动推送,这和原来的推拉架构有冲突,需要在架构层面上做改动

**相对于其他消息中间件,gmq足够简单,没有复杂而晦涩的概念,当然这也是gmq还没有提供更加丰富功能,gmq现在还是处于开发阶段,欢迎大家参与进来并提交你们的代码~**