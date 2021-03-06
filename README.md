> gmq是一个轻量级的消息中间件;第一个版本的gmq是基于redis实现,因为功能和存储严重依赖于redis特性,使得之后的优化受到限制,所以在最新的版本不再使用`redis`,完全移除对redis依赖;最新版本的消息存储部分使用文件存储,并使用内存映射文件的技术,使得gmq访问磁盘上的数据文件更加高效;
> 如果对于redis版本的gmq有兴趣,可以参考[gmq-redis](https://github.com/wuzhc/gmq-redis)

## 功能
- 支持延迟消息
- 消息集群，内置etcd注册发现
- 支持消息自定义路由（类似于rabbitmq路由模式，支持全匹配和模糊匹配）
- 支持订阅发布功能（类似于redis的订阅发布subscribe/publish）
- 支持消息持久化
- 支持消息确认
- 支持消息重试机制
- 支持死信队列
- 提供tls可选安全加密
- 提供http api接口
- 内置了pprof调试工具

## 1. 模型
gmq是一个简单的推拉模型,如图:
![架构](https://gitee.com/wuzhc123/zcnote/raw/master/images/gmq/gmq%E6%9E%B6%E6%9E%84%E8%AE%BE%E8%AE%A1.png)

### 1.1 名词:  
- etcd 注册中心,负责gnode节点信息存储，为客户端提供发现服务功能
- gnode 节点,提供消息服务
- dispatcher 调度器,负责管理topic
- topic 消息主题,即消息类型,每一条消息都有所属topic,topic会维护多个queue
- queue 队列,消息存储地方
- bind_key 绑定键，通过绑定键将队列绑定在指定topic上，一个topic上的绑定键是唯一的，绑定键可以作为队列的标识
- route_key 路由键，投递消息时指定路由键和topic，可以将消息投递到路由键和绑定键匹配的队列上（支持全匹配和模糊匹配两种模式）

### 1.2 流程说明:  
- 1. 启动etcd服务
- 2. 启动gnode节点，并向etcd注册节点信息
- 3. 客户端通过监控etcd,获取etcd中所有gnode节点信息
- 4. 客户端可以根据某个算法从节点列表中选择一个节点建立连接,然后进行消息推送
- 5. 消息通过调度器获取对应的topic(不存在则新建一个topic),然后交由topic处理
- 6. topic根据route_key，将消息投递到与bind_key相匹配queue队列中（投递之前需要先声明队列，指定bind_key）

## 2. 安装运行
### 2.1 启动etcd
gmq节点启动需要向etcd注册节点信息，所以需要先启动etcd，安装etcd过程很简单，可以在网上找到对应的教程，安装成功后启动命令如下：
```bash
# 启动etcd
./etcd 
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
build/gnode -http_addr=":9504" -tcp_addr=":9503" -etcd_endpoints="127.0.0.1:2379" -node_id=1 -node_weight=1 

# 或者使用`go run`直接运行源码
go run cmd/gnode/main.go -http_addr=":9504" -tcp_addr=":9503" -etcd_endpoints="127.0.0.1:2379" -node_id=1 -node_weight=1 
```

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

## 5. 相关文章
- [gmq架构设计](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8.md)
- [gmq通信协议](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E9%80%9A%E4%BF%A1%E5%8D%8F%E8%AE%AE.md)
- [gmq多节点使用](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%A4%9A%E8%8A%82%E7%82%B9%E4%BD%BF%E7%94%A8.md)
- [gmq消息持久化](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%8C%81%E4%B9%85%E5%8C%96%E5%AD%98%E5%82%A8.md) 
- [gmq消息确认机制](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%B6%88%E6%81%AF%E7%A1%AE%E8%AE%A4%E6%9C%BA%E5%88%B6.md)
- [gmq性能分析pprof工具](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%80%A7%E8%83%BD%E7%9B%91%E6%8E%A7.md)

## 6. 链接
- [gmq-redis基于redis版本的gmq](https://github.com/wuzhc/gmq-redis)

## 7. 未来
- 重构通信模块，使用grpc进行通信
- 重新设计消息持久化存储（应该会参考rocketmq和kafka）
- 支持消息查询，以便追踪消息
- 提供多种编程语言的客户端
- 完善文档
