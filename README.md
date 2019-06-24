## 平滑退出
当程序修复好bug,需要替换线上服务,此时就需要关闭`gmq`,如果粗暴关闭`gmq`可能会导致某些业务执行到一般就中止,进而出现一些奇怪的未知问题,为了避免这种情况,`gmq`在收到终止信号时,并不会马上退出程序,也是当每个`goroutine`都处理完业务,再退出整个服务

## 确认机制
任务被消费者获取之后,只是删除了队列job信息,`job pool`不会去删除job元数据,只有当消费者手动发起`ack`确认删除;另外,如果当`TTR=0`时,即job不会有超时时间,可以无限的执行,此时`gmq`会在消费获取job之后就直接删掉`job pool`任务

## 智能定时器
每一个`bucket`会维护一个定时器,定时器并非周期性的时钟,通俗来讲,即不是每隔多久执行一次;首先先获取`bucket`下一个要执行`job`的时间,然后用这个时间作为定时器的周期,这样当没有job时不会有其他开销;当然,事物都有两面性,这样的设计带来的弊端就是当生成者产生一个新数据时会可能需要重置定时器的时间,频繁产生意味着频繁重置定时器

## 原子性问题
成功添加到bucket时,会去设置job.status,但是,因为添加bucket和设置job.status是两个方法,两个方法分别去获取redis连接池句柄,此时会出现添加bucket成功后,但是设置job.status获取连接句柄失败(因为访问量大,连接池耗尽了),设置job.status就会阻塞在那里;如果这个时候定时器扫描到bucket,就会得到status是错误的
redis对请求是排队处理,假设我们添加一个job到bucket,经历的过程大概是add bucket -> set job status; 此时如果在`add bucket`和`set job status`两个命令之间插入其他命令,就会使得这个事物原子性问题

## redis建立连接池
刚好第三方库`redis`自带了连接池,我也可以不需要自行实现连接池,连接池是很有必要的,它带来的好处是限制redis连接数,通过复用redis连接来减少开销,另外可以防止tcp被消耗完,这在生产者大量生成数据时会很有用

## 设置job执行超时时间TTR(TIME TO RUN)
当job被消费者读取后,如果`job.TTR>0`,即job设置了执行超时时间,那么job会在读取后添加到bucket,并且设置`job.delay = job.TTR`,在TTR时间内没有得到消费者ack确认删除job,job将在TTR时间之后添加到`ready queue`,然后再次被消费(如果消费者在TTR时间之后才请求ack,会得到失败的响应)

- bucket有job,但是pool没有job
- bucket.JobNum计数有问题
- bucket存在status=ready的job (原本是在bucket中,job.status=delay,强制重启后job.status=ready,但未被加入到readQueue
- jobPool出现只有{status:1}的job (10万个快速请求造成的) 可能的原因: addBucket -> 被中断执行了 delete job -> bucket又设置了状态
- [E] [default] [2019-06-23 22:38:24] strconv.Atoi: parsing "": invalid syntax (可能和上一个bug有关)
