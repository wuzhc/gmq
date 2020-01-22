package gnode

import (
	"github.com/wuzhc/gmq/pkg/logs"
	"sync"
	"time"
)

type CommitLogService struct {
	globalCtx        *Context
	commitLogs       map[int64]*CommitLog
	commitLogsMux    sync.RWMutex
	lastCommitLog    *CommitLog
	exitChan         chan struct{}
	broker           *Broker
	dispatchProgress int64
}

func NewCommitLogService(globalCtx *Context, broker *Broker) *CommitLogService {
	return &CommitLogService{
		broker:     broker,
		exitChan:   make(chan struct{}),
		globalCtx:  globalCtx,
		commitLogs: make(map[int64]*CommitLog),
	}
}

func (cls *CommitLogService) stop() {
	close(cls.exitChan)
}

func (cls *CommitLogService) dispatch() {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ticker.C:
			for offset, commitLog := range cls.commitLogs {
				if offset >= commitLog.startOffset {
					if err := commitLog.dispatchToConsumeQueue(); err != nil {
						cls.LogError(err.Error())
					}
				}
			}
		case <-cls.exitChan:
			ticker.Stop()
			return
		}
	}

}

func (cls *CommitLogService) PutMessage(msg *CommitLogMsg) error {
	commitLog, err := cls.getLastCommitLog()
	if err != nil {
		return err
	}

	data := EncodeCommitLogMsg(msg)
	nextOffset := commitLog.writeOffset + int64(len(data))
	// insufficient space for storage, create an new commitLog file
	if nextOffset > commitLogSize {
		commitLog, err = cls.createCommitLog(nextOffset)
		if err != nil {
			return err
		}
	}

	commitLog.PutMessage(data)
	return nil
}

func (cls *CommitLogService) read(readOffset int64, msgSize int) *CommitLogMsg {
	// 获取最近的commitLog
	var tempOffset int64
	for beginOffset, _ := range cls.commitLogs {
		if readOffset > beginOffset && tempOffset < beginOffset {
			tempOffset = beginOffset
		}
	}
	commitLog := cls.commitLogs[tempOffset]
	return commitLog.read(readOffset, msgSize)
}

func (cls *CommitLogService) getLastCommitLog() (*CommitLog, error) {
	if cls.lastCommitLog == nil {
		return cls.createCommitLog(0)
	}

	return cls.lastCommitLog, nil
}

func (cls *CommitLogService) createCommitLog(offset int64) (*CommitLog, error) {
	cls.commitLogsMux.Lock()
	defer cls.commitLogsMux.Unlock()

	if _, ok := cls.commitLogs[offset]; ok {
		return cls.commitLogs[offset], nil
	}

	commitLog, err := NewCommitLog(offset, cls)
	if err != nil {
		return nil, err
	}

	cls.commitLogs[offset] = commitLog
	cls.lastCommitLog = commitLog
	return commitLog, nil
}

func (cls *CommitLogService) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("CommitLogService"))
	v = append(v, msg...)
	cls.globalCtx.Logger.Error(v...)
}

func (cls *CommitLogService) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("CommitLogService"))
	v = append(v, msg...)
	cls.globalCtx.Logger.Warn(v...)
}

func (cls *CommitLogService) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("CommitLogService"))
	v = append(v, msg...)
	cls.globalCtx.Logger.Info(v...)
}

func (cls *CommitLogService) LogDebug(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("CommitLogService"))
	v = append(v, msg...)
	cls.globalCtx.Logger.Debug(v...)
}
