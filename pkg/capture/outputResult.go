package capture

import (
	"bufio"
	"fmt"
	"myRedisCapture/tclog"
	"os"
	"sync"

	"go.uber.org/zap"
)

//s结果输出接口
type IResultOutput interface {
	WriteLine(line string, flushDisk bool) error
	WriteLines(lines []string, flushDisk bool) error
	Flush() error
	Close() error
}

//save to file
type ResultToFile struct {
	isMultiThreadsWrite bool          `json:"-"` //是否为多线程写入
	saveFile            string        `json:"-"`
	fileP               *os.File      `json:"-"`
	bufWriter           *bufio.Writer `json:"-"`
	mux                 sync.Mutex
}

//Get
func (f *ResultToFile) SaveFile() string {
	return f.saveFile
}

//Set
func (f *ResultToFile) SetSaveFile(savefile string) error {
	var err error
	err = f.Close()
	if err != nil {
		return err
	}
	if savefile == "" {
		f.bufWriter = bufio.NewWriter(os.Stdout)
	} else {
		f.saveFile = savefile
		f.fileP, err = os.OpenFile(savefile, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("open file:%s fail,err:%v", savefile, err)
			tclog.Logger.Error(err.Error())
			return err
		}
		f.bufWriter = bufio.NewWriter(f.fileP)
	}
	return nil
}

//Get
func (f *ResultToFile) IsMultiThreadsWrite() bool {
	return f.isMultiThreadsWrite
}

//Set
func (f *ResultToFile) SetIsMultiThreadsWrite(isMultiThreadsWrite bool) {
	f.isMultiThreadsWrite = isMultiThreadsWrite
}

//新建 文件输出 obj
func NewResultToFile(savefile string, isMultiThreadsWrite bool) (ret *ResultToFile, err error) {
	ret = &ResultToFile{}
	ret.SetIsMultiThreadsWrite(isMultiThreadsWrite)
	err = ret.SetSaveFile(savefile)
	return ret, err
}

//写入 key
func (f *ResultToFile) WriteLine(line01 string, flushDisk bool) error {
	if f.isMultiThreadsWrite == true {
		f.mux.Lock()
		defer f.mux.Unlock()
	}
	_, err := f.bufWriter.WriteString(line01 + "\n")
	if err != nil {
		tclog.Logger.Error("write file fail", zap.Error(err),
			zap.String("key", line01), zap.String("saveFile", f.saveFile))
		return err
	}
	if flushDisk == true {
		f.bufWriter.Flush()
	}
	return nil
}

//写入 keys
func (f *ResultToFile) WriteLines(lines []string, flushDisk bool) error {
	if f.isMultiThreadsWrite == true {
		f.mux.Lock()
		defer f.mux.Unlock()
	}
	for _, key01 := range lines {
		_, err := f.bufWriter.WriteString(key01 + "\n")
		if err != nil {
			tclog.Logger.Error("write file fail", zap.Error(err),
				zap.String("key", key01), zap.String("saveFile", f.saveFile))
			return err
		}
	}
	if flushDisk == true {
		f.bufWriter.Flush()
	}
	return nil
}

func (f *ResultToFile) Flush() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.bufWriter.Flush()
	return nil
}

//close file
func (f *ResultToFile) Close() error {
	f.mux.Lock()
	defer f.mux.Unlock()

	var err error
	f.saveFile = ""

	if f.bufWriter != nil {
		err = f.bufWriter.Flush()
		if err != nil {
			err = fmt.Errorf("bufio flush fail.err:%v,file:%s", err, f.saveFile)
			tclog.Logger.Error(err.Error())
			return nil
		}
		f.bufWriter = nil
	}
	if f.fileP != nil {
		err = f.fileP.Close()
		if err != nil {
			err = fmt.Errorf("file close fail.err:%v,file:%s", err, f.saveFile)
			tclog.Logger.Error(err.Error())
			return nil
		}
		f.fileP = nil
	}
	return nil
}
