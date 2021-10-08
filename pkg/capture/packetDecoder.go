package capture

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"myRedisCapture/pkg/constvar"
	"myRedisCapture/tclog"

	"go.uber.org/zap"
)

//解析的命令结果
type CmdState struct {
	readingMultiLine  bool     //正在读取一个数组,如 *\r\n3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n
	expectedArgsCount int      //数组中期望的参数个数,set a 1,参数个数是3
	msgType           byte     //*、$
	args              [][]byte //解析出来的命令 "set","a","b"
	bulkLen           int64    //如$3\r\nset\r\n,bulkLen中表示3

	Keys   []string
	MaxKey string
	KeyCnt int
	MaxVal string
	ValCnt int
}

func (c *CmdState) finished() bool {
	return c.expectedArgsCount > 0 && len(c.args) == c.expectedArgsCount
}

//命令参数个数
func (c *CmdState) ArgsCnt() int {
	return len(c.args)
}

//return full  cmd string
func (c *CmdState) FullCmdString() string {
	var fullCmd string
	for idx, item := range c.args {
		if idx == 0 {
			fullCmd = string(item)
		} else {
			fullCmd = fmt.Sprintf("%s %s", fullCmd, string(item))
		}
	}
	return fullCmd
}

//Redis网络包解析
type RedisPacketDecoder struct {
	SrcIP         string     `json:"srcIP"`
	SrcPort       int        `json:"srcPort"`
	DstIP         string     `json:"dstIP"`
	DstPort       int        `json:"dstPort"`
	SrcPacketData []byte     `json:"srcPacketData"`
	LastReqTime   time.Time  `json:"reqTime"`
	ReqCmds       []CmdState `json:"reqCmds"`
	Err           error      `json :"err"`
}

func NewRedisPacketDecoder(srcIP, dstIP string, srcPort, dstPort int, packetData []byte, reqTime time.Time) *RedisPacketDecoder {
	ret := &RedisPacketDecoder{
		SrcIP:         srcIP,
		SrcPort:       srcPort,
		DstIP:         dstIP,
		DstPort:       dstPort,
		SrcPacketData: packetData,
		LastReqTime:   reqTime,
	}
	// fmt.Printf("NewRedisPacketDecoder=>%s\n", string(ret.SrcPacketData))
	return ret
}

//set lastReqTime
func (r *RedisPacketDecoder) SetLastReqTime(t time.Time) {
	r.LastReqTime = t
}

//get lastReqTime
func (r *RedisPacketDecoder) GetLastReqTime() time.Time {
	return r.LastReqTime
}

//(请求包不完整),追加请求包
func (r *RedisPacketDecoder) AppendReq(reqPacket []byte) {
	r.SrcPacketData = append(r.SrcPacketData, reqPacket...)
}

//命令是否完整
func (r *RedisPacketDecoder) isFullCmd() bool {
	len01 := len(r.SrcPacketData)
	if len01 < 2 {
		return false
	}
	return r.SrcPacketData[len01-2] == '\r' && r.SrcPacketData[len01-1] == '\n'
}

func (r *RedisPacketDecoder) readLine(bufReader *bufio.Reader, state *CmdState) ([]byte, bool) {
	var msg []byte
REREAD:
	if state.bulkLen == 0 { // read normal line
		for {
			msg, r.Err = bufReader.ReadBytes('\n')
			if r.Err != nil && r.Err == io.EOF {
				r.Err = nil
				r.SrcPacketData = []byte{} //请求数据解析完了,清空
				return nil, true
			}
			if len(msg) < 2 || msg[len(msg)-2] != '\r' {
				//不合符规范的数据,直接丢弃
				r.Err = fmt.Errorf("readLine protocol error,msg:%s,srcPacketData:%s", string(msg), string(r.SrcPacketData))
				tclog.Logger.Debug(r.Err.Error())
				// return nil, false
				continue
			}
			break
		}
	} else { // read bulk line (binary safe)
		// peek先预读取,如果预读取成功则继续真正读取,否则丢弃该不完整的命令
		tmpMsg, err := bufReader.Peek(int(state.bulkLen) + 2)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			r.Err = fmt.Errorf("reader.Peek fail,err:%v", err)
			tclog.Logger.Debug(r.Err.Error(), zap.String("msg", string(msg)))
			return nil, true
		}
		if len(tmpMsg) < 2 ||
			tmpMsg[len(tmpMsg)-2] != '\r' ||
			tmpMsg[len(tmpMsg)-1] != '\n' {
			//预读取得到的bulk数据已经有问题,这个命令重置
			// fmt.Printf("FFFFFFF  cmd,tmpMsg:%s\n",string(tmpMsg)))
			state.readingMultiLine = false
			state.expectedArgsCount = 0
			state.msgType = ' '
			state.args = [][]byte{}
			state.bulkLen = 0
			goto REREAD
		}
		//预读取成功
		msg = make([]byte, state.bulkLen+2)
		var readCnt int
		readCnt, r.Err = io.ReadFull(bufReader, msg)
		if r.Err != nil && r.Err != io.EOF && r.Err != io.ErrUnexpectedEOF {
			r.Err = fmt.Errorf("io.ReadFull fail,err:%v", r.Err)
			tclog.Logger.Debug(r.Err.Error(), zap.String("msg", string(msg)))
			return nil, true
		} else if r.Err != nil && (r.Err == io.EOF || r.Err == io.ErrUnexpectedEOF) {
			msg = msg[:readCnt]
		}

		if len(msg) < 2 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			r.Err = errors.New(fmt.Sprintf("io.ReadFull protocol error,client:%s,err:%v,bulkLen:%d,readCnt:%d,msglen:%d,srcDataLen:%d,msgData:%s,srcData:%s",
				fmt.Sprintf("%s:%d => %s:%d", r.SrcIP, r.SrcPort, r.DstIP, r.DstPort),
				r.Err, state.bulkLen, readCnt, len(msg), len(r.SrcPacketData), string(msg), string(r.SrcPacketData)))
			tclog.Logger.Debug(r.Err.Error())
			return nil, false
		}
		state.bulkLen = 0
	}
	return msg, false
}

func (r *RedisPacketDecoder) parseMultiBulkHeader(msg []byte, state *CmdState) {
	var expectedLine uint64
	expectedLine, r.Err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if r.Err != nil {
		r.Err = fmt.Errorf("strconv.ParseUint fail,err:%v,msg:%s", r.Err, string(msg))
		tclog.Logger.Debug(r.Err.Error())
		return
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return
	} else {
		r.Err = fmt.Errorf("protocol error,msg:%s ", string(msg))
		tclog.Logger.Error(r.Err.Error())
		return
	}
}

func (r *RedisPacketDecoder) parseBulkHeader(msg []byte, state *CmdState) {
	state.bulkLen, r.Err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if r.Err != nil {
		r.Err = fmt.Errorf("strconv.ParseUint fail,err:%v,msg:%s", r.Err, string(msg))
		tclog.Logger.Debug(r.Err.Error())
		return
	}
	if state.bulkLen == -1 { // null bulk
		return
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return
	} else {
		r.Err = fmt.Errorf("protocol error,msg:%s", string(msg))
		tclog.Logger.Debug(r.Err.Error())
		return
	}
}
func (r *RedisPacketDecoder) readBody(msg []byte, state *CmdState) {
	if len(msg) < 2 {
		fmt.Printf("readBody ==> len:%d msg:%s srcData:%s\n", len(msg), string(msg), string(r.SrcPacketData))
	}
	line := msg[0 : len(msg)-2]
	if len(line) > 0 && line[0] == '$' {
		state.bulkLen, r.Err = strconv.ParseInt(string(line[1:]), 10, 64)
		if r.Err != nil {
			r.Err = fmt.Errorf("protocol error,msg:%s,srcData:%s", string(msg), string(r.SrcPacketData))
			tclog.Logger.Debug(r.Err.Error())
			return
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return
}

func (r *RedisPacketDecoder) parse0() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
		}
	}()
	if r.isFullCmd() == false {
		// fmt.Printf("====>NOT FULL,data:%s\n", string(r.SrcPacketData))
		return
	}
	if len(r.SrcPacketData) == 0 {
		return
	}
	reader := bytes.NewReader(r.SrcPacketData)
	bufReader := bufio.NewReader(reader)
	var state CmdState
	var msg []byte
	for {
		//read line
		var ioErr bool
		msg, ioErr = r.readLine(bufReader, &state)
		if ioErr {
			return
		}
		if r.Err != nil {
			return
		}
		//parse line
		if !state.readingMultiLine {
			if msg[0] == '*' {
				r.parseMultiBulkHeader(msg, &state)
				if r.Err != nil {
					state = CmdState{}
				}
				if state.expectedArgsCount == 0 {
					state = CmdState{}
				}
				continue
			} else if msg[0] == '$' {
				r.parseBulkHeader(msg, &state)
				if r.Err != nil {
					state = CmdState{}
				}
				if state.bulkLen == -1 {
					state = CmdState{}
				}
				continue
			}
		} else {
			r.readBody(msg, &state)
			if r.Err != nil {
				state = CmdState{}
				continue
			}
			if state.finished() {
				cmdStr := string(state.args[0])
				cmdStr = strings.ToLower(cmdStr)
				cmdMeta, ok := RedisCommandTable[cmdStr]
				if ok == true {
					//只处理认识的command
					err := cmdMeta.PreCheck(&state)
					if err != nil {
						//尽管检查出错(用户发送了错误命令),依然要打印
						goto APPENDCMD
					}
					state.Keys, state.MaxKey, state.KeyCnt, err = cmdMeta.GetKeysAndCntAndMaxKey(&state)
					if err != nil {
						goto APPENDCMD
					}
					state.MaxVal, state.ValCnt, err = cmdMeta.MaxValueAndValueCnt(&state)
					if err != nil {
						goto APPENDCMD
					}
				}
			APPENDCMD:
				r.ReqCmds = append(r.ReqCmds, state)
				state = CmdState{}
			}
		}
	}
}

//打印命令
func (r *RedisPacketDecoder) GetAllFormatedCmds() (cmds []string) {
	if len(r.ReqCmds) == 0 {
		return
	}
	var prefix string
	if len(r.ReqCmds) > 1 {
		prefix = "[PIPELINE]"
	} else {
		prefix = "[NORMAL]"
	}
	for _, cmdItem := range r.ReqCmds {
		cmd01 := cmdItem
		// fmt.Printf("[%s] client: %s:%d => redis: %s:%d %s %s\n",
		// 	r.LastReqTime.Local().Format(constvar.UNIXTIME_LAYOUT),
		// 	r.SrcIP, r.SrcPort, r.DstIP, r.DstPort,
		// 	prefix, cmd01.FullCmdString())
		cmds = append(cmds, fmt.Sprintf("[%s] client: %s:%d => redis: %s:%d %s %s",
			r.LastReqTime.Local().Format(constvar.UNIXTIME_LAYOUT),
			r.SrcIP, r.SrcPort, r.DstIP, r.DstPort,
			prefix, cmd01.FullCmdString()))
	}
	return
}

//清空解析出的命令
func (r *RedisPacketDecoder) clearReqCmd() {
	r.ReqCmds = []CmdState{}
}
