package capture

import (
	"bufio"
	"fmt"
	"log"
	"myRedisCapture/tclog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
)

var GlobalCapture RedisCapture
var packetSize int64

//执行redis网络包抓取、解析、打印
type RedisCapture struct {
	Device                  string `json:"device"`
	DstPort                 int    `json:"dstPort"`
	MaxPacket               uint32 `json:"maxPacket"`
	OnlyBigReq              int    `json:"onlyBigReq"`
	OnlyBigVal              uint32 `json:"onlyBigVal"`
	PrintValLength          bool   `json:"printValLength"`
	TopHotKeys              int    `json:"topHotKeys"`
	Timeout                 int    `json:"timeout"`
	GopacketAssemblerCnt    int    `json:"gopacketAssemblerCnt"`
	GopacketMaxPagesPerConn int    `json:"gopacketFlushConnOlderThan"`
	GopacketMaxPagesTotal   int    `json:"gopacketCloseConnOlderThan"`

	OutputFile string        `json:"outputFile"`
	Outputer   IResultOutput `json:"-"`

	decoderLimit int `json:"-"` //解析者并发数

	genChan chan *RedisPacketDecoder `json:"-"`
	retChan chan *RedisPacketDecoder `json:"-"`
	wg      sync.WaitGroup           `json:"-"`

	done chan struct{}

	Err error `json:"-"`
}

//init genChan/retChan
func (c *RedisCapture) InitRedisCapture() *RedisCapture {
	c.genChan = make(chan *RedisPacketDecoder)
	c.retChan = make(chan *RedisPacketDecoder)
	c.wg = sync.WaitGroup{}
	c.done = make(chan struct{})
	return c
}

//set device
func (c *RedisCapture) SetDevice(device string) *RedisCapture {
	if c.Err != nil {
		return c
	}
	device = strings.TrimSpace(device)
	if device == "" {
		c.Err = fmt.Errorf("device:%s cannot be empty", device)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.Device = device
	return c
}

//get device
func (c *RedisCapture) GetDevice() string {
	return c.Device
}

//set dstPort
func (c *RedisCapture) SetDstPort(dstPort int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	if dstPort < 0 || dstPort > 65535 {
		c.Err = fmt.Errorf("dst-port:%d cannot <0 or >65535", dstPort)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.DstPort = dstPort
	return c
}

//get dstPort
func (c *RedisCapture) GetDstPort() int {
	return c.DstPort
}

//set maxPacket
func (c *RedisCapture) SetMaxPacket(maxPacket uint32) *RedisCapture {
	if c.Err != nil {
		return c
	}
	if maxPacket < 1024 || maxPacket > 16777216 {
		c.Err = fmt.Errorf("max-packet:%d cannot <1024 or >16777216", maxPacket)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.MaxPacket = maxPacket
	return c
}

//get maxPacket
func (c *RedisCapture) GetMaxPacket() uint32 {
	return c.MaxPacket
}

//set onlyBigReq
func (c *RedisCapture) SetOnlyBigReq(onlyBigReq int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	c.OnlyBigReq = onlyBigReq
	return c
}

//get onlyBigReq
func (c *RedisCapture) GetOnlyBigReq() int {
	return c.OnlyBigReq
}

//set onlyBigVal
func (c *RedisCapture) SetOnlyBigVal(onlyBigVal uint32) *RedisCapture {
	if c.Err != nil {
		return c
	}
	c.OnlyBigVal = onlyBigVal
	return c
}

//get onlyBigVal
func (c *RedisCapture) GetOnlyBigVal() uint32 {
	return c.OnlyBigVal
}

//set printValLen
func (c *RedisCapture) SetPrintValLength(printValLength bool) *RedisCapture {
	if c.Err != nil {
		return c
	}
	c.PrintValLength = printValLength
	return c
}

//get printValLen
func (c *RedisCapture) GetPrintValLength() bool {
	return c.PrintValLength
}

//set topHotKeys
func (c *RedisCapture) SetTopHotKeys(topHotKeys int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	c.TopHotKeys = topHotKeys
	return c
}

//get topHotKeys
func (c *RedisCapture) GetTopHotKeys() int {
	return c.TopHotKeys
}

//set Timeout
func (c *RedisCapture) SetTimeout(timeout int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	c.Timeout = timeout
	return c
}

//get Timeout
func (c *RedisCapture) GetTimeout() int {
	return c.Timeout
}

//set  GopacketAssemblerCnt
func (c *RedisCapture) SetGopacketAssemblerCnt(interval int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	if interval < 1 || interval > 10000 {
		c.Err = fmt.Errorf("gopacket-assembler-cnt:%d cannot <1 or > 10000", interval)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.GopacketAssemblerCnt = interval
	return c
}

//get GopacketAssemblerCnt
func (c *RedisCapture) GetGopacketAssemblerCnt() int {
	return c.GopacketAssemblerCnt
}

//set  GopacketMaxPagesPerConn
func (c *RedisCapture) SetGopacketMaxPagesPerConn(interval int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	if interval < 10 {
		c.Err = fmt.Errorf("gopacket-max-pages-per-conn:%d cannot <10", interval)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.GopacketMaxPagesPerConn = interval
	return c
}

//get GopacketMaxPagesPerConn
func (c *RedisCapture) GetGopacketMaxPagesPerConn() int {
	return c.GopacketMaxPagesPerConn
}

//set  GopacketMaxPagesTotal
func (c *RedisCapture) SetGopacketMaxPagesTotal(interval int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	if interval < 100 {
		c.Err = fmt.Errorf("gopacket-max-pages-total:%d cannot <100", interval)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.GopacketMaxPagesTotal = interval
	return c
}

//get GopacketMaxPagesTotal
func (c *RedisCapture) GetGopacketMaxPagesTotal() int {
	return c.GopacketMaxPagesTotal
}

//set DecoderLimit
func (c *RedisCapture) SetDecoderLimit(limit int) *RedisCapture {
	if c.Err != nil {
		return c
	}
	if limit < 1 || limit > 1000 {
		c.Err = fmt.Errorf("decoder-limit:%d cannot <1 or >1000", limit)
		tclog.Logger.Error(c.Err.Error())
		return c
	}
	c.decoderLimit = limit
	return c
}

//get DecoderLimit
func (c *RedisCapture) GetDecoderLimit() int {
	return c.decoderLimit
}

//set outputfile
func (c *RedisCapture) SetOutputFile(outputFile string) *RedisCapture {
	if c.Err != nil {
		return c
	}
	outputFile = strings.TrimSpace(outputFile)
	c.OutputFile = outputFile
	c.Outputer, c.Err = NewResultToFile(c.OutputFile, true)
	return c
}

//get outputfile
func (c *RedisCapture) GetOutputFile() string {
	return c.OutputFile
}

func (c *RedisCapture) FlushOutput() {
	ticker := time.NewTicker(time.Second) //每秒执行一次flush
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ticker.C:
				c.Outputer.Flush()
			case <-c.done:
				return
			}
		}
	}()
}

//并发解析redis请求包
func (c *RedisCapture) ParallelDecode() {
	for woker := 0; woker < c.decoderLimit; woker++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			var ok bool = false
			var decoder *RedisPacketDecoder
			for {
				select {
				case decoder, ok = <-c.genChan:
					if !ok {
						//genChan closed
						return
					}
					decoder.parse0()
					if decoder.Err != nil && len(decoder.ReqCmds) == 0 {
						//next decoder
						break
					} else if decoder.Err != nil {
						tclog.Logger.Debug(fmt.Sprintf("DECODER ERR OCCUR,but have %d cmds\n", len(decoder.ReqCmds)))
					}

					if c.GetOnlyBigReq() > 0 {
						//如果是一个pipeline,命令超过阈值,则打印
						cmdsCnt := len(decoder.ReqCmds)
						if cmdsCnt > c.GetOnlyBigReq() {
							goto CONTINUTE_PRINT
						}
						keepCmds := []CmdState{}
						for _, state01 := range decoder.ReqCmds {
							if state01.KeyCnt < c.GetOnlyBigReq() && state01.ValCnt < c.GetOnlyBigReq() {
								//keys个数 和 value个数都不大,则跳过
								continue
							}
							keepCmds = append(keepCmds, state01)
						}
						decoder.ReqCmds = keepCmds
					}
					if c.GetOnlyBigVal() > 0 {
						keepCmds := []CmdState{}
						for _, state01 := range decoder.ReqCmds {
							if uint32(len(state01.MaxKey)) < c.GetOnlyBigVal() && uint32(len(state01.MaxVal)) < c.GetOnlyBigVal() {
								//最大的key 和 最大value长度都不大,则跳过
								continue
							}
							keepCmds = append(keepCmds, state01)
						}
						decoder.ReqCmds = keepCmds
					}
					if len(decoder.ReqCmds) == 0 {
						continue
					}
				CONTINUTE_PRINT:
					//发送数据
					select {
					case c.retChan <- decoder:
					case <-c.done:
						return
					}
				case <-c.done:
					return
				}
			}
		}()
	}
}

//打印解析的命令
func (c *RedisCapture) OutputCmds() {
	for woker := 0; woker < 30; woker++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			bufWriter := bufio.NewWriter(os.Stdout)
			defer bufWriter.Flush()
			var ok bool = false
			var decoder *RedisPacketDecoder
			for {
				select {
				case decoder, ok = <-c.retChan:
					if !ok {
						//retChan closed
						return
					}
					cmds := decoder.GetAllFormatedCmds()
					decoder.clearReqCmd()
					c.Outputer.WriteLines(cmds, false)
				case <-c.done:
					return
				}
			}
		}()
	}
}

type tcpSteam02 struct {
	net, transport gopacket.Flow
	tcp            *layers.TCP
	ac             reassembly.AssemblerContext
	tcpstate       *reassembly.TCPSimpleFSM
	optchecker     reassembly.TCPOptionCheck
	ident          string
}

func (t *tcpSteam02) Accept(tcp *layers.TCP, ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence, start *bool, ac reassembly.AssemblerContext) bool {
	return true
	// if !t.tcpstate.CheckState(tcp, dir) {
	// 	fmt.Printf("FSM %s: Packet rejected by FSM (state:%s)\n", t.ident, t.tcpstate.String())
	// 	return false
	// }
	// Options
	// err := t.optchecker.Accept(tcp, ci, dir, nextSeq, start)
	// if err != nil {
	// 	fmt.Printf("OptionChecker %s: Packet rejected by OptionChecker: %s\n", t.ident, err)
	// 	return false
	// }
	// Checksum
	// accept := true
	// c, err := tcp.ComputeChecksum()
	// if err != nil {
	// 	fmt.Printf("ChecksumCompute %s: Got error computing checksum: %v\n", t.ident, err)
	// 	accept = false
	// } else if c != 0x0 {
	// 	fmt.Printf("Checksum %s: Invalid checksum: 0x%x\n", t.ident, c)
	// 	accept = false
	// }
	// return accept
}
func (h *tcpSteam02) ReassembledSG(sg reassembly.ScatterGather, ac reassembly.AssemblerContext) {
	srcIP := h.net.Src().String()
	dstIP := h.net.Dst().String()
	srcPort, _ := strconv.Atoi(h.transport.Src().String())
	dstPort, _ := strconv.Atoi(h.transport.Dst().String())

	length, _ := sg.Lengths()
	data := sg.Fetch(length)
	decoder := NewRedisPacketDecoder(srcIP, dstIP, srcPort, dstPort, data, time.Now())
	select {
	case GlobalCapture.genChan <- decoder:
	case <-GlobalCapture.done:
		return
	}
}
func (t *tcpSteam02) ReassemblyComplete(ac reassembly.AssemblerContext) bool {
	return true
}

type tcpStreamFactory02 struct{}

func (h *tcpStreamFactory02) New(net, transport gopacket.Flow, tcp *layers.TCP, ac reassembly.AssemblerContext) reassembly.Stream {
	fsmOptions := reassembly.TCPSimpleFSMOptions{
		SupportMissingEstablishment: true,
	}
	hstream := &tcpSteam02{
		net:        net,
		transport:  transport,
		tcp:        tcp,
		ac:         ac,
		tcpstate:   reassembly.NewTCPSimpleFSM(fsmOptions),
		optchecker: reassembly.NewTCPOptionCheck(),
		ident:      fmt.Sprintf("%s:%s", net, transport),
	}
	return hstream
}

/*
 * The assembler context
 */
type Context struct {
	CaptureInfo gopacket.CaptureInfo
}

func (c *Context) GetCaptureInfo() gopacket.CaptureInfo {
	return c.CaptureInfo
}

func (c *RedisCapture) ExecuteV3() {
	version := pcap.Version()
	fmt.Printf("version=%s\n", version)
	fmt.Printf("device=%s\n", c.GetDevice())
	handle, err := pcap.OpenLive(c.GetDevice(), int32(c.GetMaxPacket()), true, pcap.BlockForever)
	if err != nil {
		panic(err)
	}
	defer handle.Close()
	defer c.Outputer.Close()
	c.ParallelDecode()
	c.OutputCmds()
	c.FlushOutput()

	handle.SetBPFFilter(fmt.Sprintf("dst port %d", c.GetDstPort()))

	// Set up assembly
	streamFactory := &tcpStreamFactory02{}
	streamPool := reassembly.NewStreamPool(streamFactory)

	assemblerCnt := c.GetGopacketAssemblerCnt()
	assemblerList := []*reassembly.Assembler{}
	packetCntList := make([]int64, assemblerCnt)
	// packetMutList := make([]sync.Mutex, assemblerCnt)
	for i := 0; i < assemblerCnt; i++ {
		assembler00 := reassembly.NewAssembler(streamPool)
		assembler00.MaxBufferedPagesPerConnection = c.GetGopacketMaxPagesPerConn()
		assembler00.MaxBufferedPagesTotal = c.GetGopacketMaxPagesTotal()
		assemblerList = append(assemblerList, assembler00)
	}
	var s01 int64 = 0
	defer func() {
		fmt.Printf("all size:%d\n", s01)
		fmt.Printf("packet size:%d\n", packetSize)
	}()

	// Read in packets, pass to assembler.
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packetSource.NoCopy = true
	packets := packetSource.Packets()
	// ticker := time.Tick(time.Millisecond * time.Duration(c.TcpFlushInterval))
	timer := time.NewTimer(time.Second * time.Duration(c.Timeout))
	var breakMain bool = false
	var index int64 = 0
	for {
		select {
		case packet := <-packets:
			index++
			// log.Println(packet)
			if packet.NetworkLayer() == nil || packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {
				log.Println("Unusable packet")
				continue
			}
			tcp := packet.TransportLayer().(*layers.TCP)
			err := tcp.SetNetworkLayerForChecksum(packet.NetworkLayer())
			if err != nil {
				log.Fatalf("Failed to set network layer for checksum: %s\n", err)
			}
			c01 := Context{
				CaptureInfo: packet.Metadata().CaptureInfo,
			}
			//将数据包进行重组
			idx := index % int64(assemblerCnt)
			assemblerList[idx].AssembleWithContext(packet.NetworkLayer().NetworkFlow(), tcp, &c01)
			packetCntList[idx]++
			// 每个assembler 处理到一定数量的packet后,则flush
			// if packetCntList[idx]%int64(flushEvePacketCnt) == 0 {
			// 	ref := packet.Metadata().CaptureInfo.Timestamp
			// 	assemblerList[idx].FlushWithOptions(reassembly.FlushOptions{
			// 		T:  ref.Add(time.Millisecond * -10),
			// 		TC: ref.Add(time.Second * -2),
			// 	})
			// }
			atomic.AddInt64(&s01, int64(len(tcp.Payload)))
		// case <-ticker:
		// 	//每隔一段时间，刷新之前不活跃的连接
		// 	ref := time.Now().Local()
		// 	for i := 0; i < assemblerCnt; i++ {
		// 		// assemblerList[i].FlushWithOptions(reassembly.FlushOptions{
		// 		// 	T:  ref.Add(time.Millisecond * time.Duration(-c.TcpFlushInterval)),
		// 		// 	TC: ref.Add(closeTimeout),
		// 		// })
		// 	}
		// 	// end := time.Now().Local()
		// 	// fmt.Printf("flush assembler00 cost %.d ms\n", end.Sub(ref).Microseconds())
		case <-timer.C:
			for i := 0; i < assemblerCnt; i++ {
				assemblerList[i].FlushAll()
			}
			time.Sleep(2 * time.Second)
			close(c.done)
			breakMain = true
			break
		}
		if breakMain == true {
			break
		}
	}
	c.wg.Wait()
}
