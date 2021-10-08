/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"myRedisCapture/pkg/capture"
	"myRedisCapture/tclog"
	"os"
	"sync"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string
var device string
var dstPort int
var maxPacket uint32
var onlyBigReq int
var onlyBigVal uint32
var printValLength bool
var topHotKeys int
var timeout int
var gopacketAssemblerCnt int
var gopacketMaxPagesPerConn int
var gopacketMaxPagesTotal int
var decoderThreadCnt int
var globalStore sync.Map
var outputFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "myredisCapture",
	Short: "Capturing redis request packets, parsing, and filtering,ouputing",
	Long: `Redis can get the executed commands through the 'monitor'. 
To capturing request commands and client ips are a common requirement for the proxy, and most redis proxies do not support the 'monitor' command.
	
If proxy requests are very frequent, the tool cannot capture them completely.
If the tool occupies too much memory, you can reduce the memory usage by reducing gopacket-max-pages-per-conn and gopacket-max-pages-total.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		capture.GlobalCapture.InitRedisCapture().
			SetDevice(device).
			SetDstPort(dstPort).
			SetMaxPacket(maxPacket).
			SetOnlyBigReq(onlyBigReq).
			SetOnlyBigVal(onlyBigVal).
			SetPrintValLength(printValLength).
			SetTopHotKeys(topHotKeys).
			SetTimeout(timeout).
			SetGopacketAssemblerCnt(gopacketAssemblerCnt).
			SetGopacketMaxPagesPerConn(gopacketMaxPagesPerConn).
			SetGopacketMaxPagesTotal(gopacketMaxPagesTotal).
			SetDecoderLimit(decoderThreadCnt).
			SetOutputFile(outputFile)
		if capture.GlobalCapture.Err != nil {
			return
		}
		fmt.Printf(`
device:%s
dstPort:%d
maxPacket:%d
onlyBigReq:%d
onlyBigVal:%d
printValLength:%v
topHotKeys:%d
timeout:%d
decoterThreadCnt:%d
gopacketAssemblerCnt:%d
gopacketMaxPagesPerConn:%d
gopacketMaxPagesTotal:%d
outputFile:%s
`,
			capture.GlobalCapture.GetDevice(),
			capture.GlobalCapture.GetDstPort(),
			capture.GlobalCapture.GetMaxPacket(),
			capture.GlobalCapture.GetOnlyBigReq(),
			capture.GlobalCapture.GetOnlyBigVal(),
			capture.GlobalCapture.GetPrintValLength(),
			capture.GlobalCapture.GetTopHotKeys(),
			capture.GlobalCapture.GetTimeout(),
			capture.GlobalCapture.GetDecoderLimit(),
			capture.GlobalCapture.GetGopacketAssemblerCnt(),
			capture.GlobalCapture.GetGopacketMaxPagesPerConn(),
			capture.GlobalCapture.GetGopacketMaxPagesTotal(),
			capture.GlobalCapture.GetOutputFile())
		capture.GlobalCapture.ExecuteV3()
		return
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(tclog.InitStdoutLog)

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.myredisCapture.yaml)")
	rootCmd.PersistentFlags().StringVar(&device, "device", "", "required,network device. e.g. eth1")
	rootCmd.MarkPersistentFlagRequired("device")
	viper.BindPFlag("device", rootCmd.PersistentFlags().Lookup("device"))

	rootCmd.PersistentFlags().IntVar(&dstPort, "dst-port", 0, "required,redis (dst) port(s). e.g. 6379")
	rootCmd.MarkPersistentFlagRequired("dst-port")
	viper.BindPFlag("dst-port", rootCmd.PersistentFlags().Lookup("dst-port"))

	rootCmd.PersistentFlags().Uint32Var(&maxPacket, "max-packet", 1048576, "default 1048576,max redis command length in byte.")
	viper.BindPFlag("max-packet", rootCmd.PersistentFlags().Lookup("max-packet"))

	rootCmd.PersistentFlags().IntVar(&onlyBigReq, "only-big-req", 0, "default 0,no limit.Only grab requests that process multiple keys > {--only-big-req}, such as mset or mget or pipeline")
	viper.BindPFlag("only-big-req", rootCmd.PersistentFlags().Lookup("only-big-req"))

	rootCmd.PersistentFlags().Uint32Var(&onlyBigVal, "only-big-val", 0, "default 0,no limit.only grab requests with a large value")
	viper.BindPFlag("only-big-val", rootCmd.PersistentFlags().Lookup("only-big-val"))

	rootCmd.PersistentFlags().BoolVar(&printValLength, "print-val-length", false, "only print value length without data")
	viper.BindPFlag("print-val-length", rootCmd.PersistentFlags().Lookup("print-vallength"))

	rootCmd.PersistentFlags().BoolVar(&printValLength, "top-hot-keys", false, "Print the top frequently accessed keys")
	viper.BindPFlag("top-hot-keys", rootCmd.PersistentFlags().Lookup("top-hot-keys"))

	rootCmd.PersistentFlags().IntVar(&decoderThreadCnt, "decoder-thread-cnt", 20, "default 20,decoder threads count")
	viper.BindPFlag("decoder-thread-cnt", rootCmd.PersistentFlags().Lookup("decoder-thread-cnt"))

	rootCmd.PersistentFlags().IntVar(&timeout, "timeout", 0, "default 0,no limit.duration of capture in seconds.")
	viper.BindPFlag("timeout", rootCmd.PersistentFlags().Lookup("timeout"))

	rootCmd.PersistentFlags().IntVar(&gopacketAssemblerCnt, "gopacket-assembler-cnt", 20, "default 20.assermber count.url: https://pkg.go.dev/github.com/google/gopacket@v1.1.19/reassembly")
	viper.BindPFlag("gopacket-assembler-cnt", rootCmd.PersistentFlags().Lookup("gopacket-assembler-cnt"))

	rootCmd.PersistentFlags().IntVar(&gopacketMaxPagesPerConn, "gopacket-max-pages-per-conn", 500, "default 500. reassembly  MaxBufferedPagesPerConnection")
	viper.BindPFlag("gopacket-max-pages-per-conn", rootCmd.PersistentFlags().Lookup("gopacket-max-pages-per-conn"))

	rootCmd.PersistentFlags().IntVar(&gopacketMaxPagesTotal, "gopacket-max-pages-total", 100000, "default 100000.reassembly  MaxBufferedPagesTotal")
	viper.BindPFlag("gopacket-max-pages-total", rootCmd.PersistentFlags().Lookup("gopacket-max-pages-total"))

	rootCmd.PersistentFlags().StringVar(&outputFile, "output-file", "", "store output into file path..")
	viper.BindPFlag("output-file", rootCmd.PersistentFlags().Lookup("output-file"))

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	viper.SetDefault("max-packet", 1048576)
	viper.SetDefault("decoder-thread-cnt", 20)
	viper.SetDefault("gopacket-assembler-cnt", 20)
	viper.SetDefault("gopacket-max-pages-per-conn", 2)
	viper.SetDefault("gopacket-max-pages-total", 2)
	viper.BindEnv("MYREDIS_CAPTURE_DEBUG")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".myredisCapture" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".myredisCapture")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
