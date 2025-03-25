// Code generated by Thrift Compiler (0.17.0). DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	thrift "github.com/apache/thrift/lib/go/thrift"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var _ = rpc.GoUnusedProtection__

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nFunctions:")
	fmt.Fprintln(os.Stderr, "  void DataInit()")
	fmt.Fprintln(os.Stderr, "   CollectAttrBasicInfo(PartitionRef partition)")
	fmt.Fprintln(os.Stderr, "  PartitionInsBasic CollectInsBasicInfo(PartitionRef partition,  relatedFeatures)")
	fmt.Fprintln(os.Stderr, "  void BeforeGenAVC( nonNaNlabelWeights)")
	fmt.Fprintln(os.Stderr, "  void AfterSplit()")
	fmt.Fprintln(os.Stderr, "  void GenGeneralAVC(PartitionRef partition,  smallAVCTasks,  conciseAVCTasks)")
	fmt.Fprintln(os.Stderr, "   GenPartialAVC(PartitionRef partition, i32 featureId,  tasks)")
	fmt.Fprintln(os.Stderr, "  void Split(PartitionRef partition, i64 newPartitionId, i32 splitAttr, double splitValue, bool hasNaN)")
	fmt.Fprintln(os.Stderr, "  void MergeGeneralAVC(PartitionRef partition, AVC avc)")
	fmt.Fprintln(os.Stderr, "  void Clear()")
	fmt.Fprintln(os.Stderr, "  void Stop()")
	fmt.Fprintln(os.Stderr)
	os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
	var m map[string]string = h
	return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
	parts := strings.Split(value, ": ")
	if len(parts) != 2 {
		return fmt.Errorf("header should be of format 'Key: Value'")
	}
	h[parts[0]] = parts[1]
	return nil
}

func main() {
	flag.Usage = Usage
	var host string
	var port int
	var protocol string
	var urlString string
	var framed bool
	var useHttp bool
	headers := make(httpHeaders)
	var parsedUrl *url.URL
	var trans thrift.TTransport
	_ = strconv.Atoi
	_ = math.Abs
	flag.Usage = Usage
	flag.StringVar(&host, "h", "localhost", "Specify host and port")
	flag.IntVar(&port, "p", 9090, "Specify port")
	flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
	flag.StringVar(&urlString, "u", "", "Specify the url")
	flag.BoolVar(&framed, "framed", false, "Use framed transport")
	flag.BoolVar(&useHttp, "http", false, "Use http")
	flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
	flag.Parse()

	if len(urlString) > 0 {
		var err error
		parsedUrl, err = url.Parse(urlString)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
			flag.Usage()
		}
		host = parsedUrl.Host
		useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
	} else if useHttp {
		_, err := url.Parse(fmt.Sprint("http://", host, ":", port))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
			flag.Usage()
		}
	}

	cmd := flag.Arg(0)
	var err error
	var cfg *thrift.TConfiguration = nil
	if useHttp {
		trans, err = thrift.NewTHttpClient(parsedUrl.String())
		if len(headers) > 0 {
			httptrans := trans.(*thrift.THttpClient)
			for key, value := range headers {
				httptrans.SetHeader(key, value)
			}
		}
	} else {
		portStr := fmt.Sprint(port)
		if strings.Contains(host, ":") {
			host, portStr, err = net.SplitHostPort(host)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error with host:", err)
				os.Exit(1)
			}
		}
		trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error resolving address:", err)
			os.Exit(1)
		}
		if framed {
			trans = thrift.NewTFramedTransportConf(trans, cfg)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating transport", err)
		os.Exit(1)
	}
	defer trans.Close()
	var protocolFactory thrift.TProtocolFactory
	switch protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactoryConf(cfg)
		break
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactoryConf(cfg)
		break
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
		break
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryConf(cfg)
		break
	default:
		fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
		Usage()
		os.Exit(1)
	}
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	client := rpc.NewServeOnWorkerClient(thrift.NewTStandardClient(iprot, oprot))
	if err := trans.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
		os.Exit(1)
	}

	switch cmd {
	case "DataInit":
		if flag.NArg()-1 != 0 {
			fmt.Fprintln(os.Stderr, "DataInit requires 0 args")
			flag.Usage()
		}
		fmt.Print(client.DataInit(context.Background()))
		fmt.Print("\n")
		break
	case "CollectAttrBasicInfo":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "CollectAttrBasicInfo requires 1 args")
			flag.Usage()
		}
		arg109 := flag.Arg(1)
		mbTrans110 := thrift.NewTMemoryBufferLen(len(arg109))
		defer mbTrans110.Close()
		_, err111 := mbTrans110.WriteString(arg109)
		if err111 != nil {
			Usage()
			return
		}
		factory112 := thrift.NewTJSONProtocolFactory()
		jsProt113 := factory112.GetProtocol(mbTrans110)
		argvalue0 := rpc.NewPartitionRef()
		err114 := argvalue0.Read(context.Background(), jsProt113)
		if err114 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.CollectAttrBasicInfo(context.Background(), value0))
		fmt.Print("\n")
		break
	case "CollectInsBasicInfo":
		if flag.NArg()-1 != 2 {
			fmt.Fprintln(os.Stderr, "CollectInsBasicInfo requires 2 args")
			flag.Usage()
		}
		arg115 := flag.Arg(1)
		mbTrans116 := thrift.NewTMemoryBufferLen(len(arg115))
		defer mbTrans116.Close()
		_, err117 := mbTrans116.WriteString(arg115)
		if err117 != nil {
			Usage()
			return
		}
		factory118 := thrift.NewTJSONProtocolFactory()
		jsProt119 := factory118.GetProtocol(mbTrans116)
		argvalue0 := rpc.NewPartitionRef()
		err120 := argvalue0.Read(context.Background(), jsProt119)
		if err120 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		arg121 := flag.Arg(2)
		mbTrans122 := thrift.NewTMemoryBufferLen(len(arg121))
		defer mbTrans122.Close()
		_, err123 := mbTrans122.WriteString(arg121)
		if err123 != nil {
			Usage()
			return
		}
		factory124 := thrift.NewTJSONProtocolFactory()
		jsProt125 := factory124.GetProtocol(mbTrans122)
		containerStruct1 := rpc.NewServeOnWorkerCollectInsBasicInfoArgs()
		err126 := containerStruct1.ReadField2(context.Background(), jsProt125)
		if err126 != nil {
			Usage()
			return
		}
		argvalue1 := containerStruct1.RelatedFeatures
		value1 := argvalue1
		fmt.Print(client.CollectInsBasicInfo(context.Background(), value0, value1))
		fmt.Print("\n")
		break
	case "BeforeGenAVC":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "BeforeGenAVC requires 1 args")
			flag.Usage()
		}
		arg127 := flag.Arg(1)
		mbTrans128 := thrift.NewTMemoryBufferLen(len(arg127))
		defer mbTrans128.Close()
		_, err129 := mbTrans128.WriteString(arg127)
		if err129 != nil {
			Usage()
			return
		}
		factory130 := thrift.NewTJSONProtocolFactory()
		jsProt131 := factory130.GetProtocol(mbTrans128)
		containerStruct0 := rpc.NewServeOnWorkerBeforeGenAVCArgs()
		err132 := containerStruct0.ReadField1(context.Background(), jsProt131)
		if err132 != nil {
			Usage()
			return
		}
		argvalue0 := containerStruct0.NonNaNlabelWeights
		value0 := argvalue0
		fmt.Print(client.BeforeGenAVC(context.Background(), value0))
		fmt.Print("\n")
		break
	case "AfterSplit":
		if flag.NArg()-1 != 0 {
			fmt.Fprintln(os.Stderr, "AfterSplit requires 0 args")
			flag.Usage()
		}
		fmt.Print(client.AfterSplit(context.Background()))
		fmt.Print("\n")
		break
	case "GenGeneralAVC":
		if flag.NArg()-1 != 3 {
			fmt.Fprintln(os.Stderr, "GenGeneralAVC requires 3 args")
			flag.Usage()
		}
		arg133 := flag.Arg(1)
		mbTrans134 := thrift.NewTMemoryBufferLen(len(arg133))
		defer mbTrans134.Close()
		_, err135 := mbTrans134.WriteString(arg133)
		if err135 != nil {
			Usage()
			return
		}
		factory136 := thrift.NewTJSONProtocolFactory()
		jsProt137 := factory136.GetProtocol(mbTrans134)
		argvalue0 := rpc.NewPartitionRef()
		err138 := argvalue0.Read(context.Background(), jsProt137)
		if err138 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		arg139 := flag.Arg(2)
		mbTrans140 := thrift.NewTMemoryBufferLen(len(arg139))
		defer mbTrans140.Close()
		_, err141 := mbTrans140.WriteString(arg139)
		if err141 != nil {
			Usage()
			return
		}
		factory142 := thrift.NewTJSONProtocolFactory()
		jsProt143 := factory142.GetProtocol(mbTrans140)
		containerStruct1 := rpc.NewServeOnWorkerGenGeneralAVCArgs()
		err144 := containerStruct1.ReadField2(context.Background(), jsProt143)
		if err144 != nil {
			Usage()
			return
		}
		argvalue1 := containerStruct1.SmallAVCTasks
		value1 := argvalue1
		arg145 := flag.Arg(3)
		mbTrans146 := thrift.NewTMemoryBufferLen(len(arg145))
		defer mbTrans146.Close()
		_, err147 := mbTrans146.WriteString(arg145)
		if err147 != nil {
			Usage()
			return
		}
		factory148 := thrift.NewTJSONProtocolFactory()
		jsProt149 := factory148.GetProtocol(mbTrans146)
		containerStruct2 := rpc.NewServeOnWorkerGenGeneralAVCArgs()
		err150 := containerStruct2.ReadField3(context.Background(), jsProt149)
		if err150 != nil {
			Usage()
			return
		}
		argvalue2 := containerStruct2.ConciseAVCTasks
		value2 := argvalue2
		fmt.Print(client.GenGeneralAVC(context.Background(), value0, value1, value2))
		fmt.Print("\n")
		break
	case "GenPartialAVC":
		if flag.NArg()-1 != 3 {
			fmt.Fprintln(os.Stderr, "GenPartialAVC requires 3 args")
			flag.Usage()
		}
		arg151 := flag.Arg(1)
		mbTrans152 := thrift.NewTMemoryBufferLen(len(arg151))
		defer mbTrans152.Close()
		_, err153 := mbTrans152.WriteString(arg151)
		if err153 != nil {
			Usage()
			return
		}
		factory154 := thrift.NewTJSONProtocolFactory()
		jsProt155 := factory154.GetProtocol(mbTrans152)
		argvalue0 := rpc.NewPartitionRef()
		err156 := argvalue0.Read(context.Background(), jsProt155)
		if err156 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		tmp1, err157 := (strconv.Atoi(flag.Arg(2)))
		if err157 != nil {
			Usage()
			return
		}
		argvalue1 := int32(tmp1)
		value1 := argvalue1
		arg158 := flag.Arg(3)
		mbTrans159 := thrift.NewTMemoryBufferLen(len(arg158))
		defer mbTrans159.Close()
		_, err160 := mbTrans159.WriteString(arg158)
		if err160 != nil {
			Usage()
			return
		}
		factory161 := thrift.NewTJSONProtocolFactory()
		jsProt162 := factory161.GetProtocol(mbTrans159)
		containerStruct2 := rpc.NewServeOnWorkerGenPartialAVCArgs()
		err163 := containerStruct2.ReadField3(context.Background(), jsProt162)
		if err163 != nil {
			Usage()
			return
		}
		argvalue2 := containerStruct2.Tasks
		value2 := argvalue2
		fmt.Print(client.GenPartialAVC(context.Background(), value0, value1, value2))
		fmt.Print("\n")
		break
	case "Split":
		if flag.NArg()-1 != 5 {
			fmt.Fprintln(os.Stderr, "Split requires 5 args")
			flag.Usage()
		}
		arg164 := flag.Arg(1)
		mbTrans165 := thrift.NewTMemoryBufferLen(len(arg164))
		defer mbTrans165.Close()
		_, err166 := mbTrans165.WriteString(arg164)
		if err166 != nil {
			Usage()
			return
		}
		factory167 := thrift.NewTJSONProtocolFactory()
		jsProt168 := factory167.GetProtocol(mbTrans165)
		argvalue0 := rpc.NewPartitionRef()
		err169 := argvalue0.Read(context.Background(), jsProt168)
		if err169 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		argvalue1, err170 := (strconv.ParseInt(flag.Arg(2), 10, 64))
		if err170 != nil {
			Usage()
			return
		}
		value1 := argvalue1
		tmp2, err171 := (strconv.Atoi(flag.Arg(3)))
		if err171 != nil {
			Usage()
			return
		}
		argvalue2 := int32(tmp2)
		value2 := argvalue2
		argvalue3, err172 := (strconv.ParseFloat(flag.Arg(4), 64))
		if err172 != nil {
			Usage()
			return
		}
		value3 := argvalue3
		argvalue4 := flag.Arg(5) == "true"
		value4 := argvalue4
		fmt.Print(client.Split(context.Background(), value0, value1, value2, value3, value4))
		fmt.Print("\n")
		break
	case "MergeGeneralAVC":
		if flag.NArg()-1 != 2 {
			fmt.Fprintln(os.Stderr, "MergeGeneralAVC requires 2 args")
			flag.Usage()
		}
		arg174 := flag.Arg(1)
		mbTrans175 := thrift.NewTMemoryBufferLen(len(arg174))
		defer mbTrans175.Close()
		_, err176 := mbTrans175.WriteString(arg174)
		if err176 != nil {
			Usage()
			return
		}
		factory177 := thrift.NewTJSONProtocolFactory()
		jsProt178 := factory177.GetProtocol(mbTrans175)
		argvalue0 := rpc.NewPartitionRef()
		err179 := argvalue0.Read(context.Background(), jsProt178)
		if err179 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		arg180 := flag.Arg(2)
		mbTrans181 := thrift.NewTMemoryBufferLen(len(arg180))
		defer mbTrans181.Close()
		_, err182 := mbTrans181.WriteString(arg180)
		if err182 != nil {
			Usage()
			return
		}
		factory183 := thrift.NewTJSONProtocolFactory()
		jsProt184 := factory183.GetProtocol(mbTrans181)
		argvalue1 := rpc.NewAVC()
		err185 := argvalue1.Read(context.Background(), jsProt184)
		if err185 != nil {
			Usage()
			return
		}
		value1 := argvalue1
		fmt.Print(client.MergeGeneralAVC(context.Background(), value0, value1))
		fmt.Print("\n")
		break
	case "Clear":
		if flag.NArg()-1 != 0 {
			fmt.Fprintln(os.Stderr, "Clear requires 0 args")
			flag.Usage()
		}
		fmt.Print(client.Clear(context.Background()))
		fmt.Print("\n")
		break
	case "Stop":
		if flag.NArg()-1 != 0 {
			fmt.Fprintln(os.Stderr, "Stop requires 0 args")
			flag.Usage()
		}
		fmt.Print(client.Stop(context.Background()))
		fmt.Print("\n")
		break
	case "":
		Usage()
		break
	default:
		fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
	}
}
