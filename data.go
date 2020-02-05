package qdata

import (
	"bytes"
	"encoding/binary"
)

type Data struct {
	//代码
	Security string
	//时间
	Time int64
	//开盘价
	Open float64
	//最高价
	High float64
	//最低价
	Low float64
	//收盘价
	Close float64
	//昨收价
	Pclose float64
	//成交量
	Volume int64
	//成交额
	Amount float64
}

func (d *Data) Encode() []byte {
	buf := new(bytes.Buffer)
	codeBuf := bytes.NewBufferString(d.Security)
	binary.Write(buf, binary.LittleEndian, int8(codeBuf.Len()))
	binary.Write(buf, binary.LittleEndian, codeBuf.Bytes())
	binary.Write(buf, binary.LittleEndian, d.Time)
	binary.Write(buf, binary.LittleEndian, d.Open)
	binary.Write(buf, binary.LittleEndian, d.High)
	binary.Write(buf, binary.LittleEndian, d.Low)
	binary.Write(buf, binary.LittleEndian, d.Close)
	binary.Write(buf, binary.LittleEndian, d.Pclose)
	binary.Write(buf, binary.LittleEndian, d.Volume)
	binary.Write(buf, binary.LittleEndian, d.Amount)

	return buf.Bytes()
}

func (d *Data) Decode(bytesData []byte) *Data {
	bufData := bytes.NewBuffer(bytesData)

	var secBytesLen int8
	binary.Read(bufData, binary.LittleEndian, &secBytesLen)

	secBytes := make([]byte, secBytesLen)
	binary.Read(bufData, binary.LittleEndian, &secBytes)
	d.Security = string(secBytes)

	binary.Read(bufData, binary.LittleEndian, &d.Time)
	binary.Read(bufData, binary.LittleEndian, &d.Open)
	binary.Read(bufData, binary.LittleEndian, &d.High)
	binary.Read(bufData, binary.LittleEndian, &d.Low)
	binary.Read(bufData, binary.LittleEndian, &d.Close)
	binary.Read(bufData, binary.LittleEndian, &d.Pclose)
	binary.Read(bufData, binary.LittleEndian, &d.Volume)
	binary.Read(bufData, binary.LittleEndian, &d.Amount)
	return d
}

func (d *Data) getDate() int32 {
	return int32(d.Time / 1000000)
}
