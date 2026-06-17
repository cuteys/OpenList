package _115

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	Cookie       string  `json:"cookie" type:"text" help:"one of QR code token and cookie required"`
	QRCodeToken  string  `json:"qrcode_token" type:"text" help:"one of QR code token and cookie required"`
	QRCodeSource string  `json:"qrcode_source" type:"select" options:"web,android,ios,tv,alipaymini,wechatmini,qandroid" default:"linux" help:"select the QR code device, default linux"`
	PageSize     int64   `json:"page_size" type:"number" default:"1000" help:"list api per page size of 115 driver"`
	LimitRate    float64 `json:"limit_rate" type:"float" default:"2" help:"limit all api request rate ([limit]r/1s)"`
	driver.RootID
	SubTransferEnabled     bool   `json:"启用小号转存取链" type:"bool" default:"false" help:"开启后，下载/播放会先在小号转存目录中复用或秒传同名文件，再使用小号获取链接"`
	SubCookies             string `json:"小号 Cookie" type:"text" help:"一行一个 115 小号 Cookie。配置后，播放/下载会先把文件转存到小号，再使用小号获取链接"`
	SubFolder              string `json:"小号转存目录" default:"tmp" help:"小号网盘中用于存放转存文件的目录，会自动创建"`
	SubFallbackToMain      bool   `json:"转存失败回退主号" type:"bool" default:"true" help:"小号不可用或转存失败时，允许继续使用主号获取链接"`
	SubTransferCacheExpire int64  `json:"转存结果缓存时间(秒)" type:"number" default:"7200" help:"秒传/复用成功后，记住主号文件对应的小号文件，避免重复转存。不是播放直链缓存，默认 7200 秒(2 小时)"`
}

var config = driver.Config{
	Name:          "115 Cloud",
	DefaultRoot:   "0",
	LinkCacheMode: driver.LinkCacheUA,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Pan115{}
	})
}
