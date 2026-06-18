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
	SubTransferEnabled     bool   `json:"启用小号转存取链" type:"bool" default:"false" help:"开启后，下载/播放会先秒传文件至小号，再使用小号获取链接"`
	SubCookies             string `json:"小号 Cookie" type:"text" help:"一行一个 115 Cookie"`
	SubFolder              string `json:"小号转存目录" default:"tmp" help:"小号网盘中用于存放秒传文件的目录"`
	SubFallbackToMain      bool   `json:"转存失败回退主号" type:"bool" default:"true" help:"小号全部不可用时，是否允许回退到主号获取链接"`
	SubAutoCleanupEnabled bool   `json:"启用自动清理" type:"bool" default:"true" help:"定时清理转存记录，并清理小号转存目录中过旧文件"`
	SubAutoCleanupDays    int64  `json:"自动清理保留天数" type:"number" default:"3" help:"转存记录和小号转存目录中文件的保留天数"`
	SubAutoCleanupCron    string `json:"自动清理运行时间" type:"text" default:"0 0 * * *" help:"cron 表达式"`
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
