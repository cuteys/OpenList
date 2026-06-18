package handles

import (
	"fmt"
	"strings"
	"time"

	driver115 "github.com/OpenListTeam/OpenList/v4/drivers/115"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

func ListDriverInfo(c *gin.Context) {
	common.SuccessResp(c, op.GetDriverInfoMap())
}

func ListDriverNames(c *gin.Context) {
	common.SuccessResp(c, op.GetDriverNames())
}

func GetDriverInfo(c *gin.Context) {
	driverName := c.Query("driver")
	infoMap := op.GetDriverInfoMap()
	items, ok := infoMap[driverName]
	if !ok {
		common.ErrorStrResp(c, fmt.Sprintf("driver [%s] not found", driverName), 404)
		return
	}
	common.SuccessResp(c, items)
}

func Get115Runtime(c *gin.Context) {
	var req runtime115Query
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.normalize()
	from, to, err := req.parseTimeRange()
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	pan, err := get115RuntimeDriver(req.StorageID)
	if err != nil {
		if strings.Contains(err.Error(), "storage is not 115 Cloud") {
			common.ErrorStrResp(c, err.Error(), 400)
			return
		}
		common.ErrorResp(c, err, 500, true)
		return
	}
	snapshot, err := pan.RuntimeSnapshotWithOptions(req.Hours, from, to, req.Kind, req.Detail)
	if err != nil {
		common.ErrorStrResp(c, err.Error(), 400)
		return
	}
	common.SuccessResp(c, snapshot)
}

type runtime115Query struct {
	StorageID uint   `form:"storage_id" binding:"required"`
	Hours     int64  `form:"hours"`
	From      string `form:"from"`
	To        string `form:"to"`
	Kind      string `form:"kind"`
	Detail    string `form:"detail"`
}

func (q *runtime115Query) normalize() {
	q.Kind = strings.TrimSpace(q.Kind)
	q.Detail = strings.TrimSpace(q.Detail)
	q.From = strings.TrimSpace(q.From)
	q.To = strings.TrimSpace(q.To)
}

func (q runtime115Query) parseTimeRange() (*time.Time, *time.Time, error) {
	var from, to *time.Time
	if q.From != "" {
		t, err := parseRuntimeTime(q.From)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid from: %w", err)
		}
		from = &t
	}
	if q.To != "" {
		t, err := parseRuntimeTime(q.To)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid to: %w", err)
		}
		to = &t
	}
	return from, to, nil
}

func parseRuntimeTime(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
	}
	var err error
	for _, layout := range layouts {
		var t time.Time
		t, err = time.Parse(layout, value)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, err
}

func get115RuntimeDriver(storageID uint) (*driver115.Pan115, error) {
	if storageID == 0 {
		return nil, fmt.Errorf("missing storage_id")
	}
	storage, err := db.GetStorageById(storageID)
	if err != nil {
		return nil, err
	}
	if storage.Driver != "115 Cloud" {
		return nil, fmt.Errorf("storage is not 115 Cloud")
	}
	storageDriver, err := op.GetStorageByMountPath(storage.MountPath)
	if err != nil {
		return nil, err
	}
	pan, ok := storageDriver.(*driver115.Pan115)
	if !ok {
		return nil, fmt.Errorf("failed to resolve 115 Cloud runtime")
	}
	return pan, nil
}
