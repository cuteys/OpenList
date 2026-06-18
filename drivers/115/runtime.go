package _115

import (
	"errors"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type runtimeRegistryKind string

const (
	runtimeRegistryTransfer runtimeRegistryKind = "transfer_registry"
	runtimeRegistryLink     runtimeRegistryKind = "direct_link_cache"
)

type runtimeAccountRole string

const (
	runtimeAccountSub1 runtimeAccountRole = "sub_1"
	runtimeAccountSub2 runtimeAccountRole = "sub_2"
)

type runtimeMemoryRow struct {
	Key         string              `json:"key,omitempty"`
	Kind        runtimeRegistryKind `json:"kind"`
	FileName    string              `json:"file_name,omitempty"`
	FilePath    string              `json:"file_path,omitempty"`
	FileID      string              `json:"file_id,omitempty"`
	PickCode    string              `json:"pick_code,omitempty"`
	Sha1        string              `json:"sha1,omitempty"`
	Size        int64               `json:"size,omitempty"`
	UserAgent   string              `json:"user_agent,omitempty"`
	AccountRole string              `json:"account_role,omitempty"`
	SubFileID   string              `json:"sub_file_id,omitempty"`
	SubPickCode string              `json:"sub_pick_code,omitempty"`
	SubFileName string              `json:"sub_file_name,omitempty"`
	SubRootID   string              `json:"sub_root_id,omitempty"`
	Source      string              `json:"source,omitempty"`
	CreatedAt   *time.Time          `json:"created_at,omitempty"`
	LastSeenAt  *time.Time          `json:"last_seen_at,omitempty"`
	ExpiresAt   *time.Time          `json:"expires_at,omitempty"`
	TTLSeconds  *int64              `json:"ttl_remaining_seconds,omitempty"`
}

type runtimeRecentError struct {
	Time    time.Time `json:"time"`
	Stage   string    `json:"stage"`
	Account string    `json:"account,omitempty"`
	Message string    `json:"message"`
}

type runtimeSubAccountState struct {
	Index             int        `json:"index"`
	Label             string     `json:"label"`
	Available         bool       `json:"available"`
	LoginStatus       string     `json:"login_status"`
	CooldownUntil     *time.Time `json:"cooldown_until,omitempty"`
	CoolingDown       bool       `json:"cooling_down"`
	LastError         string     `json:"last_error,omitempty"`
	LastErrorAt       *time.Time `json:"last_error_at,omitempty"`
	TransferCount     int64      `json:"transfer_count"`
	TransferLinkCount int64      `json:"transfer_link_count"`
	RootID            string     `json:"root_id,omitempty"`
}

type runtimeSummary struct {
	StorageID                uint                     `json:"storage_id"`
	MountPath                string                   `json:"mount_path"`
	MainAccountAvailable     bool                     `json:"main_account_available"`
	MainAccountStatus        string                   `json:"main_account_status,omitempty"`
	SubTransferEnabled       bool                     `json:"sub_transfer_enabled"`
	SubFallbackToMain        bool                     `json:"sub_fallback_to_main"`
	SubAccountCount          int                      `json:"sub_account_count"`
	UsableSubAccountCount    int                      `json:"usable_sub_account_count"`
	TransferRegistryCount    int                      `json:"transfer_registry_count"`
	DirectLinkCacheCount     int                      `json:"direct_link_cache_count"`
	RuntimeEntryCount        int                      `json:"runtime_entry_count"`
	FilteredRowCount         int                      `json:"filtered_row_count"`
	RecentErrors             []runtimeRecentError     `json:"recent_errors"`
	SubAccounts              []runtimeSubAccountState `json:"sub_accounts"`
	AutoCleanupEnabled       bool                     `json:"auto_cleanup_enabled"`
	AutoCleanupRetentionDays int64                    `json:"auto_cleanup_retention_days"`
	AutoCleanupCron          string                   `json:"auto_cleanup_cron"`
	RangeFrom                *time.Time               `json:"range_from,omitempty"`
	RangeTo                  *time.Time               `json:"range_to,omitempty"`
	KindFilter               string                   `json:"kind_filter,omitempty"`
	DetailMode               string                   `json:"detail_mode,omitempty"`
	LastCleanupAt            *time.Time               `json:"last_cleanup_at,omitempty"`
	LastCleanupError         string                   `json:"last_cleanup_error,omitempty"`
}

type runtimeSnapshot struct {
	Summary runtimeSummary     `json:"summary"`
	Rows    []runtimeMemoryRow `json:"rows"`
}

type runtimeRowKindFilter string

const (
	runtimeRowKindAll      runtimeRowKindFilter = "all"
	runtimeRowKindTransfer runtimeRowKindFilter = "transfer_registry"
	runtimeRowKindLink     runtimeRowKindFilter = "direct_link_cache"
)

type runtimeDetailMode string

const (
	runtimeDetailBasic runtimeDetailMode = "basic"
	runtimeDetailFull  runtimeDetailMode = "full"
)

type runtimeSnapshotOptions struct {
	Now    time.Time
	From   *time.Time
	To     *time.Time
	Kind   runtimeRowKindFilter
	Detail runtimeDetailMode
}

type driverRuntimeState struct {
	d *Pan115

	mu               sync.RWMutex
	transferRows     map[string]*runtimeMemoryRow
	subStates        map[int]*runtimeSubAccountState
	recentErrors     []runtimeRecentError
	lastCleanupAt    *time.Time
	lastCleanupError string
}

func newDriverRuntimeState(d *Pan115) *driverRuntimeState {
	return &driverRuntimeState{
		d:            d,
		transferRows: make(map[string]*runtimeMemoryRow),
		subStates:    make(map[int]*runtimeSubAccountState),
	}
}

func (s *driverRuntimeState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transferRows = make(map[string]*runtimeMemoryRow)
	s.subStates = make(map[int]*runtimeSubAccountState)
	s.recentErrors = nil
	s.lastCleanupAt = nil
	s.lastCleanupError = ""
}

func (s *driverRuntimeState) ensureSubState(index int) *runtimeSubAccountState {
	state, ok := s.subStates[index]
	if ok {
		return state
	}
	label := "sub"
	switch index {
	case 1:
		label = string(runtimeAccountSub1)
	case 2:
		label = string(runtimeAccountSub2)
	default:
		label = "sub_" + strconv.Itoa(index)
	}
	state = &runtimeSubAccountState{
		Index:       index,
		Label:       label,
		LoginStatus: "unknown",
	}
	s.subStates[index] = state
	return state
}

func (s *driverRuntimeState) recordSubLogin(index int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensureSubState(index)
	now := time.Now()
	if err == nil {
		state.LoginStatus = "ok"
		state.Available = true
		state.LastError = ""
		state.LastErrorAt = nil
		return
	}
	state.LoginStatus = "login_failed"
	state.Available = false
	state.LastError = err.Error()
	state.LastErrorAt = &now
	s.appendRecentErrorLocked("sub_login", state.Label, err)
}

func (s *driverRuntimeState) recordSubState(account *subAccount) {
	if account == nil {
		return
	}
	account.stateMu.Lock()
	until := account.unavailableUntil
	lastErr := account.lastError
	lastErrAt := account.lastErrorAt
	rootID := account.rootID
	account.stateMu.Unlock()

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensureSubState(account.index)
	state.LoginStatus = "ok"
	state.Available = until.IsZero() || !now.Before(until)
	if !until.IsZero() {
		state.CooldownUntil = &until
		state.CoolingDown = now.Before(until)
	} else {
		state.CooldownUntil = nil
		state.CoolingDown = false
	}
	state.LastError = lastErr
	if !lastErrAt.IsZero() {
		t := lastErrAt
		state.LastErrorAt = &t
	}
	state.RootID = rootID
}

func (s *driverRuntimeState) recordSubError(account *subAccount, stage string, err error) {
	if account == nil || err == nil {
		return
	}
	now := time.Now()
	account.stateMu.Lock()
	account.lastError = err.Error()
	account.lastErrorAt = now
	account.stateMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensureSubState(account.index)
	state.LastError = err.Error()
	state.LastErrorAt = &now
	s.appendRecentErrorLocked(stage, state.Label, err)
}

func (s *driverRuntimeState) recordTransferResult(file *FileObj, account *subAccount, result *subTransferResult, userAgent string, source string) {
	if file == nil || account == nil || result == nil {
		return
	}
	now := time.Now()
	row := &runtimeMemoryRow{
		Key:         transferRuntimeKey(file, userAgent),
		Kind:        runtimeRegistryTransfer,
		FileName:    file.GetName(),
		FilePath:    file.GetPath(),
		FileID:      file.GetID(),
		PickCode:    file.PickCode,
		Sha1:        strings.ToUpper(file.Sha1),
		Size:        file.GetSize(),
		UserAgent:   userAgent,
		AccountRole: subAccountRoleLabel(account.index),
		SubFileID:   result.fileID,
		SubPickCode: result.pickCode,
		SubFileName: result.fileName,
		SubRootID:   account.rootID,
		Source:      source,
		CreatedAt:   &now,
		LastSeenAt:  &now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.transferRows[row.Key]; ok {
		row.CreatedAt = existing.CreatedAt
	}
	s.transferRows[row.Key] = row
	state := s.ensureSubState(account.index)
	state.LoginStatus = "ok"
	state.Available = true
	state.TransferCount++
}

func (s *driverRuntimeState) recordTransferLinkUse(account *subAccount) {
	if account == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensureSubState(account.index)
	state.TransferLinkCount++
}

func (s *driverRuntimeState) touchTransfer(file *FileObj, userAgent string) {
	if file == nil {
		return
	}
	key := transferRuntimeKey(file, userAgent)
	s.mu.Lock()
	defer s.mu.Unlock()
	if row, ok := s.transferRows[key]; ok {
		now := time.Now()
		row.LastSeenAt = &now
	}
}

func (s *driverRuntimeState) deleteTransferByResult(result *subTransferResult) {
	if result == nil || result.file == nil {
		return
	}
	key := transferRuntimeKey(result.file, result.userAgent)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.transferRows, key)
}

func (s *driverRuntimeState) trimTransferRows(before time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, row := range s.transferRows {
		if row == nil {
			delete(s.transferRows, key)
			continue
		}
		if row.LastSeenAt != nil && row.LastSeenAt.Before(before) {
			delete(s.transferRows, key)
		}
	}
}

func (s *driverRuntimeState) setCleanupResult(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.lastCleanupAt = &now
	if err != nil {
		s.lastCleanupError = err.Error()
		s.appendRecentErrorLocked("cleanup", "", err)
		return
	}
	s.lastCleanupError = ""
}

func (s *driverRuntimeState) appendRecentErrorLocked(stage, account string, err error) {
	if err == nil {
		return
	}
	s.recentErrors = append([]runtimeRecentError{{
		Time:    time.Now(),
		Stage:   stage,
		Account: account,
		Message: err.Error(),
	}}, s.recentErrors...)
	if len(s.recentErrors) > 20 {
		s.recentErrors = s.recentErrors[:20]
	}
}

func (s *driverRuntimeState) snapshot() runtimeSnapshot {
	s.mu.RLock()
	transferRows := make([]runtimeMemoryRow, 0, len(s.transferRows))
	for _, row := range s.transferRows {
		if row == nil {
			continue
		}
		transferRows = append(transferRows, *row)
	}
	subAccounts := make([]runtimeSubAccountState, 0, len(s.subStates))
	usableSubs := 0
	for _, state := range s.subStates {
		if state == nil {
			continue
		}
		copyState := *state
		if copyState.Available && !copyState.CoolingDown && copyState.LoginStatus == "ok" {
			usableSubs++
		}
		subAccounts = append(subAccounts, copyState)
	}
	recentErrors := append([]runtimeRecentError(nil), s.recentErrors...)
	lastCleanupAt := s.lastCleanupAt
	lastCleanupError := s.lastCleanupError
	s.mu.RUnlock()

	sort.Slice(subAccounts, func(i, j int) bool {
		return subAccounts[i].Index < subAccounts[j].Index
	})

	linkRows := s.collectLinkCacheRows()
	rows := append(make([]runtimeMemoryRow, 0, len(transferRows)+len(linkRows)), transferRows...)
	rows = append(rows, linkRows...)
	sort.Slice(rows, func(i, j int) bool {
		ti := rows[i].LastSeenTime()
		tj := rows[j].LastSeenTime()
		if ti.Equal(tj) {
			if rows[i].Kind == rows[j].Kind {
				return rows[i].Key < rows[j].Key
			}
			return rows[i].Kind < rows[j].Kind
		}
		return ti.After(tj)
	})

	summary := runtimeSummary{
		StorageID:                s.d.GetStorage().ID,
		MountPath:                s.d.GetStorage().MountPath,
		MainAccountAvailable:     s.d.GetStorage().Status == op.WORK,
		MainAccountStatus:        s.d.GetStorage().Status,
		SubTransferEnabled:       s.d.SubTransferEnabled,
		SubFallbackToMain:        s.d.SubFallbackToMain,
		SubAccountCount:          len(subAccounts),
		UsableSubAccountCount:    usableSubs,
		TransferRegistryCount:    len(transferRows),
		DirectLinkCacheCount:     len(linkRows),
		RuntimeEntryCount:        len(rows),
		RecentErrors:             recentErrors,
		SubAccounts:              subAccounts,
		AutoCleanupEnabled:       s.d.SubAutoCleanupEnabled,
		AutoCleanupRetentionDays: s.d.autoCleanupRetentionDays(),
		AutoCleanupCron:          s.d.autoCleanupCronSpec(),
		LastCleanupError:         lastCleanupError,
	}
	if lastCleanupAt != nil {
		t := *lastCleanupAt
		summary.LastCleanupAt = &t
	}
	return runtimeSnapshot{
		Summary: summary,
		Rows:    rows,
	}
}

func (s *driverRuntimeState) collectLinkCacheRows() []runtimeMemoryRow {
	if s.d == nil || s.d.GetStorage() == nil {
		return nil
	}
	mountPath := utils.GetActualMountPath(s.d.GetStorage().MountPath)
	entries := op.Cache.SnapshotLinkCache()
	rows := make([]runtimeMemoryRow, 0)
	for _, entry := range entries {
		if !utils.IsSubPath(mountPath, entry.Key) {
			continue
		}
		row := runtimeMemoryRow{
			Key:       entry.Key + "|" + entry.TypeKey,
			Kind:      runtimeRegistryLink,
			FilePath:  entry.Key,
			UserAgent: extractUserAgentFromTypeKey(entry.TypeKey),
			Source:    "op.link_cache",
		}
		if entry.Obj != nil {
			row.FileName = entry.Obj.GetName()
			row.FileID = entry.Obj.GetID()
			row.Size = entry.Obj.GetSize()
		} else {
			row.FileName = path.Base(entry.Key)
		}
		if entry.CreatedAt != nil {
			t := *entry.CreatedAt
			row.CreatedAt = &t
		}
		if entry.LastAccessAt != nil {
			t := *entry.LastAccessAt
			row.LastSeenAt = &t
		}
		if entry.ExpiresAt != nil {
			t := *entry.ExpiresAt
			row.ExpiresAt = &t
			ttl := int64(time.Until(t).Seconds())
			if ttl < 0 {
				ttl = 0
			}
			row.TTLSeconds = &ttl
		}
		rows = append(rows, row)
	}
	return rows
}

func (d *Pan115) RuntimeSnapshot() runtimeSnapshot {
	if d.runtime == nil {
		return runtimeSnapshot{
			Summary: runtimeSummary{
				StorageID:                d.GetStorage().ID,
				MountPath:                d.GetStorage().MountPath,
				MainAccountAvailable:     d.GetStorage().Status == op.WORK,
				MainAccountStatus:        d.GetStorage().Status,
				SubTransferEnabled:       d.SubTransferEnabled,
				SubFallbackToMain:        d.SubFallbackToMain,
				AutoCleanupEnabled:       d.SubAutoCleanupEnabled,
				AutoCleanupRetentionDays: d.autoCleanupRetentionDays(),
				AutoCleanupCron:          d.autoCleanupCronSpec(),
			},
		}
	}
	return d.runtime.snapshot()
}

func (d *Pan115) RuntimeSnapshotWithOptions(hours int64, from, to *time.Time, kind, detail string) (runtimeSnapshot, error) {
	opts, err := newRuntimeSnapshotOptions(hours, from, to, kind, detail)
	if err != nil {
		return runtimeSnapshot{}, err
	}
	return d.RuntimeSnapshot().applyOptions(opts), nil
}

func transferRuntimeKey(file *FileObj, userAgent string) string {
	if file == nil {
		return ""
	}
	return strings.Join([]string{
		file.GetID(),
		file.PickCode,
		strings.ToUpper(file.Sha1),
		strconv.FormatInt(file.GetSize(), 10),
	}, "\x00")
}

func (r runtimeMemoryRow) LastSeenTime() time.Time {
	if r.LastSeenAt != nil {
		return *r.LastSeenAt
	}
	if r.ExpiresAt != nil {
		return *r.ExpiresAt
	}
	return time.Time{}
}

func (r runtimeMemoryRow) ActivityTime() time.Time {
	if r.LastSeenAt != nil {
		return *r.LastSeenAt
	}
	if r.CreatedAt != nil {
		return *r.CreatedAt
	}
	if r.ExpiresAt != nil {
		return *r.ExpiresAt
	}
	return time.Time{}
}

func (r runtimeMemoryRow) basicView() runtimeMemoryRow {
	row := runtimeMemoryRow{
		Kind:        r.Kind,
		FileName:    r.FileName,
		FilePath:    r.FilePath,
		Size:        r.Size,
		UserAgent:   r.UserAgent,
		AccountRole: r.AccountRole,
		Source:      r.Source,
	}
	if r.CreatedAt != nil {
		t := *r.CreatedAt
		row.CreatedAt = &t
	}
	if r.LastSeenAt != nil {
		t := *r.LastSeenAt
		row.LastSeenAt = &t
	}
	if r.ExpiresAt != nil {
		t := *r.ExpiresAt
		row.ExpiresAt = &t
	}
	if r.TTLSeconds != nil {
		ttl := *r.TTLSeconds
		row.TTLSeconds = &ttl
	}
	return row
}

func (s runtimeSnapshot) applyOptions(opts runtimeSnapshotOptions) runtimeSnapshot {
	rows := make([]runtimeMemoryRow, 0, len(s.Rows))
	for _, row := range s.Rows {
		if !opts.matchKind(row) {
			continue
		}
		if !opts.matchTime(row) {
			continue
		}
		if opts.Detail == runtimeDetailBasic {
			row = row.basicView()
		}
		rows = append(rows, row)
	}
	s.Summary.FilteredRowCount = len(rows)
	s.Summary.KindFilter = string(opts.Kind)
	s.Summary.DetailMode = string(opts.Detail)
	if opts.From != nil {
		t := *opts.From
		s.Summary.RangeFrom = &t
	} else {
		s.Summary.RangeFrom = nil
	}
	if opts.To != nil {
		t := *opts.To
		s.Summary.RangeTo = &t
	} else {
		s.Summary.RangeTo = nil
	}
	s.Rows = rows
	return s
}

func newRuntimeSnapshotOptions(hours int64, from, to *time.Time, kind, detail string) (runtimeSnapshotOptions, error) {
	opts := runtimeSnapshotOptions{
		Now:    time.Now(),
		Kind:   runtimeRowKindAll,
		Detail: runtimeDetailBasic,
	}
	switch strings.TrimSpace(kind) {
	case "", string(runtimeRowKindAll):
		opts.Kind = runtimeRowKindAll
	case string(runtimeRowKindTransfer):
		opts.Kind = runtimeRowKindTransfer
	case string(runtimeRowKindLink):
		opts.Kind = runtimeRowKindLink
	default:
		return runtimeSnapshotOptions{}, errors.New("invalid kind filter")
	}
	switch strings.TrimSpace(detail) {
	case "", string(runtimeDetailBasic):
		opts.Detail = runtimeDetailBasic
	case string(runtimeDetailFull):
		opts.Detail = runtimeDetailFull
	default:
		return runtimeSnapshotOptions{}, errors.New("invalid detail mode")
	}
	if hours < 0 {
		return runtimeSnapshotOptions{}, errors.New("hours must be greater than or equal to 0")
	}
	if from == nil && to == nil {
		if hours == 0 {
			hours = 6
		}
		t := opts.Now
		f := t.Add(-time.Duration(hours) * time.Hour)
		opts.From = &f
		opts.To = &t
	} else {
		if from != nil {
			t := *from
			opts.From = &t
		}
		if to != nil {
			t := *to
			opts.To = &t
		}
		if opts.From != nil && opts.To != nil && opts.From.After(*opts.To) {
			return runtimeSnapshotOptions{}, errors.New("from must be earlier than or equal to to")
		}
	}
	return opts, nil
}

func (o runtimeSnapshotOptions) matchKind(row runtimeMemoryRow) bool {
	switch o.Kind {
	case runtimeRowKindAll:
		return true
	case runtimeRowKindTransfer:
		return row.Kind == runtimeRegistryTransfer
	case runtimeRowKindLink:
		return row.Kind == runtimeRegistryLink
	default:
		return true
	}
}

func (o runtimeSnapshotOptions) matchTime(row runtimeMemoryRow) bool {
	t := row.ActivityTime()
	if t.IsZero() {
		return true
	}
	if o.From != nil && t.Before(*o.From) {
		return false
	}
	if o.To != nil && t.After(*o.To) {
		return false
	}
	return true
}

func subAccountRoleLabel(index int) string {
	switch index {
	case 1:
		return string(runtimeAccountSub1)
	case 2:
		return string(runtimeAccountSub2)
	default:
		return "sub_" + strconv.Itoa(index)
	}
}

func extractUserAgentFromTypeKey(typeKey string) string {
	idx := strings.LastIndex(typeKey, "/")
	if idx < 0 || idx+1 >= len(typeKey) {
		return ""
	}
	return typeKey[idx+1:]
}
