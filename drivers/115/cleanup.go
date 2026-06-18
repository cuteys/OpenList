package _115

import (
	"context"
	stderrors "errors"
	"strconv"
	"strings"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/pkg/errors"
)

const (
	defaultSubAutoCleanupDays = 3
	defaultSubAutoCleanupCron = "0 0 * * *"
)

func (d *Pan115) autoCleanupRetentionDays() int64 {
	if d.SubAutoCleanupDays > 0 {
		return d.SubAutoCleanupDays
	}
	return defaultSubAutoCleanupDays
}

func (d *Pan115) autoCleanupCronSpec() string {
	spec := strings.TrimSpace(d.SubAutoCleanupCron)
	if spec == "" {
		return defaultSubAutoCleanupCron
	}
	return spec
}

func (d *Pan115) startCleanup() {
	d.stopCleanup()
	if !d.SubTransferEnabled || !d.SubAutoCleanupEnabled || d.transfer == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cleanupCancel = cancel
	go d.runCleanupLoop(ctx)
}

func (d *Pan115) stopCleanup() {
	if d.cleanupCancel != nil {
		d.cleanupCancel()
		d.cleanupCancel = nil
	}
}

func (d *Pan115) runCleanupLoop(ctx context.Context) {
	for {
		wait := durationUntilNextCleanup(d.autoCleanupCronSpec(), time.Now())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if d.transfer == nil {
			continue
		}
		err := d.transfer.runCleanup(context.Background())
		if d.runtime != nil {
			d.runtime.setCleanupResult(err)
		}
	}
}

func durationUntilNextCleanup(spec string, now time.Time) time.Duration {
	next := nextCleanupTime(spec, now)
	if !next.After(now) {
		next = now.Add(24 * time.Hour)
	}
	return next.Sub(now)
}

func nextCleanupTime(spec string, now time.Time) time.Time {
	fields := strings.Fields(strings.TrimSpace(spec))
	if len(fields) != 5 {
		return nextMidnight(now)
	}
	if fields[2] != "*" || fields[3] != "*" || fields[4] != "*" {
		return nextMidnight(now)
	}
	minute, err1 := strconv.Atoi(fields[0])
	hour, err2 := strconv.Atoi(fields[1])
	if err1 != nil || err2 != nil || minute < 0 || minute > 59 || hour < 0 || hour > 23 {
		return nextMidnight(now)
	}
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func nextMidnight(now time.Time) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func (m *subTransferManager) runCleanup(ctx context.Context) error {
	if m == nil {
		return nil
	}
	cutoff := time.Now().Add(-time.Duration(m.main.autoCleanupRetentionDays()) * 24 * time.Hour)
	if m.main.runtime != nil {
		m.main.runtime.trimTransferRows(cutoff)
	}
	var errAll error
	for _, account := range m.accounts {
		if account == nil {
			continue
		}
		if err := m.cleanupAccount(ctx, account, cutoff); err != nil {
			errAll = stderrors.Join(errAll, err)
		}
	}
	return errAll
}

func (m *subTransferManager) cleanupAccount(ctx context.Context, account *subAccount, cutoff time.Time) error {
	account.mu.Lock()
	defer account.mu.Unlock()
	rootID, err := m.ensureRoot(ctx, account)
	if err != nil {
		if m.main.runtime != nil {
			m.main.runtime.recordSubError(account, "cleanup_prepare", err)
		}
		return errors.Wrapf(err, "prepare cleanup root for sub account %d", account.index)
	}
	files, err := account.client.ListWithLimit(rootID, driver115.FileListLimit, driver115.WithMultiUrls())
	if err != nil {
		if m.main.runtime != nil {
			m.main.runtime.recordSubError(account, "cleanup_list", err)
		}
		return errors.Wrapf(err, "list cleanup files for sub account %d", account.index)
	}
	var errAll error
	for _, file := range *files {
		if file.IsDirectory {
			continue
		}
		modTime := file.UpdateTime
		if modTime.IsZero() {
			modTime = file.CreateTime
		}
		if modTime.IsZero() || modTime.After(cutoff) {
			continue
		}
		if err := account.client.Delete(file.FileID); err != nil {
			if m.main.runtime != nil {
				m.main.runtime.recordSubError(account, "cleanup_delete", err)
			}
			errAll = stderrors.Join(errAll, errors.Wrapf(err, "delete sub file %s on account %d", file.FileID, account.index))
		}
	}
	return errAll
}
