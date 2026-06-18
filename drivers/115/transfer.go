package _115

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	stdpath "path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	cipher "github.com/SheltonZhu/115driver/pkg/crypto/ec115"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	maxRapidUploadVerifyAttempts  = 3
	subAccountUnavailableDuration = 10 * time.Minute
)

type subTransferManager struct {
	main     *Pan115
	accounts []*subAccount
	cache    map[string]subTransferCacheEntry
	mu       sync.Mutex
	group    singleflight.Group[*subTransferResult]
}

type subAccount struct {
	index            int
	client           *driver115.Pan115Client
	rootDir          string
	rootID           string
	mu               sync.Mutex
	stateMu          sync.Mutex
	unavailableUntil time.Time
	lastError        string
	lastErrorAt      time.Time
}

type subTransferCacheEntry struct {
	result    *subTransferResult
	expiresAt time.Time
}

type subTransferResult struct {
	client   *driver115.Pan115Client
	account  *subAccount
	pickCode string
	file      *FileObj
	userAgent string
	fileID    string
	fileName  string
}

type subHTTPStatusError struct {
	op         string
	statusCode int
	status     string
}

func (e *subHTTPStatusError) Error() string {
	if e.status != "" {
		return fmt.Sprintf("%s failed: %s", e.op, e.status)
	}
	return fmt.Sprintf("%s failed: HTTP %d", e.op, e.statusCode)
}

type sourceHashError struct {
	err error
}

func (e *sourceHashError) Error() string {
	return e.err.Error()
}

func (e *sourceHashError) Unwrap() error {
	return e.err
}

func (d *Pan115) newSubTransferManager() (*subTransferManager, error) {
	if !d.SubTransferEnabled {
		log.Debug("[115-Sub] sub-account transfer disabled")
		return nil, nil
	}
	cookies := splitSubCookies(d.SubCookies)
	if len(cookies) == 0 {
		log.Warn("[115-Sub] sub-account transfer enabled but no sub cookie configured")
		return nil, nil
	}
	rootDir := strings.TrimSpace(d.SubFolder)
	if rootDir == "" {
		rootDir = "tmp"
	}
	m := &subTransferManager{
		main:     d,
		cache:    make(map[string]subTransferCacheEntry),
	}
	for i, cookie := range cookies {
		client, err := d.newClientByCookie(cookie)
		if err != nil {
			if d.runtime != nil {
				d.runtime.recordSubLogin(i+1, err)
			}
			m.accounts = append(m.accounts, &subAccount{
				index:       i + 1,
				rootDir:     rootDir,
				lastError:   err.Error(),
				lastErrorAt: time.Now(),
			})
			continue
		}
		account := &subAccount{
			index:   i + 1,
			client:  client,
			rootDir: rootDir,
		}
		m.accounts = append(m.accounts, account)
		if d.runtime != nil {
			d.runtime.recordSubLogin(i+1, nil)
			d.runtime.recordSubState(account)
		}
	}
	usable := 0
	for _, account := range m.accounts {
		if account != nil && account.client != nil {
			usable++
		}
	}
	log.Infof("[115-Sub] sub-account transfer enabled: accounts=%d usable=%d dir=%q", len(m.accounts), usable, rootDir)
	if usable == 0 && !d.SubFallbackToMain {
		return m, errors.New("no usable 115 sub account available")
	}
	return m, nil
}

func splitSubCookies(raw string) []string {
	lines := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '\n' || r == '\r'
	})
	cookies := make([]string, 0, len(lines))
	for _, line := range lines {
		cookie := strings.TrimSpace(line)
		if cookie != "" {
			cookies = append(cookies, cookie)
		}
	}
	return cookies
}

func (d *Pan115) newClientByCookie(cookie string) (*driver115.Pan115Client, error) {
	client := driver115.New(
		driver115.UA(d.getUA()),
		func(c *driver115.Pan115Client) {
			c.Client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: conf.Conf.TlsInsecureSkipVerify})
			c.Client.OnAfterResponse(func(_ *resty.Client, resp *resty.Response) error {
				if resp != nil && isSubAccountUnavailableStatus(resp.StatusCode()) {
					return &subHTTPStatusError{op: "115 sub account request", statusCode: resp.StatusCode(), status: resp.Status()}
				}
				return nil
			})
		},
	)
	cr := &driver115.Credential{}
	if err := cr.FromCookie(cookie); err != nil {
		return nil, err
	}
	client.ImportCredential(cr)
	if err := client.LoginCheck(); err != nil {
		return nil, err
	}
	return client, nil
}

func (m *subTransferManager) prepare(ctx context.Context, file *FileObj, userAgent string, skipped map[*subAccount]struct{}) (*subTransferResult, error) {
	if len(m.accounts) == 0 {
		return nil, errors.New("no 115 sub account configured")
	}
	key := m.cacheKey(file)
	if result, ok := m.getCache(key, skipped); ok {
		log.Debugf("[115-Sub] use cached sub pickcode: account=%d file=%q id=%s", result.account.index, file.GetName(), file.GetID())
		return result, nil
	}
	flightKey := key
	if skippedKey := skippedAccountsKey(skipped); skippedKey != "" {
		flightKey += "\x00skip=" + skippedKey
	}
	result, err, _ := m.group.Do(flightKey, func() (*subTransferResult, error) {
		if result, ok := m.getCache(key, skipped); ok {
			return result, nil
		}
		accounts := m.pickAccounts(skipped)
		if len(accounts) == 0 {
			return nil, errors.New("no 115 sub account available")
		}
		var lastErr error
		for _, account := range accounts {
			if account.client == nil {
				lastErr = errors.New("sub account login failed")
				continue
			}
			result, err := m.prepareWithAccount(ctx, account, file)
			if err == nil {
				result.file = file
				result.userAgent = userAgent
				m.setCache(key, result)
				if m.main.runtime != nil {
					m.main.runtime.recordTransferResult(file, account, result, userAgent, "transfer_prepare")
				}
				log.Infof("[115-Sub] prepared sub file: account=%d file=%q id=%s", account.index, file.GetName(), file.GetID())
				return result, nil
			}
			lastErr = err
			if m.main.runtime != nil {
				m.main.runtime.recordSubError(account, "prepare", err)
			}
			m.markUnavailableIfNeeded(account, err)
			log.Warnf("[115-Sub] prepare with sub account failed: account=%d file=%q id=%s err=%v", account.index, file.GetName(), file.GetID(), err)
		}
		if lastErr == nil {
			lastErr = errors.New("no 115 sub account available")
		}
		return nil, lastErr
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *subTransferManager) cacheKey(file *FileObj) string {
	return strings.Join([]string{
		file.GetID(),
		file.PickCode,
		strings.ToUpper(file.Sha1),
		strconv.FormatInt(file.GetSize(), 10),
	}, "\x00")
}

func (m *subTransferManager) getCache(key string, skipped map[*subAccount]struct{}) (*subTransferResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.cache[key]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		delete(m.cache, key)
		return nil, false
	}
	if entry.result == nil || entry.result.account == nil {
		return nil, false
	}
	if isAccountSkipped(entry.result.account, skipped) {
		return nil, false
	}
	if !entry.result.account.isAvailable(time.Now()) {
		return nil, false
	}
	return entry.result, true
}

func (m *subTransferManager) setCache(key string, result *subTransferResult) {
	if result == nil || result.account == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache[key] = subTransferCacheEntry{
		result:    result,
		expiresAt: time.Now().Add(2 * time.Hour),
	}
}

func (m *subTransferManager) invalidateResult(file *FileObj, result *subTransferResult) {
	if file == nil {
		return
	}
	key := m.cacheKey(file)
	m.mu.Lock()
	delete(m.cache, key)
	m.mu.Unlock()
	if m.main.runtime != nil {
		m.main.runtime.deleteTransferByResult(result)
	}
}

func (m *subTransferManager) pickAccounts(skipped map[*subAccount]struct{}) []*subAccount {
	now := time.Now()
	accounts := make([]*subAccount, 0, len(m.accounts))
	for _, account := range m.accounts {
		if isAccountSkipped(account, skipped) || !account.isAvailable(now) {
			continue
		}
		accounts = append(accounts, account)
	}
	rand.Shuffle(len(accounts), func(i, j int) {
		accounts[i], accounts[j] = accounts[j], accounts[i]
	})
	return accounts
}

func (m *subTransferManager) accountCount() int {
	return len(m.accounts)
}

func (m *subTransferManager) markUnavailableIfNeeded(account *subAccount, err error) bool {
	if account == nil || !isSubAccountUnavailableError(err) {
		return false
	}
	until := account.markUnavailable(subAccountUnavailableDuration, err)
	log.Warnf("[115-Sub] mark sub account unavailable: account=%d until=%s err=%v", account.index, until.Format(time.RFC3339), err)
	return true
}

func (account *subAccount) isAvailable(now time.Time) bool {
	account.stateMu.Lock()
	defer account.stateMu.Unlock()
	return account.unavailableUntil.IsZero() || !now.Before(account.unavailableUntil)
}

func (account *subAccount) markUnavailable(duration time.Duration, err error) time.Time {
	until := time.Now().Add(duration)
	account.stateMu.Lock()
	defer account.stateMu.Unlock()
	if until.After(account.unavailableUntil) {
		account.unavailableUntil = until
	}
	if err != nil {
		account.lastError = err.Error()
		account.lastErrorAt = time.Now()
	}
	return account.unavailableUntil
}

func isAccountSkipped(account *subAccount, skipped map[*subAccount]struct{}) bool {
	if len(skipped) == 0 {
		return false
	}
	_, ok := skipped[account]
	return ok
}

func skippedAccountsKey(skipped map[*subAccount]struct{}) string {
	if len(skipped) == 0 {
		return ""
	}
	indexes := make([]int, 0, len(skipped))
	for account := range skipped {
		if account != nil {
			indexes = append(indexes, account.index)
		}
	}
	sort.Ints(indexes)
	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		parts = append(parts, strconv.Itoa(index))
	}
	return strings.Join(parts, ",")
}

func isSubAccountUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	var sourceErr *sourceHashError
	if stderrors.As(err, &sourceErr) {
		return false
	}
	var statusErr *subHTTPStatusError
	if stderrors.As(err, &statusErr) {
		return isSubAccountUnavailableStatus(statusErr.statusCode)
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "pickcode does not exist") ||
		strings.Contains(msg, "empty pickcode") ||
		strings.Contains(msg, "target does not exist") ||
		strings.Contains(msg, "can not get download url") ||
		strings.Contains(msg, "can not download directory") {
		return false
	}
	return strings.Contains(msg, "403") ||
		strings.Contains(msg, "forbidden") ||
		strings.Contains(msg, "405") ||
		strings.Contains(msg, "method not allowed") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests")
}

func isSubAccountUnavailableStatus(statusCode int) bool {
	return statusCode == http.StatusForbidden ||
		statusCode == http.StatusMethodNotAllowed ||
		statusCode == http.StatusTooManyRequests
}

func (m *subTransferManager) shouldInvalidateAndRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver115.ErrPickCodeNotExist) ||
		errors.Is(err, driver115.ErrPickCodeIsEmpty) ||
		errors.Is(err, driver115.ErrNotExist) ||
		errors.Is(err, driver115.ErrDownloadEmpty) ||
		errors.Is(err, driver115.ErrDownloadFileNotExistOrHasDeleted) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "pickcode does not exist") ||
		strings.Contains(msg, "empty pickcode") ||
		strings.Contains(msg, "target file does not exist or has deleted") ||
		strings.Contains(msg, "target does not exist")
}

func (m *subTransferManager) prepareWithAccount(ctx context.Context, account *subAccount, file *FileObj) (*subTransferResult, error) {
	account.mu.Lock()
	defer account.mu.Unlock()
	rootID, err := m.ensureRoot(ctx, account)
	if err != nil {
		return nil, err
	}
	name := m.managedFileName(file)
	existing, err := findSubFile(account.client, rootID, name, strings.ToUpper(file.Sha1), file.GetSize())
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.PickCode != "" {
		log.Infof("[115-Sub] reuse existing sub file: dir=%s name=%q", rootID, name)
		return &subTransferResult{client: account.client, account: account, pickCode: existing.PickCode, file: file, fileID: existing.FileID, fileName: existing.Name}, nil
	}
	if file.PickCode == "" {
		return nil, errors.New("source file has no pickcode")
	}
	if file.Sha1 == "" {
		return nil, errors.New("source file has no sha1")
	}
	if file.GetSize() <= 0 {
		return nil, errors.New("source file has invalid size")
	}
	meta, err := m.fastTransfer(ctx, account.client, file, rootID, name)
	if err != nil {
		return nil, err
	}
	if meta != nil && meta.PickCode != "" {
		log.Infof("[115-Sub] rapid upload succeeded: dir=%s name=%q", rootID, name)
		return &subTransferResult{client: account.client, account: account, pickCode: meta.PickCode, file: file, fileID: meta.FileID, fileName: name}, nil
	}
	created, err := findSubFile(account.client, rootID, name, strings.ToUpper(file.Sha1), file.GetSize())
	if err != nil {
		return nil, err
	}
	if created == nil || created.PickCode == "" {
		return nil, errors.New("sub-account transfer succeeded but file metadata was not visible")
	}
	return &subTransferResult{client: account.client, account: account, pickCode: created.PickCode, file: file, fileID: created.FileID, fileName: created.Name}, nil
}

func (m *subTransferManager) ensureRoot(ctx context.Context, account *subAccount) (string, error) {
	if account.rootID != "" {
		return account.rootID, nil
	}
	current := "0"
	for _, part := range splitPath(account.rootDir) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		files, err := account.client.ListWithLimit(current, driver115.FileListLimit, driver115.WithMultiUrls())
		if err != nil {
			return "", err
		}
		found := ""
		for _, file := range *files {
			if file.IsDirectory && file.Name == part {
				found = file.FileID
				break
			}
		}
		if found == "" {
			found, err = account.client.Mkdir(current, part)
			if err != nil {
				return "", err
			}
			log.Infof("[115-Sub] created sub transfer dir: parent=%s name=%q id=%s", current, part, found)
		}
		current = found
	}
	account.rootID = current
	return current, nil
}

func splitPath(path string) []string {
	cleaned := stdpath.Clean("/" + strings.TrimSpace(path))
	if cleaned == "/" || cleaned == "." {
		return nil
	}
	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	res := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" && part != "." {
			res = append(res, part)
		}
	}
	return res
}

func (m *subTransferManager) managedFileName(file *FileObj) string {
	name := file.GetName()
	if name == "" {
		name = file.GetID()
	}
	return name
}

func findSubFile(client *driver115.Pan115Client, cid, name, sha1 string, size int64) (*driver115.File, error) {
	files, err := client.ListWithLimit(cid, driver115.FileListLimit, driver115.WithMultiUrls())
	if err != nil {
		return nil, err
	}
	for _, file := range *files {
		if file.IsDirectory || file.Name != name {
			continue
		}
		if size >= 0 && file.Size != size {
			continue
		}
		if sha1 != "" && file.Sha1 != "" && !strings.EqualFold(file.Sha1, sha1) {
			continue
		}
		f := file
		return &f, nil
	}
	return nil, nil
}

func (m *subTransferManager) fastTransfer(ctx context.Context, target *driver115.Pan115Client, file *FileObj, dirID, name string) (*driver115.File, error) {
	hashUserAgent := m.main.getUA()
	preID, err := m.sourceRangeSHA1(ctx, file.PickCode, hashUserAgent, http_range.Range{
		Start:  0,
		Length: min(file.GetSize(), 128*utils.KB),
	})
	if err != nil {
		return nil, &sourceHashError{err: errors.Wrap(err, "failed to hash source preid")}
	}
	resp, err := m.main.rapidUploadWithClient(
		ctx,
		target,
		file.GetSize(),
		name,
		dirID,
		strings.ToUpper(preID),
		strings.ToUpper(file.Sha1),
		true,
		func(rangeSpec string) (string, error) {
			r, err := parseSignCheckRange(rangeSpec)
			if err != nil {
				return "", err
			}
			hash, err := m.sourceRangeSHA1(ctx, file.PickCode, hashUserAgent, r)
			if err != nil {
				return "", &sourceHashError{err: err}
			}
			return hash, nil
		},
	)
	if err != nil {
		return nil, err
	}
	ok, err := resp.Ok()
	if err != nil {
		return nil, errors.Wrapf(err, "rapid upload status=%v message=%s", resp.Status, resp.ErrorMsg)
	}
	if !ok {
		return nil, errors.Errorf("115 rapid upload did not hit second transfer: status=%v message=%s", resp.Status, resp.ErrorMsg)
	}
	if resp.PickCode != "" {
		fileObj, err := getFileByPickCode(target, resp.PickCode)
		if err == nil {
			return fileObj, nil
		}
		if isSubAccountUnavailableError(err) {
			return nil, err
		}
	}
	return nil, nil
}

func (m *subTransferManager) sourceRangeSHA1(ctx context.Context, pickCode, userAgent string, r http_range.Range) (string, error) {
	if r.Length <= 0 {
		return utils.HashData(utils.SHA1, nil), nil
	}
	info, err := m.main.client.DownloadWithUA(pickCode, userAgent)
	if err != nil {
		return "", err
	}
	if info == nil || info.Url.Url == "" {
		return "", driver115.ErrDownloadEmpty
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.Url.Url, nil)
	if err != nil {
		return "", err
	}
	for key, values := range info.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.Start, r.Start+r.Length-1))
	req.Header.Set("User-Agent", userAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return "", &subHTTPStatusError{op: "range request", statusCode: resp.StatusCode, status: fmt.Sprintf("%s (ua=%q)", resp.Status, userAgent)}
	}
	hash, err := utils.HashReader(utils.SHA1, io.LimitReader(resp.Body, r.Length))
	if err != nil {
		return "", err
	}
	return strings.ToUpper(hash), nil
}

func parseSignCheckRange(rangeSpec string) (http_range.Range, error) {
	text := strings.TrimSpace(rangeSpec)
	text = strings.TrimPrefix(strings.ToLower(text), "bytes=")
	text = strings.ReplaceAll(text, "_", "-")
	parts := strings.Split(text, "-")
	if len(parts) != 2 {
		return http_range.Range{}, fmt.Errorf("invalid sign_check range: %s", rangeSpec)
	}
	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return http_range.Range{}, err
	}
	end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return http_range.Range{}, err
	}
	if start < 0 || end < start {
		return http_range.Range{}, fmt.Errorf("invalid sign_check range: %s", rangeSpec)
	}
	return http_range.Range{Start: start, Length: end - start + 1}, nil
}

func (d *Pan115) rapidUploadWithClient(
	ctx context.Context,
	client *driver115.Pan115Client,
	fileSize int64,
	fileName, dirID, preID, fileID string,
	topUpload bool,
	hashRange func(string) (string, error),
) (*driver115.UploadInitResp, error) {
	var (
		ecdhCipher   *cipher.EcdhCipher
		encrypted    []byte
		decrypted    []byte
		encodedToken string
		err          error
		target       = "U_1_" + dirID
		bodyBytes    []byte
		result       = driver115.UploadInitResp{}
		fileSizeStr  = strconv.FormatInt(fileSize, 10)
	)
	if ctx == nil {
		ctx = context.Background()
	}
	if ecdhCipher, err = cipher.NewEcdhCipher(); err != nil {
		return nil, err
	}
	if ok, err := client.UploadAvailable(); err != nil {
		return nil, err
	} else if !ok {
		return nil, errors.New("115 upload is not available")
	}
	userID := strconv.FormatInt(client.UserID, 10)
	form := url.Values{}
	form.Set("appid", "0")
	form.Set("appversion", appVer)
	form.Set("userid", userID)
	form.Set("filename", fileName)
	form.Set("filesize", fileSizeStr)
	form.Set("fileid", fileID)
	form.Set("target", target)
	form.Set("sig", client.GenerateSignature(fileID, target))
	if topUpload {
		form.Set("topupload", "true")
	}

	signKey, signVal := "", ""
	verifyAttempts := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t := driver115.NowMilli()
		if encodedToken, err = ecdhCipher.EncodeToken(t.ToInt64()); err != nil {
			return nil, err
		}
		params := map[string]string{
			"k_ec": encodedToken,
		}
		form.Set("t", t.String())
		form.Set("token", generateTokenForUser(client.UserID, fileID, preID, t.String(), fileSizeStr, signKey, signVal))
		if signKey != "" && signVal != "" {
			form.Set("sign_key", signKey)
			form.Set("sign_val", signVal)
		}
		if encrypted, err = ecdhCipher.Encrypt([]byte(form.Encode())); err != nil {
			return nil, err
		}
		req := client.NewRequest().
			SetContext(ctx).
			SetQueryParams(params).
			SetBody(encrypted).
			SetHeaderVerbatim("Content-Type", "application/x-www-form-urlencoded").
			SetDoNotParseResponse(true)
		resp, err := req.Post(driver115.ApiUploadInit)
		if err != nil {
			return nil, err
		}
		if resp != nil && isSubAccountUnavailableStatus(resp.StatusCode()) {
			return nil, &subHTTPStatusError{op: "rapid upload request", statusCode: resp.StatusCode(), status: resp.Status()}
		}
		data := resp.RawBody()
		bodyBytes, err = io.ReadAll(data)
		data.Close()
		if err != nil {
			return nil, err
		}
		if decrypted, err = ecdhCipher.Decrypt(bodyBytes); err != nil {
			return nil, err
		}
		if err = driver115.CheckErr(utils.Json.Unmarshal(decrypted, &result), &result, resp); err != nil {
			return nil, err
		}
		result.SHA1 = fileID
		if isRapidUploadVerifyStatus(result.Status) {
			verifyAttempts++
			if verifyAttempts > maxRapidUploadVerifyAttempts {
				return nil, errors.Errorf("rapid upload verification exceeded %d attempts: status=%v message=%s", maxRapidUploadVerifyAttempts, result.Status, result.ErrorMsg)
			}
			if result.SignKey == "" || result.SignCheck == "" {
				return nil, errors.Errorf("rapid upload verification missing sign data: status=%v message=%s", result.Status, result.ErrorMsg)
			}
			signKey = result.SignKey
			signVal, err = hashRange(result.SignCheck)
			if err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	return &result, nil
}

func isRapidUploadVerifyStatus(status driver115.BoolInt) bool {
	return status == 6 || status == 7 || status == 8
}

func generateTokenForUser(rawUserID int64, fileID, preID, timeStamp, fileSize, signKey, signVal string) string {
	userID := strconv.FormatInt(rawUserID, 10)
	userIDMd5 := md5.Sum([]byte(userID))
	tokenMd5 := md5.Sum([]byte(md5Salt + fileID + fileSize + signKey + signVal + userID + timeStamp + hex.EncodeToString(userIDMd5[:]) + appVer))
	return hex.EncodeToString(tokenMd5[:])
}

func getFileByPickCode(client *driver115.Pan115Client, pickCode string) (*driver115.File, error) {
	file, err := (&Pan115{client: client}).getNewFileByPickCode(pickCode)
	if err != nil {
		return nil, err
	}
	return &file.File, nil
}
