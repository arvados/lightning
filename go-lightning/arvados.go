// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.arvados.org/arvados.git/lib/cmd"
	"git.arvados.org/arvados.git/sdk/go/arvados"
	"git.arvados.org/arvados.git/sdk/go/arvadosclient"
	"git.arvados.org/arvados.git/sdk/go/keepclient"
	"github.com/klauspost/pgzip"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/net/websocket"
)

type eventMessage struct {
	Status     int
	ObjectUUID string `json:"object_uuid"`
	EventType  string `json:"event_type"`
	Properties struct {
		Text string
	}
}

type arvadosClient struct {
	*arvados.Client
	notifying map[string]map[chan<- eventMessage]int
	wantClose chan struct{}
	wsconn    *websocket.Conn
	mtx       sync.Mutex
}

// Listen for events concerning the given uuids. When an event occurs
// (and after connecting/reconnecting to the event stream), send each
// uuid to ch. If a {ch, uuid} pair is subscribed twice, the uuid will
// be sent only once for each update, but two Unsubscribe calls will
// be needed to stop sending them.
func (client *arvadosClient) Subscribe(ch chan<- eventMessage, uuid string) {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	if client.notifying == nil {
		client.notifying = map[string]map[chan<- eventMessage]int{}
		client.wantClose = make(chan struct{})
		go client.runNotifier()
	}
	chmap := client.notifying[uuid]
	if chmap == nil {
		chmap = map[chan<- eventMessage]int{}
		client.notifying[uuid] = chmap
	}
	needSub := true
	for _, nch := range chmap {
		if nch > 0 {
			needSub = false
			break
		}
	}
	chmap[ch]++
	if needSub && client.wsconn != nil {
		go json.NewEncoder(client.wsconn).Encode(map[string]interface{}{
			"method": "subscribe",
			"filters": [][]interface{}{
				{"object_uuid", "=", uuid},
				{"event_type", "in", []string{"stderr", "crunch-run", "update"}},
			},
		})
	}
}

func (client *arvadosClient) Unsubscribe(ch chan<- eventMessage, uuid string) {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	chmap := client.notifying[uuid]
	if n := chmap[ch] - 1; n == 0 {
		delete(chmap, ch)
		if len(chmap) == 0 {
			delete(client.notifying, uuid)
		}
		if client.wsconn != nil {
			go json.NewEncoder(client.wsconn).Encode(map[string]interface{}{
				"method": "unsubscribe",
				"filters": [][]interface{}{
					{"object_uuid", "=", uuid},
					{"event_type", "in", []string{"stderr", "crunch-run", "update"}},
				},
			})
		}
	} else if n > 0 {
		chmap[ch] = n
	}
}

func (client *arvadosClient) Close() {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	if client.notifying != nil {
		client.notifying = nil
		close(client.wantClose)
	}
}

func (client *arvadosClient) runNotifier() {
reconnect:
	for {
		var cluster arvados.Cluster
		err := client.RequestAndDecode(&cluster, "GET", arvados.EndpointConfigGet.Path, nil, nil)
		if err != nil {
			log.Warnf("error getting cluster config: %s", err)
			time.Sleep(5 * time.Second)
			continue reconnect
		}
		wsURL := cluster.Services.Websocket.ExternalURL
		wsURL.Scheme = strings.Replace(wsURL.Scheme, "http", "ws", 1)
		wsURL.Path = "/websocket"
		wsURLNoToken := wsURL.String()
		wsURL.RawQuery = url.Values{"api_token": []string{client.AuthToken}}.Encode()
		conn, err := websocket.Dial(wsURL.String(), "", cluster.Services.Controller.ExternalURL.String())
		if err != nil {
			log.Warnf("websocket connection error: %s", err)
			time.Sleep(5 * time.Second)
			continue reconnect
		}
		log.Printf("connected to websocket at %s", wsURLNoToken)

		client.mtx.Lock()
		client.wsconn = conn
		resubscribe := make([]string, 0, len(client.notifying))
		for uuid := range client.notifying {
			resubscribe = append(resubscribe, uuid)
		}
		client.mtx.Unlock()

		go func() {
			w := json.NewEncoder(conn)
			for _, uuid := range resubscribe {
				w.Encode(map[string]interface{}{
					"method": "subscribe",
					"filters": [][]interface{}{
						{"object_uuid", "=", uuid},
						{"event_type", "in", []string{"stderr", "crunch-run", "crunchstat", "update"}},
					},
				})
			}
		}()

		r := json.NewDecoder(conn)
		for {
			var msg eventMessage
			err := r.Decode(&msg)
			select {
			case <-client.wantClose:
				return
			default:
				if err != nil {
					log.Printf("error decoding websocket message: %s", err)
					client.mtx.Lock()
					client.wsconn = nil
					client.mtx.Unlock()
					go conn.Close()
					continue reconnect
				}
				client.mtx.Lock()
				for ch := range client.notifying[msg.ObjectUUID] {
					go func() { ch <- msg }()
				}
				client.mtx.Unlock()
			}
		}
	}
}

var refreshTicker = time.NewTicker(5 * time.Second)

type arvadosContainerRunner struct {
	Client      *arvados.Client
	Name        string
	OutputName  string
	ProjectUUID string
	APIAccess   bool
	VCPUs       int
	RAM         int64
	Prog        string // if empty, run /proc/self/exe
	Args        []string
	Mounts      map[string]map[string]interface{}
	Priority    int
	KeepCache   int // cache buffers per VCPU (0 for default)
	Preemptible bool
}

func (runner *arvadosContainerRunner) Run() (string, error) {
	return runner.RunContext(context.Background())
}

func (runner *arvadosContainerRunner) RunContext(ctx context.Context) (string, error) {
	if runner.ProjectUUID == "" {
		return "", errors.New("cannot run arvados container: ProjectUUID not provided")
	}

	mounts := map[string]map[string]interface{}{
		"/mnt/output": {
			"kind":     "collection",
			"writable": true,
		},
	}
	for path, mnt := range runner.Mounts {
		mounts[path] = mnt
	}

	prog := runner.Prog
	if prog == "" {
		prog = "/mnt/cmd/lightning"
		cmdUUID, err := runner.makeCommandCollection()
		if err != nil {
			return "", err
		}
		mounts["/mnt/cmd"] = map[string]interface{}{
			"kind": "collection",
			"uuid": cmdUUID,
		}
	}
	command := append([]string{prog}, runner.Args...)

	priority := runner.Priority
	if priority < 1 {
		priority = 500
	}
	keepCache := runner.KeepCache
	if keepCache < 1 {
		keepCache = 2
	}
	rc := arvados.RuntimeConstraints{
		API:          runner.APIAccess,
		VCPUs:        runner.VCPUs,
		RAM:          runner.RAM,
		KeepCacheRAM: (1 << 26) * int64(keepCache) * int64(runner.VCPUs),
	}
	outname := &runner.OutputName
	if *outname == "" {
		outname = nil
	}
	var cr arvados.ContainerRequest
	err := runner.Client.RequestAndDecode(&cr, "POST", "arvados/v1/container_requests", nil, map[string]interface{}{
		"container_request": map[string]interface{}{
			"owner_uuid":          runner.ProjectUUID,
			"name":                runner.Name,
			"container_image":     "lightning-runtime",
			"command":             command,
			"mounts":              mounts,
			"use_existing":        true,
			"output_path":         "/mnt/output",
			"output_name":         outname,
			"runtime_constraints": rc,
			"priority":            runner.Priority,
			"state":               arvados.ContainerRequestStateCommitted,
			"scheduling_parameters": arvados.SchedulingParameters{
				Preemptible: runner.Preemptible,
				Partitions:  []string{},
			},
			"environment": map[string]string{
				"GOMAXPROCS": fmt.Sprintf("%d", rc.VCPUs),
			},
			"container_count_max": 1,
		},
	})
	if err != nil {
		return "", err
	}
	log.Printf("container request UUID: %s", cr.UUID)
	log.Printf("container UUID: %s", cr.ContainerUUID)

	logch := make(chan eventMessage)
	client := arvadosClient{Client: runner.Client}
	defer client.Close()
	subscribedUUID := ""
	defer func() {
		if subscribedUUID != "" {
			log.Printf("unsubscribe container UUID: %s", subscribedUUID)
			client.Unsubscribe(logch, subscribedUUID)
		}
	}()

	neednewline := ""
	logTell := map[string]int64{}

	lastState := cr.State
	refreshCR := func() {
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute))
		defer cancel()
		err = runner.Client.RequestAndDecodeContext(ctx, &cr, "GET", "arvados/v1/container_requests/"+cr.UUID, nil, nil)
		if err != nil {
			fmt.Fprint(os.Stderr, neednewline)
			neednewline = ""
			log.Printf("error getting container request: %s", err)
			return
		}
		if lastState != cr.State {
			fmt.Fprint(os.Stderr, neednewline)
			neednewline = ""
			log.Printf("container request state: %s", cr.State)
			lastState = cr.State
		}
		if subscribedUUID != cr.ContainerUUID {
			fmt.Fprint(os.Stderr, neednewline)
			neednewline = ""
			if subscribedUUID != "" {
				log.Printf("unsubscribe container UUID: %s", subscribedUUID)
				client.Unsubscribe(logch, subscribedUUID)
			}
			log.Printf("subscribe container UUID: %s", cr.ContainerUUID)
			client.Subscribe(logch, cr.ContainerUUID)
			subscribedUUID = cr.ContainerUUID
			logTell = map[string]int64{}
		}
	}

	var logWaitMax = time.Second * 10
	var logWaitMin = time.Second
	var logWait = logWaitMin
	var logWaitDone = time.After(logWait)
	var reCrunchstat = regexp.MustCompile(`mem .* (\d+) rss`)
waitctr:
	for cr.State != arvados.ContainerRequestStateFinal {
		select {
		case <-ctx.Done():
			err := runner.Client.RequestAndDecode(&cr, "PATCH", "arvados/v1/container_requests/"+cr.UUID, nil, map[string]interface{}{
				"container_request": map[string]interface{}{
					"priority": 0,
				},
			})
			if err != nil {
				log.Errorf("error while trying to cancel container request %s: %s", cr.UUID, err)
			}
			break waitctr
		case <-refreshTicker.C:
			refreshCR()
		case msg := <-logch:
			if msg.EventType == "update" {
				refreshCR()
			}
		case <-logWaitDone:
			any := false
			for _, fnm := range []string{"stderr.txt", "crunchstat.txt"} {
				req, err := http.NewRequest("GET", "https://"+runner.Client.APIHost+"/arvados/v1/container_requests/"+cr.UUID+"/log/"+cr.ContainerUUID+"/"+fnm, nil)
				if err != nil {
					log.Errorf("error preparing log request: %s", err)
					continue
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", logTell[fnm]))
				resp, err := runner.Client.Do(req)
				if err != nil {
					log.Errorf("error getting log data: %s", err)
					continue
				} else if (resp.StatusCode == http.StatusNotFound && logTell[fnm] == 0) ||
					(resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && logTell[fnm] > 0) {
					continue
				} else if resp.StatusCode >= 300 {
					log.Errorf("error getting log data: %s", resp.Status)
					continue
				}
				logdata, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Errorf("error reading log data: %s", err)
					continue
				}
				if len(logdata) == 0 {
					continue
				}
				for {
					eol := bytes.Index(logdata, []byte{'\n'})
					if eol < 0 {
						break
					}
					line := string(logdata[:eol])
					logdata = logdata[eol+1:]
					logTell[fnm] += int64(eol + 1)
					if len(line) == 0 {
						continue
					}
					any = true
					if fnm == "stderr.txt" {
						fmt.Fprint(os.Stderr, neednewline)
						neednewline = ""
						log.Print(line)
					} else if fnm == "crunchstat.txt" {
						m := reCrunchstat.FindStringSubmatch(line)
						if m != nil {
							rss, _ := strconv.ParseInt(m[1], 10, 64)
							fmt.Fprintf(os.Stderr, "%s rss %.3f GB           \r", cr.UUID, float64(rss)/1e9)
							neednewline = "\n"
						}
					}
				}
			}
			if any {
				logWait = logWaitMin
			} else {
				logWait = logWait * 2
				if logWait > logWaitMax {
					logWait = logWaitMax
				}
			}
			logWaitDone = time.After(logWait)
		}
	}
	fmt.Fprint(os.Stderr, neednewline)

	if err := ctx.Err(); err != nil {
		return "", err
	}

	var c arvados.Container
	err = runner.Client.RequestAndDecode(&c, "GET", "arvados/v1/containers/"+cr.ContainerUUID, nil, nil)
	if err != nil {
		return "", err
	} else if c.State != arvados.ContainerStateComplete {
		return "", fmt.Errorf("container did not complete: %s", c.State)
	} else if c.ExitCode != 0 {
		return "", fmt.Errorf("container exited %d", c.ExitCode)
	}
	return cr.OutputUUID, err
}

var collectionInPathRe = regexp.MustCompile(`^(.*/)?([0-9a-f]{32}\+[0-9]+|[0-9a-z]{5}-[0-9a-z]{5}-[0-9a-z]{15})(/.*)?$`)

func (runner *arvadosContainerRunner) TranslatePaths(paths ...*string) error {
	if runner.Mounts == nil {
		runner.Mounts = make(map[string]map[string]interface{})
	}
	for _, path := range paths {
		if *path == "" || *path == "-" {
			continue
		}
		m := collectionInPathRe.FindStringSubmatch(*path)
		if m == nil {
			return fmt.Errorf("cannot find uuid in path: %q", *path)
		}
		collID := m[2]
		mnt, ok := runner.Mounts["/mnt/"+collID]
		if !ok {
			mnt = map[string]interface{}{
				"kind": "collection",
			}
			if len(collID) == 27 {
				mnt["uuid"] = collID
			} else {
				mnt["portable_data_hash"] = collID
			}
			runner.Mounts["/mnt/"+collID] = mnt
		}
		*path = "/mnt/" + collID + m[3]
	}
	return nil
}

var mtxMakeCommandCollection sync.Mutex

func (runner *arvadosContainerRunner) makeCommandCollection() (string, error) {
	mtxMakeCommandCollection.Lock()
	defer mtxMakeCommandCollection.Unlock()
	exe, err := ioutil.ReadFile("/proc/self/exe")
	if err != nil {
		return "", err
	}
	b2 := blake2b.Sum256(exe)
	cname := "lightning " + cmd.Version.String() // must build with "make", not just "go install"
	var existing arvados.CollectionList
	err = runner.Client.RequestAndDecode(&existing, "GET", "arvados/v1/collections", nil, arvados.ListOptions{
		Limit: 1,
		Count: "none",
		Filters: []arvados.Filter{
			{Attr: "name", Operator: "=", Operand: cname},
			{Attr: "owner_uuid", Operator: "=", Operand: runner.ProjectUUID},
			{Attr: "properties.blake2b", Operator: "=", Operand: fmt.Sprintf("%x", b2)},
		},
	})
	if err != nil {
		return "", err
	}
	if len(existing.Items) > 0 {
		coll := existing.Items[0]
		log.Printf("using lightning binary in existing collection %s (name is %q, hash is %q; did not verify whether content matches)", coll.UUID, cname, coll.Properties["blake2b"])
		return coll.UUID, nil
	}
	log.Printf("writing lightning binary to new collection %q", cname)
	ac, err := arvadosclient.New(runner.Client)
	if err != nil {
		return "", err
	}
	kc := keepclient.New(ac)
	var coll arvados.Collection
	fs, err := coll.FileSystem(runner.Client, kc)
	if err != nil {
		return "", err
	}
	f, err := fs.OpenFile("lightning", os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "", err
	}
	_, err = f.Write(exe)
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	mtxt, err := fs.MarshalManifest(".")
	if err != nil {
		return "", err
	}
	err = runner.Client.RequestAndDecode(&coll, "POST", "arvados/v1/collections", nil, map[string]interface{}{
		"collection": map[string]interface{}{
			"owner_uuid":    runner.ProjectUUID,
			"manifest_text": mtxt,
			"name":          cname,
			"properties": map[string]interface{}{
				"blake2b": fmt.Sprintf("%x", b2),
			},
		},
	})
	if err != nil {
		return "", err
	}
	log.Printf("stored lightning binary in new collection %s", coll.UUID)
	return coll.UUID, nil
}

// zopen returns a reader for the given file, using the arvados API
// instead of arv-mount/fuse where applicable, and transparently
// decompressing the input if fnm ends with ".gz".
func zopen(fnm string) (io.ReadCloser, error) {
	f, err := open(fnm)
	if err != nil || !strings.HasSuffix(fnm, ".gz") {
		return f, err
	}
	rdr, err := pgzip.NewReader(bufio.NewReaderSize(f, 4*1024*1024))
	if err != nil {
		f.Close()
		return nil, err
	}
	return gzipr{rdr, f}, nil
}

// gzipr wraps a ReadCloser and a Closer, presenting a single Close()
// method that closes both wrapped objects.
type gzipr struct {
	io.ReadCloser
	io.Closer
}

func (gr gzipr) Close() error {
	e1 := gr.ReadCloser.Close()
	e2 := gr.Closer.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

var (
	arvadosClientFromEnv = arvados.NewClientFromEnv()
	keepClient           *keepclient.KeepClient
	siteFS               arvados.CustomFileSystem
	siteFSMtx            sync.Mutex
)

type file interface {
	io.ReadCloser
	io.Seeker
	Readdir(n int) ([]os.FileInfo, error)
}

func open(fnm string) (file, error) {
	if os.Getenv("ARVADOS_API_HOST") == "" {
		return os.Open(fnm)
	}
	m := collectionInPathRe.FindStringSubmatch(fnm)
	if m == nil {
		return os.Open(fnm)
	}
	collectionUUID := m[2]
	collectionPath := m[3]

	siteFSMtx.Lock()
	defer siteFSMtx.Unlock()
	if siteFS == nil {
		log.Info("setting up Arvados client")
		ac, err := arvadosclient.New(arvadosClientFromEnv)
		if err != nil {
			return nil, err
		}
		ac.Client = arvados.DefaultSecureClient
		keepClient = keepclient.New(ac)
		// Don't use keepclient's default short timeouts.
		keepClient.HTTPClient = arvados.DefaultSecureClient
		keepClient.BlockCache = &keepclient.BlockCache{MaxBlocks: 4}
		siteFS = arvadosClientFromEnv.SiteFileSystem(keepClient)
	} else {
		keepClient.BlockCache.MaxBlocks += 2
	}

	log.Infof("reading %q from %s using Arvados client", collectionPath, collectionUUID)
	f, err := siteFS.Open("by_id/" + collectionUUID + collectionPath)
	if err != nil {
		return nil, err
	}
	return &reduceCacheOnClose{file: f}, nil
}

type reduceCacheOnClose struct {
	file
	once sync.Once
}

func (rc *reduceCacheOnClose) Close() error {
	rc.once.Do(func() { keepClient.BlockCache.MaxBlocks -= 2 })
	return rc.file.Close()
}
