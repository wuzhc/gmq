package logs

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestFileTarget(t *testing.T) {
	var filename = "test.log"
	var msg = "hello word"

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	} else {
		fd.Close()
	}

	conf := fmt.Sprintf(`{"filename":"%v","level":10,"max_size":500,"rotate":true}`, filename)
	logger := NewDispatcher()
	logger.SetTarget(TARGET_FILE, conf)
	logger.Error(msg)

	nbyte, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(nbyte), msg) {
		t.Errorf("%v has not substring %v", string(nbyte), msg)
	}
}
