package location

import (
	"os"
	"testing"
)

func TestLocation(t *testing.T) {
	loc := NewLoc("localhost:2333")
	defer loc.Close()

	hn, err := os.Hostname()
	if err != nil {
		t.Errorf("Hostname error: %v", err)
	}
	err = loc.Report("abc")
	myurl := "http://" + hn + "/abc"
	if err != nil {
		t.Errorf("Report error: %v", err)
	}
	var lc string
	for i := 0; i < 3; i++ {
		lc, err = loc.Query("abc")
		if err != nil {
			t.Errorf("Query error: %v", err)
		}
		if lc == myurl {
			break
		}
	}
	if lc != myurl {
		t.Errorf("Object not found")
	}
}
