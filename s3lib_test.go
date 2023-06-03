package s3lib

import (
	"fmt"
	"os"
	"testing"
)

var _sess *S3Session
var _remotefolder = "aAbBcCdDeEfF123456789"
var bucket = ""

func TestInit(t *testing.T) {
	var err error
	_cfg := Config{
		S3Region:  os.Getenv("s3region"),
		Endpoint:  os.Getenv("s3endpoint"), // set to blank or nil for default AWS endpoint
		AccessKey: os.Getenv("s3accesskey"),
		SecretKey: os.Getenv("s3secret"),
	}
	bucket = os.Getenv("s3bucket")

	_sess, err = GetSession(&_cfg)
	if err != nil {
		t.Fatal("Failed to init session")
	}
}

func TestAddObjects(t *testing.T) {
	file := ""
	for i := 1; i < 4; i++ {
		file = fmt.Sprintf("wall-bg-%d.jpg", i)
		fmt.Printf("Adding file %s\n", file)
		err := _sess.UploadFile("tests/"+file, _remotefolder+"/"+file, bucket, "")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeleteObjects(t *testing.T) {
	file := "wall-bg-1.jpg"
	err := _sess.RemoveFile(bucket, _remotefolder+"/"+file)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteFolder(t *testing.T) {
	err := _sess.RemoveFolder(bucket, _remotefolder)
	if err != nil {
		t.Fatal(err)
	}
}
