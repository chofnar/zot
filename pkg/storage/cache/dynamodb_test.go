package cache_test

import (
	"os"
	"path"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
	"zotregistry.io/zot/pkg/storage/cache"
)

func skipIt(t *testing.T) {
	t.Helper()

	if os.Getenv("DYNAMODBMOCK_ENDPOINT") == "" {
		t.Skip("Skipping testing without AWS DynamoDB mock server")
	}
}

func TestDynamoDB(t *testing.T) {
	skipIt(t)
	Convey("Test dynamoDB", t, func(c C) {
		log := log.NewLogger("debug", "")
		dir := t.TempDir()

		cacheDriver, err := storage.Create("dynamodb", cache.DynamoDBDriverParameters{
			Endpoint: "http://brokenlink",
		}, log)
		So(cacheDriver, ShouldNotBeNil)
		So(err, ShouldBeNil)

		val, err := cacheDriver.GetBlob("key")
		So(err, ShouldNotBeNil)
		So(val, ShouldBeEmpty)

		err = cacheDriver.PutBlob("key", path.Join(dir, "value"))
		So(err, ShouldNotBeNil)

		exists := cacheDriver.HasBlob("key", path.Join(dir, "value"))
		So(exists, ShouldBeFalse)

		err = cacheDriver.DeleteBlob("key", path.Join(dir, "value"))
		So(err, ShouldNotBeNil)

		cacheDriver, err = storage.Create("dynamodb", cache.DynamoDBDriverParameters{
			Endpoint: os.Getenv("DYNAMODBMOCK_ENDPOINT"),
		}, log)
		So(cacheDriver, ShouldNotBeNil)
		So(err, ShouldBeNil)

		returnedName := cacheDriver.Name()
		So(returnedName, ShouldEqual, "dynamodb")

		val, err = cacheDriver.GetBlob("key")
		So(err, ShouldNotBeNil)
		So(val, ShouldBeEmpty)

		err = cacheDriver.PutBlob("key", path.Join(dir, "value"))
		So(err, ShouldBeNil)

		err = cacheDriver.PutBlob("emptypath", "")
		So(err, ShouldNotBeNil)

		val, err = cacheDriver.GetBlob("key")
		So(err, ShouldBeNil)
		So(val, ShouldNotBeEmpty)

		exists = cacheDriver.HasBlob("key", path.Join(dir, "value"))
		So(exists, ShouldBeTrue)

		err = cacheDriver.DeleteBlob("key", path.Join(dir, "value"))
		So(err, ShouldBeNil)

		exists = cacheDriver.HasBlob("key", path.Join(dir, "value"))
		So(exists, ShouldBeFalse)

		err = cacheDriver.PutBlob("key", path.Join(dir, "value1"))
		So(err, ShouldBeNil)

		err = cacheDriver.PutBlob("key", path.Join(dir, "value2"))
		So(err, ShouldBeNil)

		err = cacheDriver.DeleteBlob("key", path.Join(dir, "value1"))
		So(err, ShouldBeNil)

		exists = cacheDriver.HasBlob("key", path.Join(dir, "value2"))
		So(exists, ShouldBeTrue)

		exists = cacheDriver.HasBlob("key", path.Join(dir, "value1"))
		So(exists, ShouldBeFalse)

		err = cacheDriver.DeleteBlob("key", path.Join(dir, "value2"))
		So(err, ShouldBeNil)
	})
}
