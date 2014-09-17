package mgojuice

import (
	"os"
	"testing"

	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func TestPrepareMongoSession(t *testing.T) {
	assert := assert.New(t)
	os.Clearenv()
	os.Setenv("MONGODB_DATABASE", "mgojuice_test")

	if err := Startup(); err != nil {
		glog.Fatal("Mongo startup failed")
		os.Exit(1)
	}

	mongoSession, err := PrepareMongoSession(MonotonicSession)
	defer CloseSession(mongoSession)
	assert.Nil(err)
	assert.NotNil(mongoSession)
	assert.Equal(mgo.Monotonic, mongoSession.Mode())

	mongoSession, err = PrepareMongoSession(MasterSession)
	assert.Nil(err)
	assert.NotNil(mongoSession)
	assert.Equal(mgo.Strong, mongoSession.Mode())

	mongoSession, err = PrepareMongoSession("Extra Strong")
	assert.NotNil(err)
	assert.Nil(mongoSession)
}

func TestFindById(t *testing.T) {
	assert := assert.New(t)
	os.Clearenv()
	os.Setenv("MONGODB_DATABASE", "mgojuice_test")

	if err := Startup(); err != nil {
		glog.Fatal("Mongo startup failed")
		os.Exit(1)
	}
	err := InsertTestDocument("mycoll", bson.M{"_id": bson.ObjectIdHex("000000000000000000000001"), "n": 41})
	defer EmptyTestColl("mycoll")
	assert.Nil(err)

	result := struct{ N int }{}

	//Find a document with a valid ID
	err = FindById(&result, "mycoll", "000000000000000000000001")
	assert.Nil(err)
	assert.Equal(result.N, 41)

	//Find a document with a non-existing ID
	err = FindById(&result, "mycoll", "000000000000000000000002")
	assert.NotNil(err)

	//Trying to find a document in non existing collection
	err = FindById(&result, "mycoll1", "000000000000000000000001")
	assert.NotNil(err)
	err = FindById(&result, "", "000000000000000000000001")
	assert.NotNil(err)
}
