package mgojuice

import (
	"errors"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func FindById(C interface{}, coll string, id string, selector interface{}) error {
	if err := Execute("monotonic", coll,
		func(collection *mgo.Collection) error {
			glog.Infof("Finding id[%s] in collection[%s]", id, coll)
			return collection.FindId(bson.ObjectIdHex(id)).Select(selector).One(C)
		}); err != nil {
		return err
	}
	return nil
}

func PrepareMongoSession(sessionConst string) (*mgo.Session, error) {
	var session *mgo.Session
	var err error
	if sessionConst == "monotonic" {
		session, err = CopyMonotonicSession()
	} else if sessionConst == "master" {
		session, err = CopyMasterSession()
	} else {
		return nil, errors.New("Supplied mongo session is not supported")
	}
	if err != nil {
		glog.Errorf("Error[%s] while preparing a Mongo monotonic session", err)
		return nil, err
	}

	return session, err
}

func InsertTestDocument(coll string, docs ...interface{}) error {
	if err := Execute("master", coll,
		func(collection *mgo.Collection) error {
			return collection.Insert(docs[0])
		}); err != nil {
		return err
	}
	return nil
}

func EmptyTestColl(coll string) error {
	if err := Execute("master", coll,
		func(collection *mgo.Collection) error {
			return collection.DropCollection()
		}); err != nil {
		return err
	}
	return nil
}
