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
			if glog.V(2) {
				glog.Infof("Finding id[%s] in collection[%s]", id, coll)
			}
			return collection.FindId(bson.ObjectIdHex(id)).Select(selector).One(C)
		}); err != nil {
		return err
	}
	return nil
}

func FindByPrimaryKey(C interface{}, coll string, primaryKey string, value string) error {
	if err := Execute("monotonic", coll,
		func(collection *mgo.Collection) error {
			if glog.V(2) {
				glog.Infof("Finding field[%s] with value[%s] in collection[%s]", primaryKey, value, coll)
			}
			return collection.Find(bson.M{primaryKey: value}).One(C)
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

func RemoveDocId(coll string, id string) error {
	if err := Execute("monotonic", coll,
		func(collection *mgo.Collection) error {
			if glog.V(2) {
				glog.Infof("Removing id[%s] in collection[%s]", id, coll)
			}
			return collection.RemoveId(bson.ObjectIdHex(id))
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
