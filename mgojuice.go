package mgojuice

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/TripolisSolutions/tokyo_campaign_engine/utilities"
	"github.com/golang/glog"
	"github.com/kelseyhightower/envconfig"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	// MasterSession provides direct access to master database.
	MasterSession = "master"

	// MonotonicSession provides reads to slaves.
	MonotonicSession = "monotonic"
)

var (
	// Reference to the singleton.
	singleton mongoManager
)

type (
	// mongoConfiguration contains settings for initialization.
	mongoConfiguration struct {
		Hosts    string
		Database string
		UserName string
		Password string
	}

	// mongoManager contains dial and session information.
	mongoSession struct {
		mongoDBDialInfo *mgo.DialInfo
		mongoSession    *mgo.Session
	}

	// mongoManager manages a map of session.
	mongoManager struct {
		sessions map[string]mongoSession
	}

	// DBCall defines a type of function that can be used
	// to excecute code against MongoDB.
	DBCall       func(*mgo.Collection) error
	DBUpdateCall func(*mgo.Collection) (*mgo.ChangeInfo, error)
)

// Startup brings the manager to a running state.
func Startup() error {
	// If the system has already been started ignore the call.
	if singleton.sessions != nil {
		return nil
	}

	if glog.V(2) {
		glog.Info("Application Startup!")
	}

	// Pull in the configuration.
	var config mongoConfiguration
	if err := envconfig.Process("mongodb", &config); err != nil {
		glog.Fatalf("Error[%s] while fetching ENV variables on Startup", err)
		return err
	}

	if ret_val, err := utilities.IsStructEmpty(&config); err != nil {
		return err
	} else if ret_val {
		glog.Warning("Using default values of unset ENV variables for MongoDB connection")
	}

	// Create the Mongo Manager.
	singleton = mongoManager{
		sessions: make(map[string]mongoSession),
	}

	// Log the mongodb connection straps.
	if glog.V(2) {
		glog.Infof("Startup - MongoDB : Hosts[%s]", config.Hosts)
		glog.Infof("Startup - MongoDB : Database[%s]", config.Database)
		glog.Infof("Startup - MongoDB : Username[%s]", config.UserName)
	}

	hosts := strings.Split(config.Hosts, ",")

	// Create the strong session.
	if err := CreateSession("strong", MasterSession, hosts, config.Database, config.UserName, config.Password); err != nil {
		glog.Fatalf("Error[%s] while creating a 'strong' Mongodb session", err)
		return err
	}

	// Create the monotonic session.
	if err := CreateSession("monotonic", MonotonicSession, hosts, config.Database, config.UserName, config.Password); err != nil {
		glog.Fatalf("Error[%s] while creating a 'monotonic' Mongodb session", err)
		return err
	}

	if glog.V(2) {
		glog.Info("Startup completed!")
	}
	return nil
}

// Shutdown systematically brings the manager down gracefully.
func Shutdown() error {
	if glog.V(2) {
		glog.Info("Shutdown starting ..")
	}

	// Close the databases
	for _, session := range singleton.sessions {
		CloseSession(session.mongoSession)
	}

	if glog.V(2) {
		glog.Info("Shutdown completed")
	}
	return nil
}

// CreateSession creates a connection pool for use.
func CreateSession(mode string, sessionName string, hosts []string, databaseName string, username string, password string) error {
	if glog.V(2) {
		glog.Infof("CreateSession - Mode[%s] SessionName[%s] Hosts[%s] DatabaseName[%s] Username[%s]", mode, sessionName, hosts, databaseName, username)
	}

	// Create the database object
	mongoSession := mongoSession{
		mongoDBDialInfo: &mgo.DialInfo{
			Addrs:    hosts,
			Timeout:  60 * time.Second,
			Database: databaseName,
			Username: username,
			Password: password,
		},
	}

	// Establish the master session.
	var err error
	mongoSession.mongoSession, err = mgo.DialWithInfo(mongoSession.mongoDBDialInfo)
	if err != nil {
		if glog.V(2) {
			glog.Infof("CreateSession error [%s]", err)
		}
		return err
	}

	switch mode {
	case "strong":
		// Reads and writes will always be made to the master server using a
		// unique connection so that reads and writes are fully consistent,
		// ordered, and observing the most up-to-date data.
		// http://godoc.org/github.com/finapps/mgo#Session.SetMode
		mongoSession.mongoSession.SetMode(mgo.Strong, true)
		break

	case "monotonic":
		// Reads may not be entirely up-to-date, but they will always see the
		// history of changes moving forward, the data read will be consistent
		// across sequential queries in the same session, and modifications made
		// within the session will be observed in following queries (read-your-writes).
		// http://godoc.org/github.com/finapps/mgo#Session.SetMode
		mongoSession.mongoSession.SetMode(mgo.Monotonic, true)
	}

	// Have the session check for errors.
	// http://godoc.org/github.com/finapps/mgo#Session.SetSafe
	mongoSession.mongoSession.SetSafe(&mgo.Safe{})

	// Add the database to the map.
	singleton.sessions[sessionName] = mongoSession
	glog.Info(mongoSession.mongoSession.BuildInfo())

	if glog.V(2) {
		glog.Info("CreateSession completed")
	}
	return nil
}

// CopyMasterSession makes a copy of the master session for client use.
func CopyMasterSession() (*mgo.Session, error) {
	return CopySession(MasterSession)
}

// CopyMonotonicSession makes a copy of the monotonic session for client use.
func CopyMonotonicSession() (*mgo.Session, error) {
	return CopySession(MonotonicSession)
}

// CopySession makes a copy of the specified session for client use.
func CopySession(useSession string) (*mgo.Session, error) {
	if glog.V(2) {
		glog.Infof("CopySession UseSession[%s]", useSession)
	}

	// Find the session object.
	session := singleton.sessions[useSession]

	if session.mongoSession == nil {
		err := fmt.Errorf("Unable To Locate Session[%s]", useSession)
		glog.Fatalf("Error[%s] while CopySession", err)
		return nil, err
	}

	// Copy the master session.
	mongoSession := session.mongoSession.Copy()

	if glog.V(2) {
		glog.Info("CopySession completed")
	}
	return mongoSession, nil
}

// CloneMasterSession makes a clone of the master session for client use.
func CloneMasterSession() (*mgo.Session, error) {
	return CloneSession(MasterSession)
}

// CloneMonotonicSession makes a clone of the monotinic session for client use.
func CloneMonotonicSession() (*mgo.Session, error) {
	return CloneSession(MonotonicSession)
}

// CloneSession makes a clone of the specified session for client use.
func CloneSession(useSession string) (*mgo.Session, error) {
	if glog.V(2) {
		glog.Info("CloneSession started: UseSession[%s]", useSession)
	}

	// Find the session object.
	session := singleton.sessions[useSession]

	if session.mongoSession == nil {
		err := fmt.Errorf("Unable To Locate Session[%s]", useSession)
		glog.Fatalf("Error[%s] while CloneSession", err)
		return nil, err
	}

	// Clone the master session.
	mongoSession := session.mongoSession.Clone()

	if glog.V(2) {
		glog.Info("CloneSession completed")
	}
	return mongoSession, nil
}

// CloseSession puts the connection back into the pool.
func CloseSession(mongoSession *mgo.Session) {
	if glog.V(2) {
		glog.Info("CloseSession started")
	}
	mongoSession.Close()
	if glog.V(2) {
		glog.Info("CloseSession completed")
	}
}

// GetDatabase returns a reference to the specified database.
func GetDatabase(mongoSession *mgo.Session, useDatabase string) *mgo.Database {
	return mongoSession.DB(useDatabase)
}

// GetCollection returns a reference to a collection for the specified database and collection name.
func GetCollection(mongoSession *mgo.Session, useDatabase string, useCollection string) *mgo.Collection {
	return mongoSession.DB(useDatabase).C(useCollection)
}

// CollectionExists returns true if the collection name exists in the specified database.
func CollectionExists(mongoSession *mgo.Session, useDatabase string, useCollection string) bool {
	database := mongoSession.DB(useDatabase)
	collections, err := database.CollectionNames()

	if err != nil {
		return false
	}

	for _, collection := range collections {
		if collection == useCollection {
			return true
		}
	}

	return false
}

// ToString converts the quer map to a string.
func ToString(queryMap interface{}) string {
	json, err := json.Marshal(queryMap)
	if err != nil {
		return ""
	}

	return string(json)
}

// ToStringD converts bson.D to a string.
func ToStringD(queryMap bson.D) string {
	json, err := json.Marshal(queryMap)
	if err != nil {
		return ""
	}

	return string(json)
}

// Execute the MongoDB literal function.
func Execute(sessionName string, collectionName string, dbCall DBCall) error {
	mongoSession, err := PrepareMongoSession(sessionName)
	if err != nil {
		return err
	}
	defer CloseSession(mongoSession)
	if glog.V(2) {
		glog.Infof("Execute: Database[%s] Collection[%s]", mongoSession.DB("").Name, collectionName)
	}

	/* Capture the specified collection.
	DB returns a value representing the named database. If name is empty, the database name provided
	in the dialed URL is used instead. If that is also empty, "test" is used as a fallback in a way equivalent to the mongo shell.*/

	collection := GetCollection(mongoSession, mongoSession.DB("").Name, collectionName)

	// Execute the MongoDB call.
	err = dbCall(collection)
	if err != nil {
		glog.Error(err)
		return err
	}

	if glog.V(2) {
		glog.Info("Execute completed")
	}

	return nil
}

// Execute Mongos find and modify method
func FindAndModify(sessionName string, collectionName string, dbCall DBUpdateCall) error {
	mongoSession, err := PrepareMongoSession(sessionName)
	if err != nil {
		return err
	}
	defer CloseSession(mongoSession)

	if glog.V(2) {
		glog.Infof("Execute: Database[%s] Collection[%s]", mongoSession.DB("").Name, collectionName)
	}

	/* Capture the specified collection.
	DB returns a value representing the named database. If name is empty, the database name provided
	in the dialed URL is used instead. If that is also empty, "test" is used as a fallback in a way equivalent to the mongo shell.*/

	collection := GetCollection(mongoSession, mongoSession.DB("").Name, collectionName)

	// Execute the MongoDB call.
	info, err := dbCall(collection)
	if err != nil {
		glog.Error(err)
		return err
	}

	if glog.V(2) {
		glog.Info(info)
		glog.Info("Execute completed")
	}
	return nil
}
