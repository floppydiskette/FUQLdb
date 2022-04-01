package main

import (
	"errors"
	"fmt"
	"github.com/floppydiskette/configparser"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

/*
FUQLdb
the Fucked-Up-Query-Language database (:
*/

// Permissions
const (
	PermRead = iota
	PermWrite
	PermAdmin
)

type Permission int

type Entry struct {
	Key   interface{}
	Value interface{}
}

type Table struct {
	Name string
	Data []Entry
}

type User struct {
	Name                 string
	Password             string
	FakePassword         string
	SocialSecurityNumber string
	Permissions          []Permission
}

type Database struct {
	Name              string
	Tables            []Table
	timeSinceLastSave int
}

const (
	DemandUseDatabase = iota
	DemandUseTable
	DemandFindEntry
	DemandFindEntries
	DemandAddEntry
	DemandSetEntry
	DemandSetEntries
	DemandDeleteEntry
	DemandDeleteEntries
	DemandCreateTable
	DemandCreateDatabase
	DemandCreateUser
	DemandDeleteTable
	DemandDeleteDatabase
	DemandDeleteUser
	DemandLogin

	// internal demands
	DemandGetContextFromUUID
	DemandCreateNewContext
)

type DemandType int

type Demand struct {
	TypeOfDemand      DemandType
	Data              interface{}
	ReturnChannel     chan interface{}
	AssociatedContext *Context
}

type InternalDemand struct {
	TypeOfDemand  DemandType
	Data          interface{}
	ReturnChannel chan interface{}
}

type Context struct {
	UUID          string
	DatabaseInUse int
	TableInUse    int
	UserInUse     string
}

var config map[string]interface{}

const secondsBetweenAutoSave int = 60

var dbs []Database
var contexts []Context

func setup() {
	// if os is windows, get config from C:\fuqldb\config.conf
	// if os is linux, get config from /etc/fuqldb/config.conf
	if os.Getenv("OS") == "Windows_NT" {
		tmpConfig, err := configparser.LoadConfig("C:\\fuqldb\\config.conf")
		if err != nil {
			panic(err)
		}
		config = tmpConfig
	} else {
		tmpConfig, err := configparser.LoadConfig("/etc/fuqldb/config.conf")
		if err != nil {
			panic(err)
		}
		config = tmpConfig
	}

	// make sure the config file has the right keys
	if _, ok := config["database_storage_path"]; !ok {
		panic("config file is missing database_storage_path key")
	}
	if _, ok := config["database_name"]; !ok {
		panic("config file is missing database_name key")
	}
	if _, ok := config["sex_number"]; !ok {
		panic("config file is missing sex_number key")
	}

	// load databases from storage path
	files, err := os.ReadDir(config["database_storage_path"].(string))
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		newDatabase, err := loadDB(file.Name())
		if err != nil {
			panic(err)
		}
		newDatabase.timeSinceLastSave = 0
		dbs = append(dbs, newDatabase)
	}
}

func initDB(name string, storagePath string, sex int) error {
	outConfig := fmt.Sprintf("database_storage_path=\"%s\"\ndatabase_name=\"%s\"\nsex_number=%d\n", storagePath, name, sex)
	// write to correct file for operating system
	var configPath string
	if os.Getenv("OS") == "Windows_NT" {
		configPath = "C:\\fuqldb\\config.conf"
	} else {
		configPath = "etc/fuqldb/config.conf"
	}
	file, err := os.Create(configPath)
	if err != nil {
		return err
	}
	_, err = file.WriteString(outConfig)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func serializeDB(db Database) string {
	var out string
	for _, table := range db.Tables {
		// escape :
		table.Name = strings.Replace(table.Name, ":", "\\:", -1)
		out += fmt.Sprintf("%s:\n", table.Name)
		for _, entry := range table.Data {
			// escape :
			key := strings.Replace(fmt.Sprintf("%v", entry.Key), ":", "\\:", -1)
			value := strings.Replace(fmt.Sprintf("%v", entry.Value), ":", "\\:", -1)
			out += fmt.Sprintf("%s:%s\n", key, value)
		}
	}
	return out
}

func parseLine(line string) (string, interface{}) {
	// split on :
	split := strings.Split(line, ":")
	// split on \:
	split[0] = strings.Replace(split[0], "\\:", ":", -1)
	split[1] = strings.Replace(split[1], "\\:", ":", -1)
	return split[0], split[1]
}

func loadDB(inFile string) (Database, error) {
	var db Database
	file, err := os.Open(inFile)
	if err != nil {
		panic(err)
	}
	for {
		var table Table
		var line string
		_, err := fmt.Fscanln(file, &line)
		if err != nil {
			return db, err
		}
		table.Name = line
		for {
			_, err := fmt.Fscanln(file, &line)
			if err != nil {
				return db, err
			}
			if line == "" {
				break
			}
			var entry Entry
			entry.Key, entry.Value = parseLine(line)
			table.Data = append(table.Data, entry)
		}
		db.Tables = append(db.Tables, table)
	}
	return db, nil
}

func (db *Database) saveDB(outFile string) error {
	file, err := os.Create(outFile)
	if err != nil {
		return err
	}
	for _, table := range db.Tables {
		// escape :
		table.Name = strings.Replace(table.Name, ":", "\\:", -1)
		_, err := fmt.Fprintf(file, "%s:\n", table.Name)
		if err != nil {
			return err
		}
		for _, entry := range table.Data {
			// escape :
			key := strings.Replace(fmt.Sprintf("%v", entry.Key), ":", "\\:", -1)
			value := strings.Replace(fmt.Sprintf("%v", entry.Value), ":", "\\:", -1)
			_, err := fmt.Fprintf(file, "%s:%s\n", key, value)
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(file, "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) getTable(name string) *Table {
	for _, table := range db.Tables {
		if table.Name == name {
			return &table
		}
	}
	return nil
}

func (tb *Table) getEntry(key interface{}) *Entry {
	for _, entry := range tb.Data {
		if entry.Key == key {
			return &entry
		}
	}
	return nil
}

func (tb *Table) addEntry(key interface{}, value interface{}) {
	entry := Entry{Key: key, Value: value}
	tb.Data = append(tb.Data, entry)
}

func (tb *Table) tellEntryToFuckOff(key interface{}) {
	for i, entry := range tb.Data {
		if entry.Key == key {
			tb.Data = append(tb.Data[:i], tb.Data[i+1:]...)
			return
		}
	}
}

func (tb *Table) changeEntry(key interface{}, value interface{}) {
	for i, entry := range tb.Data {
		if entry.Key == key {
			tb.Data[i].Value = value
			return
		}
	}
}

func (db *Database) addTable(name string) {
	table := Table{Name: name}
	db.Tables = append(db.Tables, table)
}

func (db *Database) tellTableToFuckOff(name string) {
	for i, table := range db.Tables {
		if table.Name == name {
			db.Tables = append(db.Tables[:i], db.Tables[i+1:]...)
			return
		}
	}
}

func (ctx *Context) getDB(name string) *Database {
	// make sure user has read permissions
	var systemDB *Database
	for _, db := range dbs {
		if db.Name == "users" {
			systemDB = &db
			break
		}
	}
	if systemDB == nil {
		return nil
	}
	user := systemDB.getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	for _, db := range dbs {
		if db.Name == name {
			return &db
		}
	}
	return nil
}

func (ctx *Context) getTable(dbName string, tableName string) *Table {
	// make sure user has read permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	db := ctx.getDB(dbName)
	if db == nil {
		return nil
	}
	return db.getTable(tableName)
}

func (ctx *Context) getEntry(key interface{}) *Entry {
	// make sure user has read permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	if ctx.TableInUse == -1 {
		return nil
	}
	return dbs[ctx.DatabaseInUse].Tables[ctx.TableInUse].getEntry(key)
}

func (ctx *Context) addEntry(key interface{}, value interface{}) {
	// make sure user has write permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermWrite {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	if ctx.TableInUse == -1 {
		return
	}
	dbs[ctx.DatabaseInUse].Tables[ctx.TableInUse].addEntry(key, value)
}

func (ctx *Context) tellEntryToFuckOff(key interface{}) {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	if ctx.TableInUse == -1 {
		return
	}
	dbs[ctx.DatabaseInUse].Tables[ctx.TableInUse].tellEntryToFuckOff(key)
}

func (ctx *Context) changeEntry(key interface{}, value interface{}) {
	// make sure user has write permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermWrite {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	if ctx.TableInUse == -1 {
		return
	}
	dbs[ctx.DatabaseInUse].Tables[ctx.TableInUse].changeEntry(key, value)
}

func (ctx *Context) addTable(name string) {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	if ctx.DatabaseInUse == -1 {
		return
	}
	dbs[ctx.DatabaseInUse].addTable(name)
}

func (ctx *Context) tellTableToFuckOff(name string) {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	if ctx.DatabaseInUse == -1 {
		return
	}
	dbs[ctx.DatabaseInUse].tellTableToFuckOff(name)
}

func (ctx *Context) getDBNames() []string {
	// make sure user has read permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	var names []string
	for _, db := range dbs {
		names = append(names, db.Name)
	}
	return names
}

func (ctx *Context) getTableNames(dbName string) []string {
	// make sure user has read permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	db := ctx.getDB(dbName)
	if db == nil {
		return nil
	}
	var names []string
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

func (ctx *Context) getEntryKeys() []interface{} {
	// make sure user has read permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	table := dbs[ctx.DatabaseInUse].Tables[ctx.TableInUse]
	var keys []interface{}
	for _, entry := range table.Data {
		keys = append(keys, entry.Key)
	}
	return keys
}

func (ctx *Context) getEntryValues() []interface{} {
	// make sure user has read permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return nil
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermRead {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return nil
	}
	table := dbs[ctx.DatabaseInUse].Tables[ctx.TableInUse]
	var values []interface{}
	for _, entry := range table.Data {
		values = append(values, entry.Value)
	}
	return values
}

func (ctx *Context) addDatabase(name string) {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	db := Database{Name: name}
	dbs = append(dbs, db)
}

func (ctx *Context) tellDatabaseToFuckOff(name string) {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return
	}
	for i, db := range dbs {
		if db.Name == name {
			dbs = append(dbs[:i], dbs[i+1:]...)
			return
		}
	}
}

func (ctx *Context) useDatabase(name string) error {
	for i, db := range dbs {
		if db.Name == name {
			ctx.DatabaseInUse = i
			ctx.TableInUse = -1
			return nil
		}
	}
	return errors.New("database not found")
}

func (ctx *Context) useTable(name string) error {
	db := dbs[ctx.DatabaseInUse]
	for i, table := range db.Tables {
		if table.Name == name {
			ctx.TableInUse = i
			return nil
		}
	}
	return errors.New("table not found")
}

func (ctx *Context) createUser(name string, password string) error {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return errors.New("user not found")
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return errors.New("permission denied")
	}
	// put user into system database
	database := ctx.getDB("users")
	if database == nil {
		return errors.New("system database not found")
	}
	table := database.getTable("users")
	if table == nil {
		return errors.New("users table not found")
	}
	table.addEntry(name, password)
	return nil
}

func (ctx *Context) deleteUser(name string) error {
	// make sure user has admin permissions
	user := ctx.getDB("users").getTable(dbs[ctx.DatabaseInUse].Name).getEntry(ctx.UserInUse)
	if user == nil {
		return errors.New("user not found")
	}
	foundPermission := false
	for _, permission := range user.Value.(User).Permissions {
		if permission == PermAdmin {
			foundPermission = true
			break
		}
	}
	if !foundPermission {
		return errors.New("permission denied")
	}
	// delete user from system database
	database := ctx.getDB("users")
	if database == nil {
		return errors.New("system database not found")
	}
	table := database.getTable("users")
	if table == nil {
		return errors.New("users table not found")
	}
	table.tellEntryToFuckOff(name)
	return nil
}

func (ctx *Context) login(name string, password string) error {
	// check user in system database
	database := ctx.getDB("users")
	if database == nil {
		return errors.New("system database not found")
	}
	table := database.getTable("users")
	if table == nil {
		return errors.New("users table not found")
	}
	entry := table.getEntry(name)
	if entry == nil {
		return errors.New("user not found")
	}
	if entry.Value != password {
		return errors.New("incorrect password")
	}
	// set user as current user
	ctx.UserInUse = name
	return nil
}

func (ctx *Context) demandHandler(d Demand) (interface{}, error) {
	switch d.TypeOfDemand {
	case DemandCreateDatabase:
		// make sure that the data of the demand is a string (the name of the database)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		ctx.addDatabase(d.Data.(string))
	case DemandCreateTable:
		// make sure that the data of the demand is a string (the name of the table)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		ctx.addTable(d.Data.(string))
	case DemandAddEntry:
		// make sure that the data of the demand is a string (key,value)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		key := strings.Split(d.Data.(string), ",")[0]
		value := strings.Split(d.Data.(string), ",")[1]
		ctx.addEntry(key, value)
	case DemandSetEntry:
		// make sure that the data of the demand is a string array (the key and value of the entry)
		if _, ok := d.Data.([]interface{}); !ok {
			return nil, errors.New("demand data is not a string array")
		}
		if len(d.Data.([]interface{})) != 2 {
			return nil, errors.New("demand data is not a string array of length 2")
		}
		// if entry already exists, change it
		if entry := ctx.getEntry(d.Data.([]interface{})[0]); entry != nil {
			ctx.changeEntry(d.Data.([]interface{})[0], d.Data.([]interface{})[1])
		} else {
			ctx.addEntry(d.Data.([]interface{})[0], d.Data.([]interface{})[1])
		}
	case DemandDeleteEntry:
		// make sure that the data of the demand is a string (the key of the entry)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		ctx.tellEntryToFuckOff(d.Data.(string))
	case DemandDeleteTable:
		// make sure that the data of the demand is a string (the name of the table)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		ctx.tellTableToFuckOff(d.Data.(string))
	case DemandDeleteDatabase:
		// make sure that the data of the demand is a string (the name of the database)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		ctx.tellDatabaseToFuckOff(d.Data.(string))
	case DemandFindEntry:
		// make sure that the data of the demand is a string (the key of the entry)
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		if entry := ctx.getEntry(d.Data.(string)); entry != nil {
			return entry.Value, nil
		}
		return nil, nil
	case DemandFindEntries:
		// data should be an interface array, first being a bool (true if searching by key, false if searching by value), second being a regex
		if _, ok := d.Data.([]interface{}); !ok {
			return nil, errors.New("demand data is not an interface array")
		}
		if len(d.Data.([]interface{})) != 2 {
			return nil, errors.New("demand data is not an interface array of length 2")
		}
		if _, ok := d.Data.([]interface{})[0].(bool); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, first element is not a bool")
		}
		if _, ok := d.Data.([]interface{})[1].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, second element is not a string")
		}
		var keys []interface{}
		if d.Data.([]interface{})[0].(bool) {
			keys = ctx.getEntryKeys()
		} else {
			keys = ctx.getEntryValues()
		}
		var matches []interface{}
		for _, key := range keys {
			if regexp.MustCompile(d.Data.([]interface{})[1].(string)).MatchString(key.(string)) {
				matches = append(matches, key)
			}
		}
		return matches, nil
	case DemandSetEntries:
		// set entries should be an interface array, first being a bool (true if searching by key, false if searching by value), second being a regex, third being the value to set
		if _, ok := d.Data.([]interface{}); !ok {
			return nil, errors.New("demand data is not an interface array")
		}
		if len(d.Data.([]interface{})) != 3 {
			return nil, errors.New("demand data is not an interface array of length 3")
		}
		if _, ok := d.Data.([]interface{})[0].(bool); !ok {
			return nil, errors.New("demand data is not an interface array of length 3, first element is not a bool")
		}
		if _, ok := d.Data.([]interface{})[1].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 3, second element is not a string")
		}
		if _, ok := d.Data.([]interface{})[2].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 3, third element is not a string")
		}
		var keys []interface{}
		if d.Data.([]interface{})[0].(bool) {
			keys = ctx.getEntryKeys()
		} else {
			keys = ctx.getEntryValues()
		}
		for _, key := range keys {
			if regexp.MustCompile(d.Data.([]interface{})[1].(string)).MatchString(key.(string)) {
				ctx.changeEntry(key.(string), d.Data.([]interface{})[2].(string))
			}
		}
	case DemandDeleteEntries:
		// delete entries should be an interface array, first being a bool (true if searching by key, false if searching by value), second being a regex
		if _, ok := d.Data.([]interface{}); !ok {
			return nil, errors.New("demand data is not an interface array")
		}
		if len(d.Data.([]interface{})) != 2 {
			return nil, errors.New("demand data is not an interface array of length 2")
		}
		if _, ok := d.Data.([]interface{})[0].(bool); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, first element is not a bool")
		}
		if _, ok := d.Data.([]interface{})[1].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, second element is not a string")
		}
		var keys []interface{}
		if d.Data.([]interface{})[0].(bool) {
			keys = ctx.getEntryKeys()
		} else {
			keys = ctx.getEntryValues()
		}
		for _, key := range keys {
			if regexp.MustCompile(d.Data.([]interface{})[1].(string)).MatchString(key.(string)) {
				ctx.tellEntryToFuckOff(key.(string))
			}
		}
	case DemandUseDatabase:
		// data should be a string
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		if ok := ctx.useDatabase(d.Data.(string)); ok != nil {
			return nil, ok
		}
	case DemandUseTable:
		// data should be a string
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		if ok := ctx.useTable(d.Data.(string)); ok != nil {
			return nil, ok
		}
	case DemandCreateUser:
		// data should be a string array, first being the username, second being the password
		// put this into the users database
		if _, ok := d.Data.([]interface{}); !ok {
			return nil, errors.New("demand data is not an interface array")
		}
		if len(d.Data.([]interface{})) != 2 {
			return nil, errors.New("demand data is not an interface array of length 2")
		}
		if _, ok := d.Data.([]interface{})[0].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, first element is not a string")
		}
		if _, ok := d.Data.([]interface{})[1].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, second element is not a string")
		}
		if ok := ctx.createUser(d.Data.([]interface{})[0].(string), d.Data.([]interface{})[1].(string)); ok != nil {
			return nil, ok
		}
	case DemandDeleteUser:
		// data should be a string (username)
		// make sure the context's user has permission to do this
		if _, ok := d.Data.(string); !ok {
			return nil, errors.New("demand data is not a string")
		}
		if ok := ctx.deleteUser(d.Data.(string)); ok != nil {
			return nil, ok
		}
	case DemandLogin:
		// data should be a string array, first being the username, second being the password
		// make sure the user exists
		// make sure the password is correct
		// make sure the user has permission to do this
		if _, ok := d.Data.([]interface{}); !ok {
			return nil, errors.New("demand data is not an interface array")
		}
		if len(d.Data.([]interface{})) != 2 {
			return nil, errors.New("demand data is not an interface array of length 2")
		}
		if _, ok := d.Data.([]interface{})[0].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, first element is not a string")
		}
		if _, ok := d.Data.([]interface{})[1].(string); !ok {
			return nil, errors.New("demand data is not an interface array of length 2, second element is not a string")
		}
		if ok := ctx.login(d.Data.([]interface{})[0].(string), d.Data.([]interface{})[1].(string)); ok != nil {
			return nil, ok
		}
	default:
		return nil, errors.New("unknown demand type")
	}
	return nil, nil
}

func (ctx *Context) parseCommand(cmd string) (*Demand, error) {
	var d *Demand
	if cmd == "" {
		return nil, errors.New("command is empty")
	}
	insideString := false
	var commandArray []string
	// go through command and split by spaces unless it's a string
	// if it's a string, split by "
	for i, c := range cmd {
		if c == '"' {
			if i == 0 || cmd[i-1] != '\\' {
				insideString = !insideString
			}
		}
		if c == ' ' && !insideString {
			commandArray = append(commandArray, cmd[:i])
			cmd = cmd[i+1:]
			i = 0
		}
	}
	switch strings.ToLower(commandArray[0]) {
	case "use":
		switch strings.ToLower(commandArray[1]) {
		case "database":
			d.TypeOfDemand = DemandUseDatabase
			d.Data = commandArray[2]
		case "table":
			d.TypeOfDemand = DemandUseTable
			d.Data = commandArray[2]
		default:
			return nil, errors.New("unknown use command")
		}
	case "login":
		d.TypeOfDemand = DemandLogin
		d.Data = commandArray[1:]
	case "tell":
		switch strings.ToLower(commandArray[1]) {
		case "entry":
			switch strings.ToLower(commandArray[2]) {
			case "to":
				switch strings.ToLower(commandArray[3]) {
				case "present":
					// if next word is "where", then it will be finding entries
					// otherwise, the next word will be the key
					if strings.ToLower(commandArray[4]) == "where" {
						// if next word is "key", then find entries by key
						// otherwise, find entries by value
						if strings.ToLower(commandArray[5]) == "key" {
							d.TypeOfDemand = DemandFindEntries
							d.Data = []interface{}{true, commandArray[6]}
						} else if strings.ToLower(commandArray[5]) == "value" {
							d.TypeOfDemand = DemandFindEntries
							d.Data = []interface{}{false, commandArray[6]}
						} else {
							return nil, errors.New("unknown tell entry to present command")
						}
					} else {
						d.TypeOfDemand = DemandFindEntry
						// last word will be the key
						d.Data = commandArray[4]
					}
				case "create":
					// first word is key,value
					d.TypeOfDemand = DemandAddEntry
					d.Data = commandArray[4]
				case "fuck":
					// if next word is off, tell entry to fuck off
					if strings.ToLower(commandArray[4]) == "off" {
						// if next word is "where", then it will be finding entries
						// otherwise, the next word will be the key
						if strings.ToLower(commandArray[5]) == "where" {
							// if next word is "key", then find entries by key
							// otherwise, find entries by value
							if strings.ToLower(commandArray[6]) == "key" {
								d.TypeOfDemand = DemandDeleteEntries
								d.Data = []interface{}{true, commandArray[7]}
							} else if strings.ToLower(commandArray[6]) == "value" {
								d.TypeOfDemand = DemandDeleteEntries
								d.Data = []interface{}{false, commandArray[7]}
							} else {
								return nil, errors.New("unknown tell entry to fuck off command")
							}
						} else {
							d.TypeOfDemand = DemandDeleteEntry
							// last word will be the key
							d.Data = commandArray[5]
						}
					} else {
						return nil, errors.New("unknown tell entry to fuck command")
					}
				case "become":
					// if next word is "where", then it will be finding entries
					// otherwise, the next word will be the key
					if strings.ToLower(commandArray[4]) == "where" {
						// if next word is "key", then find entries by key
						// otherwise, find entries by value
						if strings.ToLower(commandArray[5]) == "key" {
							d.TypeOfDemand = DemandSetEntries
							d.Data = []interface{}{true, commandArray[6]}
						} else if strings.ToLower(commandArray[5]) == "value" {
							d.TypeOfDemand = DemandSetEntries
							d.Data = []interface{}{false, commandArray[6]}
						} else {
							return nil, errors.New("unknown tell entry to become command")
						}
					} else {
						d.TypeOfDemand = DemandSetEntry
						// last word will be the key and value separated by a comma
						d.Data = strings.Split(commandArray[4], ",")
					}
				}
			}
		case "table":
			switch strings.ToLower(commandArray[2]) {
			case "to":
				switch strings.ToLower(commandArray[3]) {
				case "create":
					// first word is name of table
					d.TypeOfDemand = DemandCreateTable
					d.Data = commandArray[4]
				case "fuck":
					// if next word is off, tell table to fuck off
					if strings.ToLower(commandArray[4]) == "off" {
						d.TypeOfDemand = DemandDeleteTable
						// last word will be the name of the table
						d.Data = commandArray[5]
					} else {
						return nil, errors.New("unknown tell table to fuck command")
					}
				}
			}
		case "database":
			switch strings.ToLower(commandArray[2]) {
			case "to":
				switch strings.ToLower(commandArray[3]) {
				case "create":
					// first word is name of database
					d.TypeOfDemand = DemandCreateDatabase
					d.Data = commandArray[4]
				case "fuck":
					// if next word is off, tell database to fuck off
					if strings.ToLower(commandArray[4]) == "off" {
						d.TypeOfDemand = DemandDeleteDatabase
						// last word will be the name of the database
						d.Data = commandArray[5]
					} else {
						return nil, errors.New("unknown tell database to fuck command")
					}
				}
			}
		}
	}

	return d, nil
}

func handleConnection(conn net.Conn, commandChannel chan *Demand) {
	defer conn.Close()
	for {
		// read until a null byte
		buf := make([]byte, 4096)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				return
			}
			if buf[0] == 0 {
				break
			}
		}
		// get uuid (first 36 bytes)
		uuid := make([]byte, 36)
		copy(uuid, buf[:36])
		// if all are 0s, the client wants to create a new context
		// send that back as a demand
		allZeros := true
		for _, char := range uuid {
			if char != '0' {
				allZeros = false
			}
		}
		if allZeros == true {
			var returnChannel chan interface{}
			newDemand := &Demand{
				TypeOfDemand:  DemandCreateNewContext,
				Data:          nil,
				ReturnChannel: returnChannel,
			}
			commandChannel <- newDemand

			// await response
			response := <-returnChannel
			// send response to client
			_, err := conn.Write([]byte(response.(string)))
			if err != nil {
				fmt.Println("WARNING: error writing to client: ", err)
			}
		} else {
			// demand to find context by uuid
			var returnChannel chan interface{}
			newDemand := &Demand{
				TypeOfDemand:  DemandGetContextFromUUID,
				Data:          uuid,
				ReturnChannel: returnChannel,
			}
			commandChannel <- newDemand

			// await response
			response := <-returnChannel
			// response should be a context pointer, check
			if response == nil {
				fmt.Println("WARNING: response from demand channel was nil")
				continue
			} else if response.(*Context) == nil {
				fmt.Println("WARNING: response from demand channel was not a context pointer")
				continue
			} else {
				// get the command (rest of the bytes)
				command := make([]byte, len(buf)-36)
				copy(command, buf[36:])
				// parse the command
				newDemand, err := response.(*Context).parseCommand(string(command))
				if err != nil {
					fmt.Println("WARNING: error parsing command: ", err)
					continue
				}
				// create a return channel and add it to the demand
				newDemand.ReturnChannel = make(chan interface{})
				// assign the context to the demand
				newDemand.AssociatedContext = response.(*Context)
				// send the demand to the command channel
				commandChannel <- newDemand
				// await response
				response = <-newDemand.ReturnChannel
				// send response to client
				_, err = conn.Write([]byte(response.(string)))
			}
		}
	}
}

func handleServer(commandChannel chan *Demand) {
	// listen on port 8008 for commands
	listener, err := net.Listen("tcp", ":8008")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConnection(conn, commandChannel)
	}
}

func main() {
	// if arg --init is passed, initialize the database
	if len(os.Args) > 1+3 && os.Args[1] == "--init" {
		sex, err := strconv.ParseInt(os.Args[4], 10, 32)
		if err != nil {
			panic("wrong sex")
		}
		err = initDB(os.Args[2], os.Args[3], int(sex))
		if err != nil {
			panic(err)
		}
		return
	} else {
		setup()
	}

	// load each database file from the database storage path
	files, err := os.ReadDir(config["database_storage_path"].(string))
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		newDatabase, err := loadDB(file.Name())
		if err != nil {
			panic(err)
		}
		newDatabase.timeSinceLastSave = 0
		dbs = append(dbs, newDatabase)
	}

	var slowDown int64 = 0

	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// create a command channel
	commandChannel := make(chan *Demand, 1000)
	go handleServer(commandChannel)
	for {
		if len(commandChannel) > 0 {
			newDemand := <-commandChannel
			if newDemand.AssociatedContext != nil {
				dataBack, err := newDemand.AssociatedContext.demandHandler(*newDemand)
				if err != nil {
					fmt.Println("WARNING: error handling demand: ", err)
				}
				if newDemand.ReturnChannel != nil {
					newDemand.ReturnChannel <- dataBack
				}
			}
		}
		if time.Now().Unix() > slowDown-30 {
			slowDown = time.Now().Unix()
			for _, db := range dbs {
				if time.Now().Unix()-int64(db.timeSinceLastSave) > int64(secondsBetweenAutoSave) {
					err := db.saveDB(config["database_storage_path"].(string) + db.Name + ".db")
					if err != nil {
						fmt.Println("WARNING: error saving database: ", err)
					}
				}
			}
		}
		// check for kill signals
		if len(sigs) > 0 {
			sig := <-sigs
			// save databases
			for _, db := range dbs {
				err := db.saveDB(config["database_storage_path"].(string) + db.Name + ".db")
				if err != nil {
					fmt.Println("WARNING: error saving database: ", err)
				}
			}
			fmt.Println("Killed by signal: ", sig)
			os.Exit(0)
		}
	}
}
