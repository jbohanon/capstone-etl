package main

import (
	"context"
	"database/sql"
	"github.com/go-redis/redis/v8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"strings"
	"time"
)

var (
	db *sql.DB
	redisdb *redis.Client
	mongodb *mongo.Client
	delimiters = [4]string{" ", "?", ".", "!"}
	allWikibooks = make(map[string]*wikibook)
	wbArr []interface{}
	wbColl *mongo.Collection
)

const (
	//workDir = `/home/jacob/Documents/WGU/capstone/`
	workDir = `C:\Users\jacob.bohanon\Documents\Code\capstone\`
	dSrcName = "file:" + workDir + "en_wikibooks.sqlite"
	dbDriver = "sqlite3"
)

type (
	wikibook struct {
		Id int `json:"_id" bson:"_id" redis:"_id"`
		Title      string      `json:"title" bson:"title" redis:"title"`
		Url        string      `json:"url" bson:"url" redis:"url"`
		Abstract   string      `json:"abstract" bson:"abstract" redis:"abstract"`
		BodyText   string      `json:"body_text" bson:"body_text" redis:"body_text"`
		BodyHtml   string      `json:"body_html" bson:"body_html" redis:"body_html"`
		childPages []*wikibook
		ChildPageIds []int `json:"child_pages,omitempty" bson:"child_pages,omitempty" redis:"child_pages"`
		parentPage *wikibook
		ParentPageId int   `json:"parent_page" bson:"parent_page" redis:"parent_page"`
	}
)

func connMongo() {
	var err error
	mongodb, err = mongo.Connect(context.Background(), nil)
	if err != nil {
		err = errors.Wrap(err, "connecting to mongodb")
		panic(err)
	}
	if err = mongodb.Ping(context.Background(), nil); err != nil {
		err = errors.Wrap(err, "pinging mongodb")
		panic(err)
	}
	wbColl = mongodb.Database("wikibooks").Collection("wikibooks")
}

func main() {
	connMongo()
	var err error
	db, err = sql.Open(dbDriver, dSrcName)
	if err != nil {
		log.Fatal(err)
	}
	if nil == db {
		log.Fatal("nil db")
	}
	rows, err := db.Query("SELECT title, url, abstract, body_text, body_html FROM en ORDER BY url")
	if err != nil {
		err = errors.Wrap(err, "getting rows")
		log.Fatal(err)
	}
	defer rows.Close()
	id := 0
	for rows.Next() {
		wb := wikibook{Id: id}
		id++
		err = rows.Scan(&wb.Title, &wb.Url, &wb.Abstract, &wb.BodyText, &wb.BodyHtml)
		if err != nil {
			err = errors.Wrap(err, "scanning row")
			log.Fatal(err)
		}
		pageLoc := strings.Split(wb.Url, "https://en.wikibooks.org/wiki/")[1]
		pageParent := strings.Join(strings.Split(pageLoc, "/")[:len(strings.Split(pageLoc, "/"))-1], "/")
		if parent, ok := allWikibooks[pageParent]; ok {
			wb.parentPage = parent
			wb.ParentPageId = parent.Id
			parent.childPages = append(parent.childPages, &wb)
			parent.ChildPageIds = append(parent.ChildPageIds, wb.Id)
			allWikibooks[pageParent] = parent
		}
		time.Sleep(1*time.Millisecond)
		allWikibooks[pageLoc] = &wb
	}
	for _, v := range allWikibooks {
		wb := *v
		wbArr = append(wbArr, &wb)
	}
	if _, err = wbColl.InsertMany(context.Background(), wbArr); err != nil {
		err = errors.Wrap(err, "inserting many into mongodb")
		log.Println(err)
	}

	db.Close()
	log.Println("ok")
}
