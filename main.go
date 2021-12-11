package main

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	//workDir = `/home/jacob/Documents/WGU/capstone/`
	workDir = `C:\Users\jacob.bohanon\Documents\Code\capstone\`
	dSrcName = "file:" + workDir + "en_wikibooks.sqlite"
	dbDriver = "sqlite3"
	mongoDbName = "wikibooks2"
)

var (
	db *sql.DB
	allWikibooks = make(map[string]*wikibook)
	wbiArr []interface{}
	wbArr []*wikibook
	wbColl *mongo.Collection
	mongodb *mongo.Client
	tokenColl        *mongo.Collection
	tokenVectorColl *mongo.Collection
	allTokensMap     = NewConcurrentMap()
	allTokens []string
	n = 0
	dictionary = make(map[string]bool, 300000)
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
	tokenDoc struct {
		Id int `json:"_id" bson:"_id" redis:"_id"`
		Token string `json:"token" bson:"token" redis:"token"`
	}
)

func connMongo(ctx context.Context) {
	var err error
	mongodb, err = mongo.Connect(ctx, nil)
	if err != nil {
		err = errors.Wrap(err, "connecting to mongodb")
		panic(err)
	}
	if err = mongodb.Ping(ctx, nil); err != nil {
		err = errors.Wrap(err, "pinging mongodb")
		panic(err)
	}
	wbColl = mongodb.Database(mongoDbName).Collection("wikibooks")
	tokenColl = mongodb.Database(mongoDbName).Collection("tokens")
	tokenVectorColl = mongodb.Database(mongoDbName).Collection("token_vector")
}

func connDb() {
	var err error
	db, err = sql.Open(dbDriver, dSrcName)
	if err != nil {
		log.Fatal(err)
	}
	if nil == db {
		log.Fatal("nil db")
	}
}

func loadDictionary() {
	dictb, err := os.ReadFile("." + string(os.PathSeparator) + `en`)
	if err != nil {
		err = errors.Wrap(err, "opening and reading dictionary file")
		log.Fatal(err)
	}
	dictArr := strings.Split(strings.ToLower(string(dictb)), "\n")
	for _, v := range dictArr {
		dictionary[v] = true
	}
}

func main() {
	ctx, cf := context.WithCancel(context.Background())
	defer cf()

	connMongo(ctx)
	connDb()

	rows, err := db.Query("SELECT title, url, abstract, body_text, body_html FROM en ORDER BY url")
	if err != nil {
		err = errors.Wrap(err, "getting rows")
		log.Fatal(err)
	}
	defer rows.Close()

	id := 0
	for rows.Next() {
		processWikibookRow(id, rows)
		id++
	}

	loadDictionary()

	log.Println("beginning concurrent token extraction loop")
	wg := sync.WaitGroup{}
	for _, doc := range wbArr {
		wg.Add(1)
		n++
		go parseDoc(&wg, *doc)
	}
	wg.Wait()
	log.Println("concurrent token extraction loop complete")

	for _, v := range allTokensMap.Keys() {
		allTokens = append(allTokens, v)
	}

	sort.Slice(allTokens, func(i, j int) bool { return allTokens[i] < allTokens[j]})

	log.Println("beginning concurrent token vector construction loop")
	wg = sync.WaitGroup{}
	wg.Add(1)
	go tokenVectors(&wg, ctx, wbArr)

	log.Println("beginning concurrent token insert loop")
	wg.Add(1)
	go tokens(&wg, err, ctx)

	wg.Wait()
	log.Println("done parsing.")

	if _, err = wbColl.InsertMany(context.Background(), wbiArr); err != nil {
		err = errors.Wrap(err, "inserting many into mongodb")
		log.Println(err)
	}

	db.Close()
	log.Println("ok")
}

func processWikibookRow(id int, rows *sql.Rows) {
	wb := wikibook{Id: id}

	err := rows.Scan(&wb.Title, &wb.Url, &wb.Abstract, &wb.BodyText, &wb.BodyHtml)
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

	time.Sleep(1 * time.Millisecond)

	allWikibooks[pageLoc] = &wb
	wbiArr = append(wbiArr, &wb)
	wbArr = append(wbArr, &wb)
}

func tokens(wg *sync.WaitGroup, err error, ctx context.Context) {
	wg2 := sync.WaitGroup{}
	for i, tkn := range allTokens {
		wg2.Add(1)
		go func(innerWg *sync.WaitGroup, i2 int, tkn2 string) {
			_, err = tokenColl.InsertOne(ctx, tokenDoc{
				Id:    i2,
				Token: tkn2,
			})
			if err != nil {
				err = errors.Wrapf(err, "inserting token %s", tkn2)
				log.Println(err)
			}
			innerWg.Done()
		}(&wg2, i, tkn)
	}
	wg.Done()
	log.Println("concurrent token insert loop complete")
}

func tokenVectors(wg *sync.WaitGroup, ctx context.Context, wbs []*wikibook) {
	for i, wbptr := range wbs {
		wb := *wbptr
		tv := make([]int, len(allTokens), len(allTokens))
		if i % 500 == 0 {
			log.Printf("%.2f%%", 100*(float64(i)/float64(len(wbs))))
		}
		sqSum := 0
		uniqueWords := 0
		for j, v := range allTokens {
			count := strings.Count(wb.BodyText, v)
			if count != 0 {
				uniqueWords++
			}
			tv[j] = count
			sqSum += count*count
		}
		euclidianNorm := math.Sqrt(float64(sqSum))
		_, err := tokenVectorColl.InsertOne(ctx, bson.D{
			{"_id", wb.Id},
			{"token_vector", tv},
			{"euclidian_norm", euclidianNorm},
			{"unique_token_count", uniqueWords},
		})
		if err != nil {
			err = errors.Wrapf(err, "updating %s", wb.Url)
			log.Println(err)
		}
	}
	wg.Done()
	log.Println("sequential concurrent vector construction loop complete")
}

func parseDoc(wg *sync.WaitGroup, doc wikibook) {
	strippedBody := clean([]byte(doc.BodyText))
	finalSplit := strings.Fields(strings.ToLower(strippedBody))

	for _, v := range finalSplit {
		if _, englishWord := dictionary[v]; englishWord {
			if _, stopWord := stopWords[v]; !stopWord {
				allTokensMap.Put(v, true)
			}
		}
	}
	n--
	wg.Done()
}

func clean(s []byte) string {
	j := 0
	for _, b := range s {
		if ('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9') ||
			b == ' ' {
			s[j] = b
			j++
		}
	}
	return string(s[:j])
}

var stopWords = map[string]bool{
	"a": true,
	"about": true,
	"actually": true,
	"almost": true,
	"also": true,
	"although": true,
	"always": true,
	"am": true,
	"an": true,
	"and": true,
	"any": true,
	"are": true,
	"as": true,
	"at": true,
	"be": true,
	"became": true,
	"become": true,
	"but": true,
	"by": true,
	"can": true,
	"could": true,
	"did": true,
	"do": true,
	"does": true,
	"each": true,
	"either": true,
	"else": true,
	"for": true,
	"from": true,
	"had": true,
	"has": true,
	"have": true,
	"hence": true,
	"how": true,
	"i": true,
	"if": true,
	"in": true,
	"is": true,
	"it": true,
	"its": true,
	"just": true,
	"may": true,
	"maybe": true,
	"me": true,
	"might": true,
	"mine": true,
	"must": true,
	"my": true,
	"neither": true,
	"nor": true,
	"not": true,
	"of": true,
	"oh": true,
	"ok": true,
	"when": true,
	"where": true,
	"whereas": true,
	"wherever": true,
	"whenever": true,
	"whether": true,
	"which": true,
	"while": true,
	"who": true,
	"whom": true,
	"whoever": true,
	"whose": true,
	"why": true,
	"will": true,
	"with": true,
	"within": true,
	"without": true,
	"would": true,
	"yes": true,
	"yet": true,
	"you": true,
	"your": true,
}