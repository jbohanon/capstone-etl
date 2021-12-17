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
	"strconv"
	"strings"
	"time"
)

const (
	workDir = `.` + string(os.PathSeparator)
	dSrcName = "file:" + workDir + "en_wikibooks.sqlite"
	dbDriver = "sqlite3"
)

var (
	mongoDbName = os.Getenv("MONGODB_NAME")
	db                 *sql.DB
	allWikibooksByPath = make(map[string]*wikibook)
	allWikibooksById = make(map[int]*wikibook)
	wbiArr             []interface{}
	wbArr []*wikibook
	wbColl *mongo.Collection
	mongodb *mongo.Client
	tokenColl        *mongo.Collection
	tokenVectorColl *mongo.Collection
	allTokensMap     = NewConcurrentMap()
	allTokens []string
	allTokenDocs []interface{}
	tokenRefs = make(map[string]map[int]bool)
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
		ChildPageIds []int `json:"child_pages" bson:"child_pages" redis:"child_pages"`
		parentPage *wikibook
		ParentPageId int   `json:"parent_page" bson:"parent_page" redis:"parent_page"`
		CountUniqueWords int `json:"count_unique_words" bson:"count_unique_words"` // count of all unique valid tokens -- initial extraction
		CountExternalLinks int `json:"count_external_links" bson:"count_external_links"` // count of all occurrences of "href=\"h" indicating an external link -- initial extraction
		CountChildren int `json:"count_children" bson:"count_children"` // count of all child pages (chapters) only on top-level pages -- final sweep
		Tokens []tokenQty `json:"tokens" bson:"tokens"` // strings and quantities for all tokens included in this wikibook
		TokenRefs []int // ids of all tokens in final sorted list -- final sweep
		EuclidianNorm float64 `json:"euclidian_norm" bson:"euclidian_norm"` // pre-calculated euclidian norm for use later with similarities
		tknQtyMap map[string]int // tmp use to optimize tokenization
	}
	tokenDoc struct {
		Id int `json:"_id" bson:"_id" redis:"_id"`
		Token string `json:"token" bson:"token" redis:"token"`
		References []idQty `json:"references" bson:"references" redis:"references"`
	}
	idQty struct {
		Id int `bson:"_id" json:"_id" redis:"_id"`
		Qty int `bson:"qty" json:"qty" redis:"qty"`
	}
	tokenQty struct {
		Token string `bson:"token" json:"token" redis:"token"`
		Qty int `bson:"qty" json:"qty" redis:"qty"`
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
	log.Println("begin")

	ctx, cf := context.WithCancel(context.Background())
	defer cf()

	connMongo(ctx)
	connDb()
	loadDictionary()

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

	for _, v := range allTokensMap.Keys() {
		allTokens = append(allTokens, v)
	}

	sort.Slice(allTokens, func(i, j int) bool { return allTokens[i] < allTokens[j]})

	allTokenDocs = make([]interface{}, len(allTokens), len(allTokens))

	for i, v := range allTokens {
		tkDoc := tokenDoc{
			Id:         i,
			Token:      v,
		}
		for k := range tokenRefs[v] {
			tkDoc.References = append(tkDoc.References, idQty{
				Id:  k,
				Qty: allWikibooksById[k].tknQtyMap[v],
			})
		}
		allTokenDocs[i] = &tkDoc
	}
	if _, err = tokenColl.InsertMany(ctx, allTokenDocs); err != nil {
		err = errors.Wrap(err, "inserting many into mongodb")
		log.Println(err)
	}

	log.Println("beginning sequential token vector construction loop")
	tokenVectors(ctx, wbArr)
	log.Println("sequential vector construction loop complete")
	log.Println("done parsing.")

	if _, err = wbColl.InsertMany(context.Background(), wbiArr); err != nil {
		err = errors.Wrap(err, "inserting many into mongodb")
		log.Println(err)
	}

	db.Close()
	log.Println("fin.")
}

func processWikibookRow(id int, rows *sql.Rows) {
	wb := wikibook{Id: id, tknQtyMap: make(map[string]int)}

	err := rows.Scan(&wb.Title, &wb.Url, &wb.Abstract, &wb.BodyText, &wb.BodyHtml)
	if err != nil {
		err = errors.Wrap(err, "scanning row")
		log.Fatal(err)
	}

	wb.CountExternalLinks = strings.Count(wb.BodyHtml, "href=\"h")

	pageLoc := strings.Split(wb.Url, "https://en.wikibooks.org/wiki/")[1]
	pageParent := strings.Join(strings.Split(pageLoc, "/")[:len(strings.Split(pageLoc, "/"))-1], "/")

	if parent, ok := allWikibooksByPath[pageParent]; ok {
		wb.parentPage = parent
		wb.ParentPageId = parent.Id
		parent.childPages = append(parent.childPages, &wb)
		parent.CountChildren++
		parent.ChildPageIds = append(parent.ChildPageIds, wb.Id)
		allWikibooksByPath[pageParent] = parent
	}

	wb = parseDoc(wb)
	time.Sleep(1 * time.Millisecond)

	allWikibooksByPath[pageLoc] = &wb
	allWikibooksById[wb.Id] = &wb
	wbiArr = append(wbiArr, &wb)
	wbArr = append(wbArr, &wb)
}

func tokenVectors(ctx context.Context, wbs []*wikibook) {
	insVal := make([]interface{}, len(wbs), len(wbs))
	for i, wbptr := range wbs {
		wb := *wbptr
		if i % 500 == 0 {
			log.Printf("%.2f%%", 100*(float64(i)/float64(len(wbs))))
		}
		sparseVector := make(bson.M, len(wb.Tokens))
		wb.TokenRefs = make([]int, len(allTokens), len(allTokens))
		k := 0 //TokenRefs iterator
		for j, v := range allTokens {
			if q, ok := wb.tknQtyMap[v]; ok {
				sparseVector[strconv.Itoa(j)] = q
				wb.TokenRefs[k] = j
				k++
			}
		}
		insVal[i] = bson.D{
			{"_id", wb.Id},
			{"compressed_token_vector", sparseVector},
		}
		wbs[i] = &wb
	}
	_, err := tokenVectorColl.InsertMany(ctx, insVal)
	if err != nil {
		err = errors.Wrap(err, "inserting token vector colls")
		log.Println(err)
	}
}

func parseDoc(doc wikibook) wikibook {
	strippedBody := clean([]byte(doc.BodyText))
	finalSplit := strings.Fields(strings.ToLower(strippedBody))

	thisWbTokens := make(map[string]int)
	for _, v := range finalSplit {
		if _, englishWord := dictionary[v]; englishWord {
			if _, stopWord := stopWords[v]; !stopWord {
				allTokensMap.Put(v, true)
				if qty, ok := thisWbTokens[v]; !ok {
					thisWbTokens[v] = 1
					doc.CountUniqueWords = doc.CountUniqueWords + 1
				} else {
					thisWbTokens[v] = qty + 1
				}

				if m, ok := tokenRefs[v]; !ok {
					tokenRefs[v] = map[int]bool{doc.Id: true}
				} else {
					if _, ok = m[doc.Id]; !ok {
						m[doc.Id] = true
						tokenRefs[v] = m
					}
				}
			}
		}
	}
	sqSum := 0
	for k, v := range thisWbTokens {
		doc.Tokens = append(doc.Tokens, tokenQty{
			Token: k,
			Qty:   v,
		})
		doc.tknQtyMap[k] = v
		sqSum += v*v
	}
	doc.EuclidianNorm = math.Sqrt(float64(sqSum))
	return doc
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

// list from https://www.ranks.nl/stopwords#article09e9cfa21e73da42e8e88ea97bc0a432
// added "t" since it wasn't already included
var stopWords = map[string]bool{
	"t": true,
	"": true,
	"a": true,
	"able": true,
	"about": true,
	"above": true,
	"abst": true,
	"accordance": true,
	"according": true,
	"accordingly": true,
	"across": true,
	"act": true,
	"actually": true,
	"added": true,
	"adj": true,
	"affected": true,
	"affecting": true,
	"affects": true,
	"after": true,
	"afterwards": true,
	"again": true,
	"against": true,
	"ah": true,
	"all": true,
	"almost": true,
	"alone": true,
	"along": true,
	"already": true,
	"also": true,
	"although": true,
	"always": true,
	"am": true,
	"among": true,
	"amongst": true,
	"an": true,
	"and": true,
	"announce": true,
	"another": true,
	"any": true,
	"anybody": true,
	"anyhow": true,
	"anymore": true,
	"anyone": true,
	"anything": true,
	"anyway": true,
	"anyways": true,
	"anywhere": true,
	"apparently": true,
	"approximately": true,
	"are": true,
	"aren": true,
	"arent": true,
	"arise": true,
	"around": true,
	"as": true,
	"aside": true,
	"ask": true,
	"asking": true,
	"at": true,
	"auth": true,
	"available": true,
	"away": true,
	"awfully": true,
	"b": true,
	"back": true,
	"be": true,
	"became": true,
	"because": true,
	"become": true,
	"becomes": true,
	"becoming": true,
	"been": true,
	"before": true,
	"beforehand": true,
	"begin": true,
	"beginning": true,
	"beginnings": true,
	"begins": true,
	"behind": true,
	"being": true,
	"believe": true,
	"below": true,
	"beside": true,
	"besides": true,
	"between": true,
	"beyond": true,
	"biol": true,
	"both": true,
	"brief": true,
	"briefly": true,
	"but": true,
	"by": true,
	"c": true,
	"ca": true,
	"came": true,
	"can": true,
	"cannot": true,
	"cant": true,
	"cause": true,
	"causes": true,
	"certain": true,
	"certainly": true,
	"co": true,
	"com": true,
	"come": true,
	"comes": true,
	"contain": true,
	"containing": true,
	"contains": true,
	"could": true,
	"couldnt": true,
	"d": true,
	"date": true,
	"did": true,
	"didnt": true,
	"different": true,
	"do": true,
	"does": true,
	"doesnt": true,
	"doing": true,
	"done": true,
	"dont": true,
	"down": true,
	"downwards": true,
	"due": true,
	"during": true,
	"e": true,
	"each": true,
	"ed": true,
	"edu": true,
	"effect": true,
	"eg": true,
	"eight": true,
	"eighty": true,
	"either": true,
	"else": true,
	"elsewhere": true,
	"end": true,
	"ending": true,
	"enough": true,
	"especially": true,
	"et": true,
	"etal": true,
	"etc": true,
	"even": true,
	"ever": true,
	"every": true,
	"everybody": true,
	"everyone": true,
	"everything": true,
	"everywhere": true,
	"ex": true,
	"except": true,
	"f": true,
	"far": true,
	"few": true,
	"ff": true,
	"fifth": true,
	"first": true,
	"five": true,
	"fix": true,
	"followed": true,
	"following": true,
	"follows": true,
	"for": true,
	"former": true,
	"formerly": true,
	"forth": true,
	"found": true,
	"four": true,
	"from": true,
	"further": true,
	"furthermore": true,
	"g": true,
	"gave": true,
	"get": true,
	"gets": true,
	"getting": true,
	"give": true,
	"given": true,
	"gives": true,
	"giving": true,
	"go": true,
	"goes": true,
	"gone": true,
	"got": true,
	"gotten": true,
	"h": true,
	"had": true,
	"happens": true,
	"hardly": true,
	"has": true,
	"hasnt": true,
	"have": true,
	"havent": true,
	"having": true,
	"he": true,
	"hed": true,
	"hence": true,
	"her": true,
	"here": true,
	"hereafter": true,
	"hereby": true,
	"herein": true,
	"heres": true,
	"hereupon": true,
	"hers": true,
	"herself": true,
	"hes": true,
	"hi": true,
	"hid": true,
	"him": true,
	"himself": true,
	"his": true,
	"hither": true,
	"home": true,
	"how": true,
	"howbeit": true,
	"however": true,
	"hundred": true,
	"i": true,
	"id": true,
	"ie": true,
	"if": true,
	"ill": true,
	"im": true,
	"immediate": true,
	"immediately": true,
	"importance": true,
	"important": true,
	"in": true,
	"inc": true,
	"indeed": true,
	"index": true,
	"information": true,
	"instead": true,
	"into": true,
	"invention": true,
	"inward": true,
	"is": true,
	"isnt": true,
	"it": true,
	"itd": true,
	"itll": true,
	"its": true,
	"itself": true,
	"ive": true,
	"j": true,
	"just": true,
	"k": true,
	"keepkeeps": true,
	"kept": true,
	"kg": true,
	"km": true,
	"know": true,
	"known": true,
	"knows": true,
	"l": true,
	"largely": true,
	"last": true,
	"lately": true,
	"later": true,
	"latter": true,
	"latterly": true,
	"least": true,
	"less": true,
	"lest": true,
	"let": true,
	"lets": true,
	"like": true,
	"liked": true,
	"likely": true,
	"line": true,
	"little": true,
	"ll": true,
	"look": true,
	"looking": true,
	"looks": true,
	"ltd": true,
	"m": true,
	"made": true,
	"mainly": true,
	"make": true,
	"makes": true,
	"many": true,
	"may": true,
	"maybe": true,
	"me": true,
	"mean": true,
	"means": true,
	"meantime": true,
	"meanwhile": true,
	"merely": true,
	"mg": true,
	"might": true,
	"million": true,
	"miss": true,
	"ml": true,
	"more": true,
	"moreover": true,
	"most": true,
	"mostly": true,
	"mr": true,
	"mrs": true,
	"much": true,
	"mug": true,
	"must": true,
	"my": true,
	"myself": true,
	"n": true,
	"na": true,
	"name": true,
	"namely": true,
	"nay": true,
	"nd": true,
	"near": true,
	"nearly": true,
	"necessarily": true,
	"necessary": true,
	"need": true,
	"needs": true,
	"neither": true,
	"never": true,
	"nevertheless": true,
	"new": true,
	"next": true,
	"nine": true,
	"ninety": true,
	"no": true,
	"nobody": true,
	"non": true,
	"none": true,
	"nonetheless": true,
	"noone": true,
	"nor": true,
	"normally": true,
	"nos": true,
	"not": true,
	"noted": true,
	"nothing": true,
	"now": true,
	"nowhere": true,
	"o": true,
	"obtain": true,
	"obtained": true,
	"obviously": true,
	"of": true,
	"off": true,
	"often": true,
	"oh": true,
	"ok": true,
	"okay": true,
	"old": true,
	"omitted": true,
	"on": true,
	"once": true,
	"one": true,
	"ones": true,
	"only": true,
	"onto": true,
	"or": true,
	"ord": true,
	"other": true,
	"others": true,
	"otherwise": true,
	"ought": true,
	"our": true,
	"ours": true,
	"ourselves": true,
	"out": true,
	"outside": true,
	"over": true,
	"overall": true,
	"owing": true,
	"own": true,
	"p": true,
	"page": true,
	"pages": true,
	"part": true,
	"particular": true,
	"particularly": true,
	"past": true,
	"per": true,
	"perhaps": true,
	"placed": true,
	"please": true,
	"plus": true,
	"poorly": true,
	"possible": true,
	"possibly": true,
	"potentially": true,
	"pp": true,
	"predominantly": true,
	"present": true,
	"previously": true,
	"primarily": true,
	"probably": true,
	"promptly": true,
	"proud": true,
	"provides": true,
	"put": true,
	"q": true,
	"que": true,
	"quickly": true,
	"quite": true,
	"qv": true,
	"r": true,
	"ran": true,
	"rather": true,
	"rd": true,
	"re": true,
	"readily": true,
	"really": true,
	"recent": true,
	"recently": true,
	"ref": true,
	"refs": true,
	"regarding": true,
	"regardless": true,
	"regards": true,
	"related": true,
	"relatively": true,
	"research": true,
	"respectively": true,
	"resulted": true,
	"resulting": true,
	"results": true,
	"right": true,
	"run": true,
	"s": true,
	"said": true,
	"same": true,
	"saw": true,
	"say": true,
	"saying": true,
	"says": true,
	"sec": true,
	"section": true,
	"see": true,
	"seeing": true,
	"seem": true,
	"seemed": true,
	"seeming": true,
	"seems": true,
	"seen": true,
	"self": true,
	"selves": true,
	"sent": true,
	"seven": true,
	"several": true,
	"shall": true,
	"she": true,
	"shed": true,
	"shell": true,
	"shes": true,
	"should": true,
	"shouldnt": true,
	"show": true,
	"showed": true,
	"shown": true,
	"showns": true,
	"shows": true,
	"significant": true,
	"significantly": true,
	"similar": true,
	"similarly": true,
	"since": true,
	"six": true,
	"slightly": true,
	"so": true,
	"some": true,
	"somebody": true,
	"somehow": true,
	"someone": true,
	"somethan": true,
	"something": true,
	"sometime": true,
	"sometimes": true,
	"somewhat": true,
	"somewhere": true,
	"soon": true,
	"sorry": true,
	"specifically": true,
	"specified": true,
	"specify": true,
	"specifying": true,
	"still": true,
	"stop": true,
	"strongly": true,
	"sub": true,
	"substantially": true,
	"successfully": true,
	"such": true,
	"sufficiently": true,
	"suggest": true,
	"sup": true,
	"suret": true,
	"take": true,
	"taken": true,
	"taking": true,
	"tell": true,
	"tends": true,
	"th": true,
	"than": true,
	"thank": true,
	"thanks": true,
	"thanx": true,
	"that": true,
	"thatll": true,
	"thats": true,
	"thatve": true,
	"the": true,
	"their": true,
	"theirs": true,
	"them": true,
	"themselves": true,
	"then": true,
	"thence": true,
	"there": true,
	"thereafter": true,
	"thereby": true,
	"thered": true,
	"therefore": true,
	"therein": true,
	"therell": true,
	"thereof": true,
	"therere": true,
	"theres": true,
	"thereto": true,
	"thereupon": true,
	"thereve": true,
	"these": true,
	"they": true,
	"theyd": true,
	"theyll": true,
	"theyre": true,
	"theyve": true,
	"think": true,
	"this": true,
	"those": true,
	"thou": true,
	"though": true,
	"thoughh": true,
	"thousand": true,
	"throug": true,
	"through": true,
	"throughout": true,
	"thru": true,
	"thus": true,
	"til": true,
	"tip": true,
	"to": true,
	"together": true,
	"too": true,
	"took": true,
	"toward": true,
	"towards": true,
	"tried": true,
	"tries": true,
	"truly": true,
	"try": true,
	"trying": true,
	"ts": true,
	"twice": true,
	"two": true,
	"u": true,
	"un": true,
	"under": true,
	"unfortunately": true,
	"unless": true,
	"unlike": true,
	"unlikely": true,
	"until": true,
	"unto": true,
	"up": true,
	"upon": true,
	"ups": true,
	"us": true,
	"use": true,
	"used": true,
	"useful": true,
	"usefully": true,
	"usefulness": true,
	"uses": true,
	"using": true,
	"usually": true,
	"v": true,
	"value": true,
	"various": true,
	"ve": true,
	"very": true,
	"via": true,
	"viz": true,
	"vol": true,
	"vols": true,
	"vs": true,
	"w": true,
	"want": true,
	"wants": true,
	"was": true,
	"wasnt": true,
	"way": true,
	"we": true,
	"wed": true,
	"welcome": true,
	"well": true,
	"went": true,
	"were": true,
	"werent": true,
	"weve": true,
	"what": true,
	"whatever": true,
	"whatll": true,
	"whats": true,
	"when": true,
	"whence": true,
	"whenever": true,
	"where": true,
	"whereafter": true,
	"whereas": true,
	"whereby": true,
	"wherein": true,
	"wheres": true,
	"whereupon": true,
	"wherever": true,
	"whether": true,
	"which": true,
	"while": true,
	"whim": true,
	"whither": true,
	"who": true,
	"whod": true,
	"whoever": true,
	"whole": true,
	"wholl": true,
	"whom": true,
	"whomever": true,
	"whos": true,
	"whose": true,
	"why": true,
	"widely": true,
	"willing": true,
	"wish": true,
	"with": true,
	"within": true,
	"without": true,
	"wont": true,
	"words": true,
	"world": true,
	"would": true,
	"wouldnt": true,
	"www": true,
	"x": true,
	"y": true,
	"yes": true,
	"yet": true,
	"you": true,
	"youd": true,
	"youll": true,
	"your": true,
	"youre": true,
	"yours": true,
	"yourself": true,
	"yourselves": true,
	"youve": true,
	"z": true,
	"zero": true,
}