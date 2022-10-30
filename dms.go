package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	runtimeconfig "google.golang.org/api/runtimeconfig/v1beta1"
	"google.golang.org/api/sheets/v4"
)

func stringify(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%#v", v)
	}
	return string(b)
}

func PollDMs(ctx context.Context, ds *datastore.Client) error {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	if err := pollDMsOnce(ctx, ds); err != nil {
		log.Printf("Failed to poll DMs: %s", err)
	}
	for {
		select {
		case <-t.C:
			if err := pollDMsOnce(ctx, ds); err != nil {
				log.Printf("Failed to poll DMs: %s", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func twitterClient(appCreds *TwitterCredentials, userCreds *TwitterUserCredentials) *twitter.Client {
	config := oauth1.NewConfig(appCreds.APIKey, appCreds.APIKeySecret)
	token := oauth1.NewToken(userCreds.Token, userCreds.TokenSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	return twitter.NewClient(httpClient)
}

func pollDMsOnce(ctx context.Context, ds *datastore.Client) error {
	log.Printf("Polling DMs")

	rcService, err := runtimeconfig.NewService(ctx)
	if err != nil {
		return err
	}
	vars := rcService.Projects.Configs.Variables
	spreadsheetID, err := vars.Get(fmt.Sprintf("projects/%s/configs/prod/variables/%s", os.Getenv("GOOGLE_CLOUD_PROJECT"), "spreadsheet_id")).Do()
	if err != nil {
		return fmt.Errorf("fetching spreadsheet_id: %w", err)
	}
	senderWhitelist := map[string]string{}
	err = vars.List(fmt.Sprintf("projects/%s/configs/prod", os.Getenv("GOOGLE_CLOUD_PROJECT"))).
		Filter(fmt.Sprintf("projects/%s/configs/prod/variables/whitelist/", os.Getenv("GOOGLE_CLOUD_PROJECT"))).
		PageSize(1000).
		ReturnValues(true).
		Pages(ctx, func(resp *runtimeconfig.ListVariablesResponse) error {
			prefix := fmt.Sprintf("projects/%s/configs/prod/variables/whitelist/", os.Getenv("GOOGLE_CLOUD_PROJECT"))
			for _, v := range resp.Variables {
				if !strings.HasPrefix(v.Name, prefix) {
					continue
				}
				senderWhitelist[v.Text] = strings.TrimPrefix(v.Name, prefix)
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("fetching whitelist: %w", err)
	}

	userCreds := &TwitterUserCredentials{}
	if err := ds.Get(ctx, datastore.NameKey(credentialsEntity, credentialsID, nil), userCreds); err != nil {
		return fmt.Errorf("failed to get user token: %w", err)
	}

	appCreds, err := creds(ctx)
	if err != nil {
		return fmt.Errorf("failed to get app credentials: %w", err)
	}

	twClient := twitterClient(&appCreds, userCreds)

	sheetsService, err := sheets.NewService(ctx)
	if err != nil {
		return fmt.Errorf("failed to create sheets service: %w", err)
	}

	lastTweetID, err := lastStoredTweetIDPerUser(ctx, sheetsService, spreadsheetID.Text, senderWhitelist)
	if err != nil {
		return fmt.Errorf("getting last stored tweet ID: %w", err)
	}

	needLastTweet := map[string]bool{}
	for id := range lastTweetID {
		needLastTweet[id] = true
	}

	events := []twitter.DirectMessageEvent{}
	cursor := ""
	for {
		resp, _, err := twClient.DirectMessages.EventsList(&twitter.DirectMessageEventsListParams{Cursor: cursor, Count: 50})
		if err != nil {
			return fmt.Errorf("failed to fetch DMs: %w", err)
		}
		cursor := resp.NextCursor

		for _, e := range resp.Events {
			if e.Type != "message_create" {
				continue
			}
			_, ok := senderWhitelist[e.Message.SenderID]
			if !ok {
				continue
			}
			if lastTweetID[e.Message.SenderID].ID != "" && !needLastTweet[e.Message.SenderID] {
				// Already reached the last recorded tweet for this sender
				continue
			}
			events = append(events, e)

			tid := tweetIDFromDM(e.Message)
			if tid != "" && tid == lastTweetID[e.Message.SenderID].ID {
				delete(needLastTweet, e.Message.SenderID)
			}
		}

		if cursor == "" {
			break
		}
	}

	sort.Slice(events, func(i, j int) bool {
		a := events[i].CreatedAt
		b := events[j].CreatedAt
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})
	eventsBySender := map[string][]twitter.DirectMessageEvent{}
	for _, e := range events {
		eventsBySender[e.Message.SenderID] = append(eventsBySender[e.Message.SenderID], e)
	}

	header, err := getSheetHeader(ctx, sheetsService, spreadsheetID.Text)
	if err != nil {
		return fmt.Errorf("getting spreadsheet header: %w", err)
	}

	for sender, events := range eventsBySender {
		for _, group := range groupDMsPerTweet(events) {
			data := map[string]interface{}{
				"sender_id":       sender,
				"sender_username": senderWhitelist[sender],
			}
			if len(group) == 0 {
				log.Printf("Error: empty group. Sender ID: %s, Events: %s", sender, stringify(events))
				continue
			}
			tweetID := tweetIDFromDM(group[0].Message)
			if tweetID == "" {
				log.Printf("Error: missing tweetID in the first message. Sender ID: %s, Group: %s", sender, stringify(group))
				continue
			}
			if tweetID == lastTweetID[sender].ID {
				if err := json.Unmarshal([]byte(lastTweetID[sender].JSON), &data); err != nil {
					log.Printf("Failed to parse JSON from the spreadsheet: %s\nJSON: %q\nRow number: %d", err, lastTweetID[sender].JSON, lastTweetID[sender].Row)
					continue
				}
				data["notes"] = groupToNotes(group, tweetID)
				row, err := tweetToRow(data, header)
				if err != nil {
					log.Printf("Failed to convert data for tweet %s into a row: %s", tweetID, err)
					continue
				}
				_, err = sheetsService.Spreadsheets.Values.Update(spreadsheetID.Text, fmt.Sprintf("Tweets!R%dC1:R%d", lastTweetID[sender].Row, lastTweetID[sender].Row), &sheets.ValueRange{
					Values: [][]interface{}{row},
				}).ValueInputOption("USER_ENTERED").Do()
				if err != nil {
					return fmt.Errorf("updating row %d: %w", lastTweetID[sender].Row, err)
				}
				log.Printf("updated row %d", lastTweetID[sender].Row)
				continue
			}
			id, err := strconv.ParseInt(tweetID, 10, 64)
			if err != nil {
				log.Printf("Failed to parse tweet ID %q as int64: %s", tweetID, err)
				continue
			}

			tweet, _, err := twClient.Statuses.Show(id, &twitter.StatusShowParams{IncludeEntities: twitter.Bool(true)})
			if err != nil {
				log.Printf("Failed to fetch tweet %s: %s", tweetID, err)
				continue
			}
			data["tweet"] = tweet
			data["notes"] = groupToNotes(group, tweetID)
			data["url"] = fmt.Sprintf("https://twitter.com/%s/status/%s", tweet.User.ScreenName, tweet.IDStr)

			row, err := tweetToRow(data, header)
			if err != nil {
				log.Printf("Failed to convert data for tweet %s into a row: %s", tweetID, err)
				continue
			}
			_, err = sheetsService.Spreadsheets.Values.Append(spreadsheetID.Text, "Tweets", &sheets.ValueRange{
				Values: [][]interface{}{row},
			}).ValueInputOption("USER_ENTERED").Do()
			if err != nil {
				return fmt.Errorf("appending tweet %s: %w", tweetID, err)
			}
		}
	}
	return nil
}

func getSheetHeader(ctx context.Context, sheetsService *sheets.Service, spreadsheetID string) ([]string, error) {
	sheet, err := sheetsService.Spreadsheets.Values.Get(spreadsheetID, "Tweets!1:1").MajorDimension("ROWS").Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get the values from spreadsheet: %w", err)
	}

	if len(sheet.Values) < 1 {
		return nil, fmt.Errorf("header row in the spreadsheet is empty")
	}

	header := []string{}
	for _, v := range sheet.Values[0] {
		header = append(header, fmt.Sprint(v))
	}
	return header, nil
}

type storedTweetInfo struct {
	ID   string
	Row  int
	JSON string
}

func lastStoredTweetIDPerUser(ctx context.Context, sheetsService *sheets.Service, spreadsheetID string, senderWhitelist map[string]string) (map[string]storedTweetInfo, error) {
	header, err := getSheetHeader(ctx, sheetsService, spreadsheetID)
	if err != nil {
		return nil, fmt.Errorf("getting sheet header: %w", err)
	}

	jsonColumnNumber := -1
	for i, h := range header {
		if h == "json" {
			jsonColumnNumber = i
		}
	}
	if jsonColumnNumber < 0 {
		return nil, fmt.Errorf("missing \"json\" column in the spreadsheet")
	}

	jsonValues, err := sheetsService.Spreadsheets.Values.Get(spreadsheetID, fmt.Sprintf("Tweets!R2C%d:C%d", jsonColumnNumber+1, jsonColumnNumber+1)).MajorDimension("COLUMNS").Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get \"json\" column from spreadsheet: %w", err)
	}

	if len(jsonValues.Values) <= 0 {
		return nil, nil
	}
	r := map[string]storedTweetInfo{}

	for i := len(jsonValues.Values[0]) - 1; i >= 0; i-- {
		s := fmt.Sprint(jsonValues.Values[0][i])
		j := struct {
			SenderID string `json:"sender_id"`
			Tweet    struct {
				ID string `json:"id_str"`
			} `json:"tweet"`
		}{}
		if err := json.Unmarshal([]byte(s), &j); err != nil {
			return nil, fmt.Errorf("unmarshaling last stored tweet: %w", err)
		}
		if r[j.SenderID].ID != "" {
			continue
		}
		if _, ok := senderWhitelist[j.SenderID]; !ok {
			continue
		}

		r[j.SenderID] = storedTweetInfo{ID: j.Tweet.ID, Row: i + 2, JSON: s}

		if len(r) == len(senderWhitelist) {
			break
		}
	}
	return r, nil
}

var tweetIDRe = regexp.MustCompile("^https://twitter.com/[^/]+/status/([0-9]+)([^0-9].*)?$")

func tweetIDFromDM(msg *twitter.DirectMessageEventMessage) string {
	for _, u := range msg.Data.Entities.Urls {
		m := tweetIDRe.FindStringSubmatch(u.ExpandedURL)
		if m == nil {
			continue
		}
		return m[1]
	}
	return ""
}

func groupDMsPerTweet(ms []twitter.DirectMessageEvent) [][]twitter.DirectMessageEvent {
	r := [][]twitter.DirectMessageEvent{}
	group := []twitter.DirectMessageEvent{}
	for _, e := range ms {
		if tweetIDFromDM(e.Message) != "" {
			if len(group) > 0 {
				r = append(r, group)
			}
			group = []twitter.DirectMessageEvent{e}
		} else {
			group = append(group, e)
		}
	}
	if len(group) > 0 {
		r = append(r, group)
	}
	return r
}

func groupToNotes(group []twitter.DirectMessageEvent, tweetID string) string {
	lines := []string{}
	for _, e := range group {
		line := e.Message.Data.Text
		for _, u := range e.Message.Data.Entities.Urls {
			replacement := u.ExpandedURL
			m := tweetIDRe.FindStringSubmatch(u.ExpandedURL)
			if m != nil && m[1] == tweetID {
				replacement = ""
			}
			line = strings.ReplaceAll(line, u.URL, replacement)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func tweetToRow(data map[string]interface{}, header []string) ([]interface{}, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshaling data: %s", err)
	}
	converted := map[string]interface{}{}
	if err := json.Unmarshal(b, &converted); err != nil {
		return nil, fmt.Errorf("unmarshaling data: %s", err)
	}

	lookup := func(field string) string {
		var cur interface{} = converted
		parts := strings.Split(field, ".")
		for _, part := range parts {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return ""
			}
			cur = m[part]
		}
		if cur == nil {
			return ""
		}
		return fmt.Sprint(cur)
	}

	r := []interface{}{}
	for _, field := range header {
		if field == "json" {
			r = append(r, string(b))
			continue
		}
		r = append(r, lookup(field))
	}
	return r, nil
}
