package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"cloud.google.com/go/datastore"
	oauth1Login "github.com/dghubble/gologin/v2/oauth1"
	twitterlogin "github.com/dghubble/gologin/v2/twitter"
	"github.com/dghubble/oauth1"
	twitterOAuth1 "github.com/dghubble/oauth1/twitter"
	runtimeconfig "google.golang.org/api/runtimeconfig/v1beta1"
	"google.golang.org/appengine/v2"
)

const credentialsEntity = "Credentials"
const credentialsID = credentialsEntity

type TwitterCredentials struct {
	APIKey       string
	APIKeySecret string
	BearerToken  string
	ClientID     string
	ClientSecret string
}

type TwitterUserCredentials struct {
	Token       string
	TokenSecret string
}

func credsFromRuntimeConfig(ctx context.Context) (TwitterCredentials, error) {
	rcService, err := runtimeconfig.NewService(ctx)
	if err != nil {
		return TwitterCredentials{}, err
	}
	vars := rcService.Projects.Configs.Variables
	r := TwitterCredentials{}
	fields := []struct {
		name string
		dest *string
	}{
		{"twitter/api_key", &r.APIKey},
		{"twitter/api_key_secret", &r.APIKeySecret},
		{"twitter/bearer_token", &r.BearerToken},
		{"twitter/client_id", &r.ClientID},
		{"twitter/client_secret", &r.ClientSecret},
	}
	for _, f := range fields {
		v, err := vars.Get(fmt.Sprintf("projects/%s/configs/prod/variables/%s", os.Getenv("GOOGLE_CLOUD_PROJECT"), url.PathEscape(f.name))).Do()
		if err != nil {
			return TwitterCredentials{}, fmt.Errorf("getting variable %q: %w", f.name, err)
		}
		*f.dest = v.Text
	}
	return r, nil
}

func credsFromEnv() TwitterCredentials {
	r := TwitterCredentials{}
	vars := []struct {
		name string
		dest *string
	}{
		{"TWITTER_API_KEY", &r.APIKey},
		{"TWITTER_API_KEY_SECRET", &r.APIKeySecret},
		{"TWITTER_BEARER_TOKEN", &r.BearerToken},
		{"TWITTER_CLIENT_ID", &r.ClientID},
		{"TWITTER_CLIENT_SECRET", &r.ClientSecret},
	}
	for _, v := range vars {
		*v.dest = os.Getenv(v.name)
	}
	return r
}

func creds(ctx context.Context) (TwitterCredentials, error) {
	if appengine.IsAppEngine() {
		return credsFromRuntimeConfig(ctx)
	}
	return credsFromEnv(), nil
}

func datastoreClient(ctx context.Context) (*datastore.Client, error) {
	if os.Getenv("DATASTORE_EMULATOR_HOST") != "" {
		return datastore.NewClient(ctx, "")
	}
	return datastore.NewClient(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"))
}

func main() {
	ctx := context.Background()
	creds, err := creds(ctx)
	if err != nil {
		log.Fatalf("Failed to get credentials: %s", err)
	}
	oauth1Config := &oauth1.Config{
		ConsumerKey:    creds.APIKey,
		ConsumerSecret: creds.APIKeySecret,
		CallbackURL:    "https://ukd-tweet-saver.nw.r.appspot.com/oauth_callback",
		Endpoint:       twitterOAuth1.AuthorizeEndpoint,
	}
	ds, err := datastoreClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create datastore client: %s", err)
	}

	rcService, err := runtimeconfig.NewService(ctx)
	if err != nil {
		log.Fatalf("Failed to initialise runtimeconfig client: %s", err)
	}
	botUserID, err := rcService.Projects.Configs.Variables.Get(fmt.Sprintf("projects/%s/configs/prod/variables/%s", os.Getenv("GOOGLE_CLOUD_PROJECT"), url.PathEscape("twitter/bot_user_id"))).Do()
	if err != nil {
		log.Fatalf("Failed to get bot user ID: %s", err)
	}

	rebuild := make(chan struct{})
	http.Handle("/", twitterlogin.LoginHandler(oauth1Config, nil))
	http.Handle("/oauth_callback", twitterlogin.CallbackHandler(oauth1Config, loginHandler(ds, botUserID.Text), nil))
	http.HandleFunc("/rebuild", func(w http.ResponseWriter, r *http.Request) {
		rebuild <- struct{}{}
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/_ah/warmup", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	go func() {
		if err := PollDMs(ctx, ds, rebuild); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func loginHandler(ds *datastore.Client, botUserID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()

		twitterUser, err := twitterlogin.UserFromContext(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if twitterUser.IDStr != botUserID {
			http.Error(w, fmt.Sprintf("Unauthorized user %s", twitterUser.IDStr), http.StatusUnauthorized)
			return
		}
		accessToken, accessSecret, err := oauth1Login.AccessTokenFromContext(ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get access token: %s", err), http.StatusBadRequest)
			return
		}
		creds := &TwitterUserCredentials{
			Token:       accessToken,
			TokenSecret: accessSecret,
		}
		if _, err := ds.Put(ctx, datastore.NameKey(credentialsEntity, credentialsID, nil), creds); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store credentials: %s", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "OK")
	})
}
