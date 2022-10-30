.PHONY: all build run datastore deploy

PROJECT:=ukd-tweet-saver
gcloud:=gcloud --project=$(PROJECT)

all: build

build: tweet-saver

tweet-saver: $(wildcard *.go go.*)
	go build .

run:
	$$($(gcloud) beta emulators datastore env-init --data-dir=datastore-emulator); \
	source .secrets/twitter.sh; \
	export GOOGLE_APPLICATION_CREDENTIALS=".secrets/service-account-key.json"; \
	export GOOGLE_CLOUD_PROJECT="$(PROJECT)"; \
	go run .

datastore:
	$(gcloud) beta emulators datastore start --data-dir=datastore-emulator

deploy:
	$(gcloud) app deploy --quiet
