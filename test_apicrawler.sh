#!/bin/sh
curl -v -H "Content-Type: application/json" -X POST -d '[["facebook","search",["helium"],"lola"],["twitter","search",["helium"],"lola"],["youtube","search",["helium"],"lola"],["flickr","search",["helium"],"lola"],["google_plus","search",["helium"],"lola"]]' localhost:8080/crawl/add_direct
