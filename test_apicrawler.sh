#!/bin/sh
curl -v -H "Content-Type: application/json" -X POST -d '[["lola","facebook","search",["helium"]],["lola","twitter","search",["helium"]],["lola","youtube","search",["helium"]],["lola","flickr","search",["helium"]],["lola","google_plus","search",["helium"]]]' localhost:8080/crawl/add
