#!/bin/sh
curl -v -H "Content-Type: application/json" -X POST -d '[["lola","facebook","search",["olympics", "business"]],["lola","twitter","search",["olympics", "business"]],["lola","youtube","search",["olympics", "business"]],["lola","flickr","search",["olympics", "business"]],["lola","google_plus","search",["olympics", "business"]]]' localhost:8080/crawl/add
