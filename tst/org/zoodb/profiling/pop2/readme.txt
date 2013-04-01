### parse tags.xml
file is not in UTF-8 -->SAXParser uses UTF-8 stream reader as default
1) change encoding to Windows-1252
2) remove problem line: 208050


### parse dblp.xml
- create conference_series via distinct 'partial_keys' from conference.key attribute
	pattern:
		conf/{series.key}/year... (DONE) --> create ConferenceSeries according to {series.key}
- how to get the name of the conference_series?
	--> scrape website
