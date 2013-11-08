#/bin/bash

#curl -k --request GET --user haksoo:haksoorocks! "https://128.97.93.251:8443/api/streams/test?http_streaming=true&limit=-1" > output

#curl -k --request POST --user haksoo:haksoorocks! --data-binary @NESL_Veris_Current0_2013-10-01T00-00_data\ \(3\).csv --header "Content-type:application/octet-stream" "https://128.97.93.251:8443/api/streams/xively.csv"

curl -k --request GET --user user1:userrocks! "https://128.97.93.251:9443/api/streams/NESL_Veris__Current0?stream_owner=nesl_owner&http_streaming=true&limit=0"

