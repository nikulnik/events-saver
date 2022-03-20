FROM golang:1.17.8-alpine3.14
WORKDIR /root/
COPY . .

RUN go build

FROM alpine:3.14
WORKDIR /root/

COPY --from=0 /root/events-saver .
CMD [ "./events-saver" ]
