FROM golang:1.17 AS builder
LABEL maintainer="Jacob Bohanon <jacobbohanon@gmail.com>"
WORKDIR /build
COPY go.mod .
COPY go.sum .
COPY *.go .
RUN go mod tidy
RUN go mod download
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /app/capstone-etl
COPY en /app/
COPY en_wikibooks.sqlite /app/
WORKDIR /app
ENTRYPOINT ["/app/capstone-etl"]