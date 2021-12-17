FROM golang:1.17-alpine AS builder
LABEL maintainer="Jacob Bohanon <jacobbohanon@gmail.com>"
WORKDIR /build
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
WORKDIR /build/copy
COPY en .
COPY en_wikibooks.sqlite .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /build/copy/capstone-etl
FROM scratch
WORKDIR /app
COPY --from=builder /build/copy/ /app/
ENTRYPOINT ["/app/capstone-etl"]