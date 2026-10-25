FROM alpine:3.20

RUN adduser -D -H -u 10001 app
WORKDIR /app
COPY bin/server-linux /app/server

USER app
EXPOSE 8080 9090
ENTRYPOINT ["/app/server"]
