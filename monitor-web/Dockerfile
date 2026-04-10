FROM nginx:1.27-alpine

RUN apk add --no-cache wget

COPY monitor-web/nginx.conf /etc/nginx/conf.d/default.conf
COPY monitor-web/index.html /usr/share/nginx/html/index.html
COPY monitor-web/app.js /usr/share/nginx/html/app.js
COPY monitor-web/styles.css /usr/share/nginx/html/styles.css
