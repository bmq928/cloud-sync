FROM node:14.4-alpine

RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
  apk update \
  apk add s3cmd

WORKDIR /app
COPY . /app
RUN yarn install --prod
ENTRYPOINT ["yarn", "start"]