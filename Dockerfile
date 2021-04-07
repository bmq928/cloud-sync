FROM node:14.4-alpine

WORKDIR /app
COPY . /app
RUN yarn install --prod
ENTRYPOINT ["yarn", "start"]