FROM node:latest
MAINTAINER Leandro Dias
RUN mkdir -p /usr/src/api-gateway
WORKDIR /usr/src/api-gateway
COPY package.json /usr/src/api-gateway
RUN npm install
COPY . /usr/src/api-gateway
EXPOSE 5000
CMD [ "npm", "start" ]