FROM node:latest
MAINTAINER Leandro Dias
RUN mkdir -p /usr/src/contact-service
WORKDIR /usr/src/contact-service
COPY package.json /usr/src/contact-service
RUN npm install
COPY . /usr/src/contact-service
CMD [ "npm", "start" ]