FROM node:16-alpine
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH
COPY package.json ./
COPY package-lock.json ./
RUN npm ci 
COPY . ./
RUN ["chmod", "+x", "/app/process-bulk-uploads.sh"]
CMD [ "/app/process-bulk-uploads.sh" ]
