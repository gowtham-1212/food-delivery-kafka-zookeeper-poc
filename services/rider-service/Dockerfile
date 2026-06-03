FROM node:20-slim

WORKDIR /app

COPY services/rider-service/package.json ./
RUN npm install

COPY services/rider-service/ ./

EXPOSE 3002

CMD ["npm", "run", "dev"]
