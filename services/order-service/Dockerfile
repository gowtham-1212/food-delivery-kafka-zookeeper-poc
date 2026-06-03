FROM node:20-slim

WORKDIR /app

COPY services/order-service/package.json ./
RUN npm install

COPY services/order-service/ ./

EXPOSE 3000

CMD ["npm", "run", "dev"]
